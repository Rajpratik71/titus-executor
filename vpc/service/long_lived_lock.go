package service

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/Netflix/titus-executor/logger"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
	"k8s.io/apimachinery/pkg/util/sets"
)

// TODO: Break this out into its own package

const (
	lockTime                        = 30 * time.Second
	timeBetweenTryingToAcquireLocks = 15 * time.Second
)

type keyedItem interface {
	key() string
	String() string
}

type itemLister func(context.Context) ([]keyedItem, error)
type workFunc func(context.Context, keyedItem) error

func generateLockName(taskName string, item keyedItem) string {
	return fmt.Sprintf("%s_%s", taskName, item.key())
}

func (vpcService *vpcService) runFunctionUnderLongLivedLock(ctx context.Context, taskName string, lister itemLister, wf workFunc) error {
	startedLockers := sets.NewString()
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	t := time.NewTimer(timeBetweenTryingToAcquireLocks)
	defer t.Stop()
	for {
		items, err := lister(ctx)
		if err != nil {
			logger.G(ctx).WithError(err).Error("Cannot list items")
		} else {
			for idx := range items {
				item := items[idx]
				if !startedLockers.Has(item.key()) {
					startedLockers.Insert(item.key())
					ctx2 := logger.WithFields(ctx, map[string]interface{}{
						"taskName": taskName,
					})
					lockName := generateLockName(taskName, item)
					logger.G(ctx2).Info("Starting new long running function under lock")
					go vpcService.waitToAcquireLongLivedLock(ctx2, hostname, lockName, func(ctx3 context.Context) error {
						logger.G(ctx3).Info("Work fun starting")
						err2 := wf(ctx3, item)
						logger.G(ctx3).WithError(err2).Info("Work fun ending")
						return err2
					})
				}
			}
		}
		t.Reset(timeBetweenTryingToAcquireLocks)
		select {
		case <-t.C:
		case <-ctx.Done():
			return nil
		}
	}
}

func (vpcService *vpcService) waitToAcquireLongLivedLock(ctx context.Context, hostname, lockName string, workFun func(context.Context) error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	timer := time.NewTimer(lockTime / 2)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		lockAcquired, id, err := vpcService.tryToAcquireLock(ctx, hostname, lockName)
		if err != nil {
			logger.G(ctx).WithError(err).Error("Error while trying to acquire lock")
		} else if lockAcquired {
			logger.G(ctx).Debug("Lock acquired")
			err = vpcService.holdLock(ctx, hostname, id, workFun)
			if err != nil {
				logger.G(ctx).WithError(err).Error("Error while holding lock")
			}
		}
		timer.Reset(lockTime / 2)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

func (vpcService *vpcService) tryToAcquireLock(ctx context.Context, hostname, lockName string) (bool, int, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx, span := trace.StartSpan(ctx, "tryToAcquireLock")
	defer span.End()

	tx, err := vpcService.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		err = errors.Wrap(err, "Could not start database transaction")
		span.SetStatus(traceStatusFromError(err))
		return false, 0, err
	}

	defer func() {
		_ = tx.Rollback()
	}()

	row := tx.QueryRowContext(ctx, `
INSERT INTO long_lived_locks(lock_name, held_by, held_until)
VALUES ($1, $2, now() + ($3 * interval '1 sec')) ON CONFLICT (lock_name) DO
UPDATE
SET held_by = $2,
    held_until = now() + ($3 * interval '1 sec')
WHERE long_lived_locks.held_until < now() RETURNING id
`, lockName, hostname, lockTime.Seconds())
	var id int
	err = row.Scan(&id)
	if err == sql.ErrNoRows {
		return false, 0, nil
	} else if err != nil {
		err = errors.Wrap(err, "Could not insert into long lived locks")
		span.SetStatus(traceStatusFromError(err))
		return false, 0, err
	}

	err = tx.Commit()
	if err != nil {
		err = errors.Wrap(err, "Could not commit transaction")
		span.SetStatus(traceStatusFromError(err))
		return false, 0, err
	}

	return true, id, nil
}

func (vpcService *vpcService) tryToHoldLock(ctx context.Context, hostname string, id int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx, span := trace.StartSpan(ctx, "tryToHoldLock")
	defer span.End()

	logger.G(ctx).WithField("hostname", hostname).WithField("id", id).Debug("Trying to hold lock")

	tx, err := vpcService.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		err = errors.Wrap(err, "Could not start database transaction")
		span.SetStatus(traceStatusFromError(err))
		return err
	}

	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(ctx, "UPDATE long_lived_locks SET held_until = now() + ($1 * interval '1 sec') WHERE id = $2 AND held_by = $3", lockTime.Seconds(), id, hostname)
	if err != nil {
		err = errors.Wrap(err, "Could update lock time")
		span.SetStatus(traceStatusFromError(err))
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		err = errors.Wrap(err, "Could not get rows affected")
		span.SetStatus(traceStatusFromError(err))
		return err
	}

	if n != 1 {
		err = fmt.Errorf("Unexpected number of rows updated: %d", n)
		span.SetStatus(traceStatusFromError(err))
		return err
	}

	err = tx.Commit()
	if err != nil {
		err = errors.Wrap(err, "Could not commit transaction")
		span.SetStatus(traceStatusFromError(err))
		return err
	}

	return nil
}

func (vpcService *vpcService) holdLock(ctx context.Context, hostname string, id int, workFun func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error)
	go func() {
		errCh <- workFun(ctx)
	}()

	ticker := time.NewTicker(lockTime / 4)
	defer ticker.Stop()
	for {
		err := vpcService.tryToHoldLock(ctx, hostname, id)
		if err != nil {
			return err
		}
		select {
		case err = <-errCh:
			logger.G(ctx).WithError(err).Error("Worker encountered error, releasing lock")
			return errors.Wrap(err, "Worker encountered error")
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitFor(ctx context.Context, duration time.Duration) error {
	t := time.NewTimer(duration)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
