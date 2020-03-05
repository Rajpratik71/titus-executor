package service

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/Netflix/titus-executor/logger"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

const (
	lockTime                        = 30 * time.Second
	timeBetweenTryingToAcquireLocks = 15 * time.Second
)

type keyedItem interface {
	key() string
	String() string
}

type itemLister func(context.Context) ([]keyedItem, error)
type workFunc func(context.Context, keyedItem)

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
					ctx := logger.WithFields(ctx, map[string]interface{}{
						"key":      item.key(),
						"taskName": taskName,
					})
					logger.G(ctx).Info("Starting new long running function under lock")
					lockName := fmt.Sprintf("%s_%s", taskName, item.key())
					go vpcService.waitToAcquireLongLivedLock(ctx, hostname, lockName, func(ctx2 context.Context) {
						logger.G(ctx2).Debug("Work fun starting")
						wf(ctx2, item)
						logger.G(ctx2).Debug("Work fun ending")
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

func (vpcService *vpcService) waitToAcquireLongLivedLock(ctx context.Context, hostname, lockName string, workFun func(context.Context)) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	timer := time.NewTimer(lockTime / 2)
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

	row := tx.QueryRowContext(ctx, "INSERT INTO long_lived_locks(lock_name, held_by, held_until) VALUES ($1, $2, now() + ($3 * interval '1 sec')) ON CONFLICT (lock_name) DO UPDATE SET held_by = $2, held_until = now() + ($3 * interval '1 sec') WHERE long_lived_locks.held_until < now() RETURNING id", lockName, hostname, lockTime.Seconds())
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

func (vpcService *vpcService) holdLock(ctx context.Context, hostname string, id int, workFun func(context.Context)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go workFun(ctx)

	ticker := time.NewTicker(lockTime / 4)
	defer ticker.Stop()
	for {
		err := vpcService.tryToHoldLock(ctx, hostname, id)
		if err != nil {
			return err
		}
		select {
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
