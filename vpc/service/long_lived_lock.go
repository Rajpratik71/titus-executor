package service

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/Netflix/titus-executor/logger"
	vpcapi "github.com/Netflix/titus-executor/vpc/api"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

const (
	lockTime                        = 30 * time.Second
	timeBetweenTryingToAcquireLocks = 15 * time.Second
)

func (vpcService *vpcService) readLocks(rows *sql.Rows) ([]*vpcapi.Lock, error) {
	locks := []*vpcapi.Lock{}
	for rows.Next() {

		lock := vpcapi.Lock{}
		var heldUntil time.Time
		err := rows.Scan(&lock.Id, &lock.LockName, &lock.HeldBy, &heldUntil)
		if err != nil {
			return nil, err
		}

		lock.HeldUntil, err = ptypes.TimestampProto(heldUntil)
		if err != nil {
			return nil, err
		}

		locks = append(locks, &lock)
	}

	return locks, nil
}

func (vpcService *vpcService) GetLocks(ctx context.Context, empty *empty.Empty) (*vpcapi.GetLocksResponse, error) {
	rows, err := vpcService.db.QueryContext(ctx, "SELECT id, lock_name, held_by, held_until FROM long_lived_locks LIMIT 1000")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	locks, err := vpcService.readLocks(rows)
	if err != nil {
		return nil, err
	}

	return &vpcapi.GetLocksResponse{Locks: locks}, nil
}

func (vpcService *vpcService) GetLock(ctx context.Context, id *vpcapi.LockId) (*vpcapi.Lock, error) {
	rows, err := vpcService.db.QueryContext(ctx, "SELECT id, lock_name, held_by, held_until FROM long_lived_locks WHERE id = $1", id.GetId())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	locks, err := vpcService.readLocks(rows)
	if err != nil {
		return nil, err
	}

	if len(locks) == 0 {
		return nil, status.Error(codes.NotFound, "lock not found")
	}

	return locks[0], nil
}

func (vpcService *vpcService) DeleteLock(ctx context.Context, id *vpcapi.LockId) (*empty.Empty, error) {

	res, err := vpcService.db.ExecContext(ctx, "DELETE FROM long_lived_locks WHERE id = $1", id.GetId())
	if err != nil {
		return nil, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	if affected == 0 {
		return nil, status.Error(codes.NotFound, "lock not found")
	}

	return &empty.Empty{}, nil
}

type keyedItem interface {
	key() string
}

func (vpcService *vpcService) runFunctionUnderLongLivedLock(ctx context.Context, taskName string, itemLister func(context.Context) ([]keyedItem, error), workFunc func(context.Context, keyedItem)) error {
	startedLockers := sets.NewString()
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	t := time.NewTimer(timeBetweenTryingToAcquireLocks)
	defer t.Stop()
	for {
		items, err := itemLister(ctx)
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
						workFunc(ctx2, item)
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
