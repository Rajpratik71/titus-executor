package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Netflix/titus-executor/logger"
	"github.com/pkg/errors"
)

const (
	maxPreemption = 10 * time.Minute
)

// TODO: Write a proper version of tryToAcquireLock which safely preempts the lock
func (vpcService *vpcService) preemptLock(ctx context.Context, item keyedItem, llt longLivedTask) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("Deadline must be set")
	}
	if deadline.After(time.Now().Add(maxPreemption)) {
		return fmt.Errorf("Max preemption time %s exceeded", maxPreemption)
	}

	tx, err := vpcService.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		err = errors.Wrap(err, "Could not start database transaction")
		return err
	}

	defer func() {
		_ = tx.Rollback()
	}()

	lockName := generateLockName(llt.taskName, item)
	row := tx.QueryRowContext(ctx, "SELECT held_until, held_by FROM long_lived_locks WHERE lock_name = $1", lockName)
	var heldBy string
	var heldUntil time.Time
	err = row.Scan(&heldUntil, &heldBy)
	if err == sql.ErrNoRows {
		heldUntil = time.Now()
	} else if err != nil {
		err = errors.Wrap(err, "Cannot get lock held until")
		return err
	} else {
		ctx = logger.WithFields(ctx, map[string]interface{}{
			"previouslyHeldBy":    heldBy,
			"previouslyHeldUntil": heldUntil,
		})
	}

	row = tx.QueryRowContext(ctx, `
INSERT INTO long_lived_locks(lock_name, held_by, held_until)
VALUES ($1, $2, $3) ON CONFLICT (lock_name) DO
UPDATE
SET held_by = $2,
    held_until = $3
RETURNING id
`, lockName, vpcService.hostname, deadline)

	var id int
	err = row.Scan(&id)
	if err != nil {
		err = errors.Wrap(err, "Unable to scan row for lock preemption query")
		return err
	}
	logger.G(ctx).WithField("id", id).WithField("lockName", lockName).Info("Preempted lock")

	err = tx.Commit()
	if err != nil {
		err = errors.Wrap(err, "Could not commit transaction")
		return err
	}

	if heldBy == vpcService.hostname {
		logger.G(ctx).Debug("Lock previously held by us, assuming we can use it right away")
		return nil
	}

	// This is "suboptimal" in the sense that the lock will actually be knocked out by lockTime / 4 --
	// since runUnderLock checks every lockTime / 4 if it still holds the lock
	timer := time.NewTimer(time.Until(heldUntil))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
