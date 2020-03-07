package service

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	vpcapi "github.com/Netflix/titus-executor/vpc/api"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var longLivedLockMockRows = sqlmock.NewRows([]string{"id", "lock_name", "held_by", "held_until"})

func generateLockAndRows(t *testing.T, mock sqlmock.Sqlmock) (*vpcapi.Lock, *sqlmock.Rows) {
	heldUntil := time.Now()
	protoHeldUntil, err := ptypes.TimestampProto(heldUntil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lock := &vpcapi.Lock{
		Id:        8222250,
		LockName:  "branch_eni_associate_nilitem",
		HeldBy:    "titusvpcservice-cell-instance",
		HeldUntil: protoHeldUntil,
	}

	rows := longLivedLockMockRows.AddRow(
		lock.GetId(),
		lock.GetLockName(),
		lock.GetHeldBy(),
		heldUntil,
	)

	return lock, rows
}

func TestAPIShouldGetLocks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("could not open mock db: %v", err)
	}
	defer db.Close()

	expected, rows := generateLockAndRows(t, mock)
	mock.ExpectQuery("SELECT id, lock_name, held_by, held_until FROM long_lived_locks LIMIT 1000").WillReturnRows(rows)

	service := vpcService{db: db}

	ctx := context.Background()
	res, err := service.GetLocks(ctx, &empty.Empty{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := res.GetLocks()[0]
	if !proto.Equal(expected, got) {
		t.Fatalf("expected: %v, got %v", expected, got)
	}
}

func TestAPIShouldGetLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("could not open mock db: %v", err)
	}
	defer db.Close()

	expected, rows := generateLockAndRows(t, mock)
	mock.ExpectQuery("SELECT id, lock_name, held_by, held_until FROM long_lived_locks WHERE id = \\$1").WithArgs(expected.GetId()).WillReturnRows(rows)

	service := vpcService{db: db}

	ctx := context.Background()
	got, err := service.GetLock(ctx, &vpcapi.LockId{Id: expected.GetId()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !proto.Equal(expected, got) {
		t.Fatalf("expected: %v, got %v", expected, got)
	}
}

func TestAPIGetLockNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("could not open mock db: %v", err)
	}
	defer db.Close()

	id := int64(1)
	mock.ExpectQuery("SELECT id, lock_name, held_by, held_until FROM long_lived_locks WHERE id = \\$1").WithArgs(id).WillReturnRows(longLivedLockMockRows)

	service := vpcService{db: db}

	ctx := context.Background()
	_, err = service.GetLock(ctx, &vpcapi.LockId{Id: id})
	if err == nil {
		t.Fatal("expected error but got nil")
	}

	stat := status.Convert(err)

	got := stat.Code()
	expected := codes.NotFound
	if expected != got {
		t.Fatalf("expected: %v, got %v", codes.NotFound, got)
	}
}

func TestAPIShouldDeleteLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("could not open mock db: %v", err)
	}
	defer db.Close()

	service := vpcService{db: db}
	ctx := context.Background()

	id := int64(123)
	mock.ExpectExec("DELETE FROM long_lived_locks WHERE id = \\$1").WithArgs(id).WillReturnResult(sqlmock.NewResult(1, 1))

	_, err = service.DeleteLock(ctx, &vpcapi.LockId{Id: id})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAPIDeleteLockNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("could not open mock db: %v", err)
	}
	defer db.Close()

	id := int64(123)
	mock.ExpectExec("DELETE FROM long_lived_locks WHERE id = \\$1").WithArgs(id).WillReturnResult(sqlmock.NewResult(0, 0))

	service := vpcService{db: db}

	ctx := context.Background()
	_, err = service.DeleteLock(ctx, &vpcapi.LockId{Id: id})
	if err == nil {
		t.Fatal("expected error but got nil")
	}

	stat := status.Convert(err)

	got := stat.Code()
	expected := codes.NotFound
	if expected != got {
		t.Fatalf("expected: %v, got %v", codes.NotFound, got)
	}
}
