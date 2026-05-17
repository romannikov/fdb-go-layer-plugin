//go:build integration

package atomic_test

import (
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	fdblayer "github.com/romannikov/fdb-go-layer-plugin/fdb-layer"
	"github.com/romannikov/fdb-go-layer-plugin/tests"
	"github.com/romannikov/fdb-go-layer-plugin/tests/atomic"
)

func init() {
	fdb.MustAPIVersion(710)
}

func TestIntegration_AtomicMutations(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	counterRepo := atomic.NewCounterRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"Counter"})
	})

	// 1. Create a counter
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		c := &atomic.Counter{
			Id:       "c1",
			Value:    10,
			MaxValue: 100,
			MinValue: 5,
		}
		return counterRepo.Create(ctx, tr, dir, c)
	})

	// Verify initial state
	var retrieved *atomic.Counter
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, tr, dir, "c1")
		return err
	})
	if retrieved.Value != 10 || retrieved.MaxValue != 100 || retrieved.MinValue != 5 {
		t.Fatalf("unexpected initial state: %+v", retrieved)
	}

	// 2. Test Add
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.AddCounterValue(ctx, tr, dir, "c1", 5)
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, tr, dir, "c1")
		return err
	})
	if retrieved.Value != 15 {
		t.Fatalf("expected value 15, got %d", retrieved.Value)
	}

	// 3. Test Max
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.MaxCounterMaxValue(ctx, tr, dir, "c1", 50) // should not change
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, tr, dir, "c1")
		return err
	})
	if retrieved.MaxValue != 100 {
		t.Fatalf("expected max_value 100, got %d", retrieved.MaxValue)
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.MaxCounterMaxValue(ctx, tr, dir, "c1", 150) // should change
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, tr, dir, "c1")
		return err
	})
	if retrieved.MaxValue != 150 {
		t.Fatalf("expected max_value 150, got %d", retrieved.MaxValue)
	}

	// 4. Test Min
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.MinCounterMinValue(ctx, tr, dir, "c1", 10) // should not change
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, tr, dir, "c1")
		return err
	})
	if retrieved.MinValue != 5 {
		t.Fatalf("expected min_value 5, got %d", retrieved.MinValue)
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.MinCounterMinValue(ctx, tr, dir, "c1", 2) // should change
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, tr, dir, "c1")
		return err
	})
	if retrieved.MinValue != 2 {
		t.Fatalf("expected min_value 2, got %d", retrieved.MinValue)
	}
}
