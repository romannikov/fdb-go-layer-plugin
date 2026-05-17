//go:build integration

package store

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/romannikov/fdb-go-layer-plugin/tests/atomic"
)

func TestIntegration_AtomicMutations(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	recordStore := atomic.NewRecordStore()
	withTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(tr, dir)
	})

	counterRepo := atomic.NewCounterRepository(recordStore)

	// 1. Create a counter
	withTx(t, db, func(tr fdb.Transaction) error {
		c := &atomic.Counter{
			Id:       "c1",
			Value:    10,
			MaxValue: 100,
			MinValue: 5,
		}
		return counterRepo.Create(tr, dir, c)
	})

	// Verify initial state
	var retrieved *atomic.Counter
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(tr, dir, tuple.Tuple{"c1"})
		return err
	})
	if retrieved.Value != 10 || retrieved.MaxValue != 100 || retrieved.MinValue != 5 {
		t.Fatalf("unexpected initial state: %+v", retrieved)
	}

	// 2. Test Add
	withTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.AddCounterValue(tr, dir, "c1", 5)
	})

	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(tr, dir, tuple.Tuple{"c1"})
		return err
	})
	if retrieved.Value != 15 {
		t.Fatalf("expected value 15, got %d", retrieved.Value)
	}

	// 3. Test Max
	withTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.MaxCounterMaxValue(tr, dir, "c1", 50) // should not change
	})
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(tr, dir, tuple.Tuple{"c1"})
		return err
	})
	if retrieved.MaxValue != 100 {
		t.Fatalf("expected max_value 100, got %d", retrieved.MaxValue)
	}

	withTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.MaxCounterMaxValue(tr, dir, "c1", 150) // should change
	})
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(tr, dir, tuple.Tuple{"c1"})
		return err
	})
	if retrieved.MaxValue != 150 {
		t.Fatalf("expected max_value 150, got %d", retrieved.MaxValue)
	}

	// 4. Test Min
	withTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.MinCounterMinValue(tr, dir, "c1", 10) // should not change
	})
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(tr, dir, tuple.Tuple{"c1"})
		return err
	})
	if retrieved.MinValue != 5 {
		t.Fatalf("expected min_value 5, got %d", retrieved.MinValue)
	}

	withTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.MinCounterMinValue(tr, dir, "c1", 2) // should change
	})
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(tr, dir, tuple.Tuple{"c1"})
		return err
	})
	if retrieved.MinValue != 2 {
		t.Fatalf("expected min_value 2, got %d", retrieved.MinValue)
	}
}
