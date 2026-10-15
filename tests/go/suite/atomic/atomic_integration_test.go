//go:build integration

package atomic_test

import (
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	tests "github.com/romannikov/fdb-layer/tests/go"
	"github.com/romannikov/fdb-layer/tests/go/atomic"
)

func init() {
	fdb.MustAPIVersion(710)
}

func TestIntegration_AtomicMutations(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	counterRepo := atomic.NewCounterRepository()

	// 1. Create a counter
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(tr, dir)
		c := &atomic.Counter{
			Id:       "c1",
			Value:    10,
			MaxValue: 100,
			MinValue: 5,
		}
		return counterRepo.Create(ctx, tx, c)
	})

	// Verify initial state
	var retrieved *atomic.Counter
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c1")
		return err
	})
	if retrieved.Value != 10 || retrieved.MaxValue != 100 || retrieved.MinValue != 5 {
		t.Fatalf("unexpected initial state: %+v", retrieved)
	}

	// 2. Test Add
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.AddValue(ctx, fdblayer.NewTx(tr, dir), "c1", 5)
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c1")
		return err
	})
	if retrieved.Value != 15 {
		t.Fatalf("expected value 15, got %d", retrieved.Value)
	}

	// 3. Test Max
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.MaxMaxValue(ctx, fdblayer.NewTx(tr, dir), "c1", 50) // should not change
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c1")
		return err
	})
	if retrieved.MaxValue != 100 {
		t.Fatalf("expected max_value 100, got %d", retrieved.MaxValue)
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.MaxMaxValue(ctx, fdblayer.NewTx(tr, dir), "c1", 150) // should change
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c1")
		return err
	})
	if retrieved.MaxValue != 150 {
		t.Fatalf("expected max_value 150, got %d", retrieved.MaxValue)
	}

	// 4. Test Min
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.MinMinValue(ctx, fdblayer.NewTx(tr, dir), "c1", 10) // should not change
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c1")
		return err
	})
	if retrieved.MinValue != 5 {
		t.Fatalf("expected min_value 5, got %d", retrieved.MinValue)
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.MinMinValue(ctx, fdblayer.NewTx(tr, dir), "c1", 2) // should change
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c1")
		return err
	})
	if retrieved.MinValue != 2 {
		t.Fatalf("expected min_value 2, got %d", retrieved.MinValue)
	}

	// 5. Verify BatchGetCounter populates atomic fields
	var batchRes []*atomic.Counter
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		batchRes, err = counterRepo.BatchGet(ctx, fdblayer.NewTx(tr, dir), []string{"c1"})
		return err
	})
	if len(batchRes) != 1 || batchRes[0] == nil || batchRes[0].Value != 15 || batchRes[0].MaxValue != 150 || batchRes[0].MinValue != 2 {
		t.Fatalf("BatchGetCounter did not populate atomic fields: %+v", batchRes)
	}

	// 6. Verify ListCounter populates atomic fields
	var listRes *fdblayer.PaginatedResult[*atomic.Counter]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		listRes, err = counterRepo.List(ctx, fdblayer.NewTx(tr, dir), fdblayer.PaginationOptions{Limit: 10})
		return err
	})
	if len(listRes.Items) != 1 {
		t.Fatalf("expected 1 counter in ListCounter, got %d", len(listRes.Items))
	}
	listItem := listRes.Items[0]
	if listItem.Value != 15 || listItem.MaxValue != 150 || listItem.MinValue != 2 {
		t.Fatalf("ListCounter did not populate atomic fields: %+v", listItem)
	}

	// 7. Verify Set does not overwrite atomic fields in FieldNamespace (C-2)
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.Set(ctx, fdblayer.NewTx(tr, dir), &atomic.Counter{Id: "c1"})
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c1")
		return err
	})
	if retrieved.Value != 15 || retrieved.MaxValue != 150 || retrieved.MinValue != 2 {
		t.Fatalf("Set overwrote atomic fields: %+v", retrieved)
	}

	// 8. Verify Create with default zero MinValue allows positive Min mutations (C-3)
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.Create(ctx, fdblayer.NewTx(tr, dir), &atomic.Counter{Id: "c_zero"})
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.MinMinValue(ctx, fdblayer.NewTx(tr, dir), "c_zero", 42)
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c_zero")
		return err
	})
	if retrieved.MinValue != 42 {
		t.Fatalf("expected min_value 42 on zero-initialized counter, got %d", retrieved.MinValue)
	}

	// 9. Verify negative int64 values on Create and Max/Min mutations against live FDB
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.Create(ctx, fdblayer.NewTx(tr, dir), &atomic.Counter{
			Id:       "c_neg",
			Value:    -10,
			MaxValue: -100,
			MinValue: -5,
		})
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c_neg")
		return err
	})
	if retrieved.Value != -10 || retrieved.MaxValue != -100 || retrieved.MinValue != -5 {
		t.Fatalf("unexpected initial negative state: %+v", retrieved)
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(tr, dir)
		if err := counterRepo.MaxMaxValue(ctx, tx, "c_neg", -200); err != nil {
			return err
		}
		if err := counterRepo.MaxMaxValue(ctx, tx, "c_neg", -50); err != nil {
			return err
		}
		if err := counterRepo.MinMinValue(ctx, tx, "c_neg", -2); err != nil {
			return err
		}
		if err := counterRepo.MinMinValue(ctx, tx, "c_neg", -20); err != nil {
			return err
		}
		if err := counterRepo.MaxMaxValue(ctx, tx, "c1", -1); err != nil {
			return err
		}
		return counterRepo.MinMinValue(ctx, tx, "c1", -10)
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c_neg")
		return err
	})
	if retrieved.MaxValue != -50 || retrieved.MinValue != -20 {
		t.Fatalf("unexpected c_neg after negative Max/Min: %+v", retrieved)
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		retrieved, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c1")
		return err
	})
	if retrieved.MaxValue != 150 || retrieved.MinValue != -10 {
		t.Fatalf("unexpected c1 after negative Max/Min: %+v", retrieved)
	}
}

func TestIntegration_DeleteAndUnsigned32BitAtomic(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	counterRepo := atomic.NewCounterRepository()

	const highU64 uint64 = 1<<63 + 999
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.Create(ctx, fdblayer.NewTx(tr, dir), &atomic.Counter{
			Id:       "c_live",
			Value:    42,
			MaxValue: 100,
			MinValue: 10,
			U64Max:   500,
			I32Min:   -10,
			U32Add:   7,
		})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(tr, dir)
		if err := counterRepo.MaxU64Max(ctx, tx, "c_live", highU64); err != nil {
			return err
		}
		if err := counterRepo.MinI32Min(ctx, tx, "c_live", -300); err != nil {
			return err
		}
		return counterRepo.AddU32Add(ctx, tx, "c_live", 13)
	})

	var got *atomic.Counter
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c_live")
		return err
	})
	if got.U64Max != highU64 || got.I32Min != -300 || got.U32Add != 20 {
		t.Fatalf("unexpected unsigned/32-bit atomic values: %+v", got)
	}

	// Delete and re-create with zero values to verify FieldNamespace was cleared
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.Delete(ctx, fdblayer.NewTx(tr, dir), "c_live")
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.Create(ctx, fdblayer.NewTx(tr, dir), &atomic.Counter{Id: "c_live"})
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c_live")
		return err
	})
	if got.Value != 0 || got.MaxValue != 0 || got.MinValue != 0 || got.U64Max != 0 || got.I32Min != 0 || got.U32Add != 0 {
		t.Fatalf("Delete did not clear FieldNamespace in live FDB: %+v", got)
	}

	// Verify BUG-1 in live FDB:
	// 1. Set upsert with MaxValue=0 writes signed bias so Max(-100) stays 0
	// 2. Set with non-zero atomic fields from a prior Get does NOT clobber concurrent Add
	// 3. Explicit SetCounter* resets atomic fields to 0
	// 4. ListCounter with Reverse: true populates FieldNamespace when firstPK > lastPK
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return counterRepo.Set(ctx, fdblayer.NewTx(tr, dir), &atomic.Counter{
			Id:       "c_rmw",
			Value:    20,
			MaxValue: 0,
			MinValue: 10,
		})
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(tr, dir)
		var err error
		got, err = counterRepo.Get(ctx, tx, "c_rmw")
		if err != nil {
			return err
		}
		if err := counterRepo.AddValue(ctx, tx, "c_rmw", 30); err != nil {
			return err
		}
		if err := counterRepo.MaxMaxValue(ctx, tx, "c_rmw", -100); err != nil {
			return err
		}
		// got.Value is still 20, while FieldNamespace has 50
		return counterRepo.Set(ctx, tx, got)
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = counterRepo.Get(ctx, fdblayer.NewTx(tr, dir), "c_rmw")
		return err
	})
	if got.Value != 50 || got.MaxValue != 0 || got.MinValue != 10 {
		t.Fatalf("expected {Value:50, MaxValue:0, MinValue:10} after RMW Set in live FDB, got %+v", got)
	}

	// Reset Value to 0 via SetCounterValue and set c_live Value to 77
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(tr, dir)
		if err := counterRepo.SetValue(ctx, tx, "c_rmw", 0); err != nil {
			return err
		}
		return counterRepo.SetValue(ctx, tx, "c_live", 77)
	})

	var revList *fdblayer.PaginatedResult[*atomic.Counter]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		revList, err = counterRepo.List(ctx, fdblayer.NewTx(tr, dir), fdblayer.PaginationOptions{
			Limit:   10,
			Reverse: true,
		})
		return err
	})
	if len(revList.Items) != 2 || revList.Items[0].Id != "c_rmw" || revList.Items[0].Value != 0 || revList.Items[0].MinValue != 10 || revList.Items[1].Id != "c_live" || revList.Items[1].Value != 77 {
		t.Fatalf("unexpected ListCounter(Reverse:true) result in live FDB: %+v", revList.Items)
	}
}

