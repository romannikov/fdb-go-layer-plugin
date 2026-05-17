package atomic

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/romannikov/fdb-go-layer-plugin/tests"
)

func TestAtomicMutations(t *testing.T) {
	kv := store.NewMockKV()
	tr := store.NewMockTransaction(kv)
	dir := &store.MockDirectorySubspace{}
	recordStore := NewRecordStore()
	err := recordStore.SyncMetadata(tr, dir)
	if err != nil {
		t.Fatalf("failed to sync metadata: %v", err)
	}

	// 1. Create a counter
	c := &Counter{
		Id:       "c1",
		Value:    10,
		MaxValue: 100,
		MinValue: 5,
	}

	err = recordStore.CreateCounter(tr, dir, c)
	if err != nil {
		t.Fatalf("failed to create counter: %v", err)
	}

	// Verify initial state
	retrieved, err := recordStore.GetCounter(tr, dir, "c1")
	if err != nil {
		t.Fatalf("failed to get counter: %v", err)
	}
	if retrieved.Value != 10 || retrieved.MaxValue != 100 || retrieved.MinValue != 5 {
		t.Fatalf("unexpected initial state: %+v", retrieved)
	}

	// 2. Test Add
	err = recordStore.AddCounterValue(tr, dir, "c1", 5)
	if err != nil {
		t.Fatalf("failed to add value: %v", err)
	}

	retrieved, _ = recordStore.GetCounter(tr, dir, "c1")
	if retrieved.Value != 15 {
		t.Fatalf("expected value 15, got %d", retrieved.Value)
	}

	// 3. Test Max
	err = recordStore.MaxCounterMaxValue(tr, dir, "c1", 50) // should not change
	if err != nil {
		t.Fatalf("failed to max value: %v", err)
	}
	retrieved, _ = recordStore.GetCounter(tr, dir, "c1")
	if retrieved.MaxValue != 100 {
		t.Fatalf("expected max_value 100, got %d", retrieved.MaxValue)
	}

	err = recordStore.MaxCounterMaxValue(tr, dir, "c1", 150) // should change
	if err != nil {
		t.Fatalf("failed to max value: %v", err)
	}
	retrieved, _ = recordStore.GetCounter(tr, dir, "c1")
	if retrieved.MaxValue != 150 {
		t.Fatalf("expected max_value 150, got %d", retrieved.MaxValue)
	}

	// 4. Test Min
	err = recordStore.MinCounterMinValue(tr, dir, "c1", 10) // should not change
	if err != nil {
		t.Fatalf("failed to min value: %v", err)
	}
	retrieved, _ = recordStore.GetCounter(tr, dir, "c1")
	if retrieved.MinValue != 5 {
		t.Fatalf("expected min_value 5, got %d", retrieved.MinValue)
	}

	err = recordStore.MinCounterMinValue(tr, dir, "c1", 2) // should change
	if err != nil {
		t.Fatalf("failed to min value: %v", err)
	}
	retrieved, _ = recordStore.GetCounter(tr, dir, "c1")
	if retrieved.MinValue != 2 {
		t.Fatalf("expected min_value 2, got %d", retrieved.MinValue)
	}
}
