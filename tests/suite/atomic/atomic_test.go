package atomic_test

import (
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	fdblayer "github.com/romannikov/fdb-go-layer-plugin/fdb-layer"
	"github.com/romannikov/fdb-go-layer-plugin/tests"
	"github.com/romannikov/fdb-go-layer-plugin/tests/atomic"
)

func TestAtomicMutations(t *testing.T) {
	ctx := context.Background()
	kv := tests.NewMockKV()
	tr := tests.NewMockTransaction(kv)
	dir := &tests.MockDirectorySubspace{}
	recordStore := fdblayer.NewRecordStore()
	err := recordStore.SyncMetadata(ctx, tr, dir, []string{"Counter"})
	if err != nil {
		t.Fatalf("failed to sync metadata: %v", err)
	}

	counterRepo := atomic.NewCounterRepository(recordStore)

	// 1. Create a counter
	c := &atomic.Counter{
		Id:       "c1",
		Value:    10,
		MaxValue: 100,
		MinValue: 5,
	}

	err = counterRepo.Create(ctx, tr, dir, c)
	if err != nil {
		t.Fatalf("failed to create counter: %v", err)
	}

	// Verify initial state
	retrieved, err := counterRepo.Get(ctx, tr, dir, "c1")
	if err != nil {
		t.Fatalf("failed to get counter: %v", err)
	}
	if retrieved.Value != 10 || retrieved.MaxValue != 100 || retrieved.MinValue != 5 {
		t.Fatalf("unexpected initial state: %+v", retrieved)
	}

	// 2. Test Add
	err = counterRepo.AddCounterValue(ctx, tr, dir, "c1", 5)
	if err != nil {
		t.Fatalf("failed to add value: %v", err)
	}

	retrieved, _ = counterRepo.Get(ctx, tr, dir, "c1")
	if retrieved.Value != 15 {
		t.Fatalf("expected value 15, got %d", retrieved.Value)
	}

	// 3. Test Max
	err = counterRepo.MaxCounterMaxValue(ctx, tr, dir, "c1", 50) // should not change
	if err != nil {
		t.Fatalf("failed to max value: %v", err)
	}
	retrieved, _ = counterRepo.Get(ctx, tr, dir, "c1")
	if retrieved.MaxValue != 100 {
		t.Fatalf("expected max_value 100, got %d", retrieved.MaxValue)
	}

	err = counterRepo.MaxCounterMaxValue(ctx, tr, dir, "c1", 150) // should change
	if err != nil {
		t.Fatalf("failed to max value: %v", err)
	}
	retrieved, _ = counterRepo.Get(ctx, tr, dir, "c1")
	if retrieved.MaxValue != 150 {
		t.Fatalf("expected max_value 150, got %d", retrieved.MaxValue)
	}

	// 4. Test Min
	err = counterRepo.MinCounterMinValue(ctx, tr, dir, "c1", 10) // should not change
	if err != nil {
		t.Fatalf("failed to min value: %v", err)
	}
	retrieved, _ = counterRepo.Get(ctx, tr, dir, "c1")
	if retrieved.MinValue != 5 {
		t.Fatalf("expected min_value 5, got %d", retrieved.MinValue)
	}

	err = counterRepo.MinCounterMinValue(ctx, tr, dir, "c1", 2) // should change
	if err != nil {
		t.Fatalf("failed to min value: %v", err)
	}
	retrieved, _ = counterRepo.Get(ctx, tr, dir, "c1")
	if retrieved.MinValue != 2 {
		t.Fatalf("expected min_value 2, got %d", retrieved.MinValue)
	}

	// Verify that the generated CounterRepository can be instantiated and used
	var counterRepoInterface atomic.CounterRepository = counterRepo
	var genCounterRepo fdblayer.GenericRepository[*atomic.Counter, string] = counterRepoInterface

	// Test Get via GenericRepository interface
	genRetrieved, err := genCounterRepo.Get(ctx, tr, dir, "c1")
	if err != nil {
		t.Fatalf("generic counter Get failed: %v", err)
	}
	if genRetrieved.MinValue != 2 {
		t.Fatalf("unexpected min_value retrieved: %d", genRetrieved.MinValue)
	}

	// 5. Verify BatchGetCounter populates atomic fields
	batchRes, err := counterRepo.BatchGetCounter(ctx, tr, dir, []tuple.Tuple{{"c1"}})
	if err != nil {
		t.Fatalf("BatchGetCounter failed: %v", err)
	}
	batchItem, ok := batchRes[`("c1")`]
	if !ok || batchItem == nil {
		t.Fatalf("expected c1 in BatchGetCounter result, got: %+v", batchRes)
	}
	if batchItem.Value != 15 || batchItem.MaxValue != 150 || batchItem.MinValue != 2 {
		t.Fatalf("BatchGetCounter did not populate atomic fields: got %+v", batchItem)
	}

	// 6. Verify Set does not overwrite atomic fields in FieldNamespace (C-2)
	err = counterRepo.Set(ctx, tr, dir, &atomic.Counter{Id: "c1"})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	retrievedAfterSet, err := counterRepo.Get(ctx, tr, dir, "c1")
	if err != nil {
		t.Fatalf("Get after Set failed: %v", err)
	}
	if retrievedAfterSet.Value != 15 || retrievedAfterSet.MaxValue != 150 || retrievedAfterSet.MinValue != 2 {
		t.Fatalf("Set overwrote atomic fields: got %+v", retrievedAfterSet)
	}

	// 7. Verify Create with default zero MinValue allows positive Min mutations (C-3)
	err = counterRepo.Create(ctx, tr, dir, &atomic.Counter{Id: "c_zero"})
	if err != nil {
		t.Fatalf("failed to create zero-initialized counter: %v", err)
	}
	err = counterRepo.MinCounterMinValue(ctx, tr, dir, "c_zero", 42)
	if err != nil {
		t.Fatalf("failed to apply Min on zero-initialized counter: %v", err)
	}
	retrievedZero, err := counterRepo.Get(ctx, tr, dir, "c_zero")
	if err != nil {
		t.Fatalf("failed to get c_zero: %v", err)
	}
	if retrievedZero.MinValue != 42 {
		t.Fatalf("expected min_value 42 on zero-initialized counter, got %d", retrievedZero.MinValue)
	}
}

