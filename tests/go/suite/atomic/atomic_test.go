package atomic_test

import (
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	tests "github.com/romannikov/fdb-layer/tests/go"
	"github.com/romannikov/fdb-layer/tests/go/atomic"
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

	// 8. Verify negative int64 values on Create and Max/Min/Add mutations
	err = counterRepo.Create(ctx, tr, dir, &atomic.Counter{
		Id:       "c_neg",
		Value:    -10,
		MaxValue: -100,
		MinValue: -5,
	})
	if err != nil {
		t.Fatalf("failed to create negative counter: %v", err)
	}
	retrievedNeg, err := counterRepo.Get(ctx, tr, dir, "c_neg")
	if err != nil {
		t.Fatalf("failed to get c_neg: %v", err)
	}
	if retrievedNeg.Value != -10 || retrievedNeg.MaxValue != -100 || retrievedNeg.MinValue != -5 {
		t.Fatalf("unexpected initial negative state: %+v", retrievedNeg)
	}

	// Max: -200 < -100 (should stay -100), -50 > -100 (should become -50)
	_ = counterRepo.MaxCounterMaxValue(ctx, tr, dir, "c_neg", -200)
	retrievedNeg, _ = counterRepo.Get(ctx, tr, dir, "c_neg")
	if retrievedNeg.MaxValue != -100 {
		t.Fatalf("expected max_value -100 after -200, got %d", retrievedNeg.MaxValue)
	}
	_ = counterRepo.MaxCounterMaxValue(ctx, tr, dir, "c_neg", -50)
	retrievedNeg, _ = counterRepo.Get(ctx, tr, dir, "c_neg")
	if retrievedNeg.MaxValue != -50 {
		t.Fatalf("expected max_value -50 after -50, got %d", retrievedNeg.MaxValue)
	}

	// Min: -2 > -5 (should stay -5), -20 < -5 (should become -20)
	_ = counterRepo.MinCounterMinValue(ctx, tr, dir, "c_neg", -2)
	retrievedNeg, _ = counterRepo.Get(ctx, tr, dir, "c_neg")
	if retrievedNeg.MinValue != -5 {
		t.Fatalf("expected min_value -5 after -2, got %d", retrievedNeg.MinValue)
	}
	_ = counterRepo.MinCounterMinValue(ctx, tr, dir, "c_neg", -20)
	retrievedNeg, _ = counterRepo.Get(ctx, tr, dir, "c_neg")
	if retrievedNeg.MinValue != -20 {
		t.Fatalf("expected min_value -20 after -20, got %d", retrievedNeg.MinValue)
	}

	// Negative Max on positive counter c1 (currently MaxValue=150) must NOT overwrite 150
	_ = counterRepo.MaxCounterMaxValue(ctx, tr, dir, "c1", -1)
	retrievedC1, _ := counterRepo.Get(ctx, tr, dir, "c1")
	if retrievedC1.MaxValue != 150 {
		t.Fatalf("expected c1 max_value to stay 150 after Max(-1), got %d", retrievedC1.MaxValue)
	}
	// Negative Min on positive counter c1 (currently MinValue=2) MUST update to -10
	_ = counterRepo.MinCounterMinValue(ctx, tr, dir, "c1", -10)
	retrievedC1, _ = counterRepo.Get(ctx, tr, dir, "c1")
	if retrievedC1.MinValue != -10 {
		t.Fatalf("expected c1 min_value to become -10 after Min(-10), got %d", retrievedC1.MinValue)
	}
}
