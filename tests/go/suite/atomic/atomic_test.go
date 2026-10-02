package atomic_test

import (
	"context"
	"sync"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	tests "github.com/romannikov/fdb-layer/tests/go"
	"github.com/romannikov/fdb-layer/tests/go/atomic"
	"google.golang.org/protobuf/proto"
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

func TestConcurrentSetAndCreate_DoesNotMutateCallerStruct(t *testing.T) {
	ctx := context.Background()
	kv := tests.NewMockKV()
	tr := tests.NewMockTransaction(kv)
	dir := &tests.MockDirectorySubspace{}
	recordStore := fdblayer.NewRecordStore()
	if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"Counter"}); err != nil {
		t.Fatalf("failed to sync metadata: %v", err)
	}
	counterRepo := atomic.NewCounterRepository(recordStore)

	shared := &atomic.Counter{
		Id:       "shared-counter",
		Value:    999,
		MaxValue: 5000,
		MinValue: 10,
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localKV := tests.NewMockKV()
			localTr := tests.NewMockTransaction(localKV)
			for j := 0; j < 200; j++ {
				_ = counterRepo.Set(ctx, localTr, dir, shared)
				if shared.Value != 999 || shared.MaxValue != 5000 || shared.MinValue != 10 {
					t.Errorf("caller struct mutated during concurrent Set: %+v", shared)
					return
				}
			}
		}()
	}
	wg.Wait()

	if shared.Value != 999 || shared.MaxValue != 5000 || shared.MinValue != 10 {
		t.Fatalf("caller struct corrupted after concurrent Set: got %+v", shared)
	}
}

func TestReadZerosAtomicFieldsBeforeFieldNamespace(t *testing.T) {
	ctx := context.Background()
	kv := tests.NewMockKV()
	tr := tests.NewMockTransaction(kv)
	dir := &tests.MockDirectorySubspace{}
	recordStore := fdblayer.NewRecordStore()
	if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"Counter"}); err != nil {
		t.Fatalf("failed to sync metadata: %v", err)
	}
	counterRepo := atomic.NewCounterRepository(recordStore)

	// Simulate a legacy/external write where DataNamespace contains non-zero atomic fields
	// in the protobuf blob while FieldNamespace has no keys.
	typeID, err := recordStore.GetTypeID("Counter")
	if err != nil {
		t.Fatalf("failed to get typeID: %v", err)
	}
	rawBytes, err := proto.Marshal(&atomic.Counter{
		Id:       "legacy",
		Value:    777,
		MaxValue: 888,
		MinValue: 999,
	})
	if err != nil {
		t.Fatalf("failed to marshal legacy counter: %v", err)
	}
	dataKey := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, "legacy"})
	tr.Set(dataKey, rawBytes)

	// Get must return 0 for all atomic fields since FieldNamespace is empty.
	got, err := counterRepo.Get(ctx, tr, dir, "legacy")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Value != 0 || got.MaxValue != 0 || got.MinValue != 0 {
		t.Fatalf("Get did not zero atomic fields from DataNamespace blob: got %+v", got)
	}

	// BatchGetCounter must also return 0 for all atomic fields.
	batchGot, err := counterRepo.BatchGetCounter(ctx, tr, dir, []tuple.Tuple{{"legacy"}})
	if err != nil {
		t.Fatalf("BatchGetCounter failed: %v", err)
	}
	if b := batchGot[`("legacy")`]; b == nil || b.Value != 0 || b.MaxValue != 0 || b.MinValue != 0 {
		t.Fatalf("BatchGetCounter did not zero atomic fields from DataNamespace blob: got %+v", b)
	}

	// ListCounter must also return 0 for all atomic fields.
	listGot, err := counterRepo.ListCounter(ctx, tr, dir, atomic.CounterPaginationOptions{})
	if err != nil {
		t.Fatalf("ListCounter failed: %v", err)
	}
	if len(listGot.Items) != 1 || listGot.Items[0].Value != 0 || listGot.Items[0].MaxValue != 0 || listGot.Items[0].MinValue != 0 {
		t.Fatalf("ListCounter did not zero atomic fields from DataNamespace blob: got %+v", listGot.Items)
	}
}

func TestDeleteCounter_ClearsFieldNamespace(t *testing.T) {
	ctx := context.Background()
	kv := tests.NewMockKV()
	tr := tests.NewMockTransaction(kv)
	dir := &tests.MockDirectorySubspace{}
	recordStore := fdblayer.NewRecordStore()
	if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"Counter"}); err != nil {
		t.Fatalf("failed to sync metadata: %v", err)
	}
	counterRepo := atomic.NewCounterRepository(recordStore)

	c := &atomic.Counter{
		Id:       "c_del",
		Value:    100,
		MaxValue: 500,
		MinValue: 10,
		U64Max:   900,
		I32Min:   -20,
		U32Add:   40,
	}
	if err := counterRepo.Create(ctx, tr, dir, c); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if err := counterRepo.AddCounterValue(ctx, tr, dir, "c_del", 50); err != nil {
		t.Fatalf("AddCounterValue failed: %v", err)
	}

	if err := counterRepo.Delete(ctx, tr, dir, "c_del"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Re-create the same primary key with zero values — stale FieldNamespace keys must not leak!
	if err := counterRepo.Create(ctx, tr, dir, &atomic.Counter{Id: "c_del"}); err != nil {
		t.Fatalf("re-Create failed: %v", err)
	}
	got, err := counterRepo.Get(ctx, tr, dir, "c_del")
	if err != nil {
		t.Fatalf("Get after re-Create failed: %v", err)
	}
	if got.Value != 0 || got.MaxValue != 0 || got.MinValue != 0 || got.U64Max != 0 || got.I32Min != 0 || got.U32Add != 0 {
		t.Fatalf("Delete did not clear FieldNamespace; stale atomic fields leaked into re-created counter: %+v", got)
	}
}

func TestAtomicMutations_UnsignedAnd32Bit(t *testing.T) {
	ctx := context.Background()
	kv := tests.NewMockKV()
	tr := tests.NewMockTransaction(kv)
	dir := &tests.MockDirectorySubspace{}
	recordStore := fdblayer.NewRecordStore()
	if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"Counter"}); err != nil {
		t.Fatalf("failed to sync metadata: %v", err)
	}
	counterRepo := atomic.NewCounterRepository(recordStore)

	c := &atomic.Counter{
		Id:     "c_types",
		U64Max: 1000,
		I32Min: -50,
		U32Add: 10,
	}
	if err := counterRepo.Create(ctx, tr, dir, c); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// 1. uint64 Max including values > math.MaxInt64 (high bit set)
	const highU64 uint64 = 1<<63 + 12345
	if err := counterRepo.MaxCounterU64Max(ctx, tr, dir, "c_types", 500); err != nil {
		t.Fatalf("MaxCounterU64Max(500) failed: %v", err)
	}
	got, _ := counterRepo.Get(ctx, tr, dir, "c_types")
	if got.U64Max != 1000 {
		t.Fatalf("expected U64Max 1000, got %d", got.U64Max)
	}
	if err := counterRepo.MaxCounterU64Max(ctx, tr, dir, "c_types", highU64); err != nil {
		t.Fatalf("MaxCounterU64Max(highU64) failed: %v", err)
	}
	got, _ = counterRepo.Get(ctx, tr, dir, "c_types")
	if got.U64Max != highU64 {
		t.Fatalf("expected U64Max %d, got %d", highU64, got.U64Max)
	}

	// 2. int32 Min with negative values
	if err := counterRepo.MinCounterI32Min(ctx, tr, dir, "c_types", -10); err != nil {
		t.Fatalf("MinCounterI32Min(-10) failed: %v", err)
	}
	got, _ = counterRepo.Get(ctx, tr, dir, "c_types")
	if got.I32Min != -50 {
		t.Fatalf("expected I32Min -50, got %d", got.I32Min)
	}
	if err := counterRepo.MinCounterI32Min(ctx, tr, dir, "c_types", -200); err != nil {
		t.Fatalf("MinCounterI32Min(-200) failed: %v", err)
	}
	got, _ = counterRepo.Get(ctx, tr, dir, "c_types")
	if got.I32Min != -200 {
		t.Fatalf("expected I32Min -200, got %d", got.I32Min)
	}

	// 3. uint32 Add
	if err := counterRepo.AddCounterU32Add(ctx, tr, dir, "c_types", 25); err != nil {
		t.Fatalf("AddCounterU32Add(25) failed: %v", err)
	}
	got, _ = counterRepo.Get(ctx, tr, dir, "c_types")
	if got.U32Add != 35 {
		t.Fatalf("expected U32Add 35, got %d", got.U32Add)
	}
}


