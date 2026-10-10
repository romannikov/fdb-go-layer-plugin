package basic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"google.golang.org/protobuf/proto"

	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	tests "github.com/romannikov/fdb-layer/tests/go"
	"github.com/romannikov/fdb-layer/tests/go/store"
)

// RecordStore & Metadata Tests

func TestSyncMetadata_FreshStore(t *testing.T) {
	recordStore, _, _, kv := tests.SyncAndSetup()

	meta := recordStore.Metadata()
	if meta["User"] == 0 || meta["Product"] == 0 {
		t.Fatalf("metadata not populated: %v", meta)
	}
	if meta["User"] == meta["Product"] {
		t.Fatalf("User and Product got the same type ID")
	}
	// Four metadata keys should have been written
	if len(kv.Snapshot()) != 4 {
		t.Fatalf("expected 4 keys written, got %d", len(kv.Snapshot()))
	}
}

func TestSyncMetadata_Idempotent(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	firstMeta := recordStore.Metadata()

	// Sync again — IDs should remain stable, no extra writes
	err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"})
	if err != nil {
		t.Fatalf("second SyncMetadata failed: %v", err)
	}
	secondMeta := recordStore.Metadata()
	if firstMeta["User"] != secondMeta["User"] || firstMeta["Product"] != secondMeta["Product"] {
		t.Fatalf("IDs changed on second sync: %v → %v", firstMeta, secondMeta)
	}
}

func TestMetadata_ReturnsCopy(t *testing.T) {
	recordStore, _, _, _ := tests.SyncAndSetup()
	m := recordStore.Metadata()
	m["User"] = 9999
	if recordStore.Metadata()["User"] == 9999 {
		t.Fatal("Metadata() returned a reference, not a copy")
	}
}

func TestNewRecordStore_EmptyMetadata(t *testing.T) {
	recordStore := fdblayer.NewRecordStore()
	m := recordStore.Metadata()
	if len(m) != 0 {
		t.Fatalf("expected empty metadata, got %v", m)
	}
}

func TestSyncMetadata_TransactionRetry(t *testing.T) {
	ctx := context.Background()
	recordStore := fdblayer.NewRecordStore()
	dir := &tests.MockDirectorySubspace{}

	// Attempt 1: populates in-memory recordStore, but its transactionKV is discarded (simulating FDB conflict).
	abortedKV := tests.NewMockKV()
	abortedTr := tests.NewMockTransaction(abortedKV)
	if err := recordStore.SyncMetadata(ctx, abortedTr, dir, []string{"User", "Product"}); err != nil {
		t.Fatalf("first attempt SyncMetadata failed: %v", err)
	}

	// Attempt 2: runs on a fresh committed KV. It must still write the metadata keys to committedKV.
	committedKV := tests.NewMockKV()
	committedTr := tests.NewMockTransaction(committedKV)
	if err := recordStore.SyncMetadata(ctx, committedTr, dir, []string{"User", "Product"}); err != nil {
		t.Fatalf("retry SyncMetadata failed: %v", err)
	}

	if len(committedKV.Snapshot()) != 2 {
		t.Fatalf("expected 2 metadata keys written on retry, got %d", len(committedKV.Snapshot()))
	}
}

func TestSyncMetadata_IncrementalAndCancelSafe(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()

	if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"Order"}); err != nil {
		t.Fatalf("incremental SyncMetadata failed: %v", err)
	}
	meta := recordStore.Metadata()
	if meta["User"] != 1 || meta["Order"] != 5 {
		t.Fatalf("expected User=1 and Order=5 after incremental SyncMetadata, got %v", meta)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := recordStore.SyncMetadata(cancelCtx, tr, dir, []string{"AuditLog"}); err == nil {
		t.Fatal("expected error for canceled context")
	}
	if recordStore.Metadata()["User"] != 1 || recordStore.Metadata()["Order"] != 5 {
		t.Fatalf("canceled SyncMetadata corrupted existing metadata: %v", recordStore.Metadata())
	}
}

// CRUD Tests

// Create
func TestCreateUser_Success(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	user := &store.User{Id: "u1", Name: "Alice", Email: "alice@example.com"}

	repo := store.NewUserRepository(recordStore)
	if err := repo.Create(ctx, tr, dir, user); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	typeID := recordStore.Metadata()["User"]
	// Primary key
	pk := tuple.Tuple{typeID, fdblayer.DataNamespace, "u1"}.Pack()
	if !kv.HasKey(pk) {
		t.Fatal("primary key not written")
	}
	// Index key
	ik := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(2324124615), "alice@example.com", "u1"}.Pack()
	if !kv.HasKey(ik) {
		t.Fatal("index key not written")
	}
	// Deserialize and verify
	val := kv.Get(pk)
	got := &store.User{}
	if err := proto.Unmarshal(val, got); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got.Id != "u1" || got.Name != "Alice" || got.Email != "alice@example.com" {
		t.Fatalf("unexpected user: %+v", got)
	}
}

func TestCreateProduct_Success(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	p := &store.Product{Id: "p1", Name: "Widget", Category: "tools", Price: 42}

	repo := store.NewProductRepository(recordStore)
	if err := repo.Create(ctx, tr, dir, p); err != nil {
		t.Fatalf("CreateProduct failed: %v", err)
	}

	typeID := recordStore.Metadata()["Product"]
	pk := tuple.Tuple{typeID, fdblayer.DataNamespace, "p1"}.Pack()
	if !kv.HasKey(pk) {
		t.Fatal("primary key not written")
	}
	ik := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(3475980913), "tools", "p1"}.Pack()
	if !kv.HasKey(ik) {
		t.Fatal("category index key not written")
	}
	val := kv.Get(pk)
	got := &store.Product{}
	if err := proto.Unmarshal(val, got); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got.Price != 42 {
		t.Fatalf("expected price 42, got %d", got.Price)
	}
}

func TestCreateUser_AlreadyExists(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	repo := store.NewUserRepository(recordStore)

	first := &store.User{Id: "u1", Name: "Alice", Email: "alice@example.com"}
	if err := repo.Create(ctx, tr, dir, first); err != nil {
		t.Fatalf("initial Create failed: %v", err)
	}

	// Attempting to Create a second time with the same PK and a different email
	// must fail with fdblayer.ErrAlreadyExists and leave both the primary record
	// and secondary index untouched.
	duplicate := &store.User{Id: "u1", Name: "Bob", Email: "bob@example.com"}
	err := repo.Create(ctx, tr, dir, duplicate)
	if !errors.Is(err, fdblayer.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists on duplicate Create, got: %v", err)
	}

	got, err := repo.Get(ctx, tr, dir, "u1")
	if err != nil {
		t.Fatalf("Get after rejected Create failed: %v", err)
	}
	if got.Name != "Alice" || got.Email != "alice@example.com" {
		t.Fatalf("existing record was overwritten by duplicate Create: %+v", got)
	}

	typeID := recordStore.Metadata()["User"]
	bobIdx := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(2324124615), "bob@example.com", "u1"}.Pack()
	if kv.HasKey(bobIdx) {
		t.Fatal("duplicate Create must not write secondary index entry")
	}
}

func TestCreate_BeforeSync(t *testing.T) {
	ctx := context.Background()
	kv := tests.NewMockKV()
	tr := tests.NewMockTransaction(kv)
	dir := &tests.MockDirectorySubspace{}
	recordStore := fdblayer.NewRecordStore() // no SyncMetadata

	repo := store.NewUserRepository(recordStore)
	err := repo.Create(ctx, tr, dir, &store.User{Id: "u1"})
	if err == nil {
		t.Fatal("expected error when metadata not synced")
	}
	if !strings.Contains(err.Error(), "not found in metadata") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Get
func TestGetUser_Success(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	user := &store.User{Id: "u1", Name: "Alice", Email: "alice@example.com"}
	repo := store.NewUserRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, user)

	got, err := repo.Get(ctx, tr, dir, "u1")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	if got.Name != "Alice" || got.Email != "alice@example.com" {
		t.Fatalf("unexpected user: %+v", got)
	}
}

func TestGetUser_NotFound(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()

	repo := store.NewUserRepository(recordStore)
	_, err := repo.Get(ctx, tr, dir, "nonexistent")
	if err == nil {
		t.Fatal("expected not found error")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetProduct_Success(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	repo := store.NewProductRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.Product{Id: "p1", Name: "Gizmo", Category: "tech", Price: 99})

	got, err := repo.Get(ctx, tr, dir, "p1")
	if err != nil {
		t.Fatalf("GetProduct failed: %v", err)
	}
	if got.Name != "Gizmo" || got.Price != 99 {
		t.Fatalf("unexpected product: %+v", got)
	}
}

func TestGet_BeforeSync(t *testing.T) {
	ctx := context.Background()
	recordStore := fdblayer.NewRecordStore()
	kv := tests.NewMockKV()
	tr := tests.NewMockTransaction(kv)
	dir := &tests.MockDirectorySubspace{}

	repo := store.NewUserRepository(recordStore)
	_, err := repo.Get(ctx, tr, dir, "u1")
	if err == nil {
		t.Fatal("expected error")
	}
}

// Set (Update)
func TestSetUser_UpdateFields(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	repo := store.NewUserRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})

	updated := &store.User{Id: "u1", Name: "Bob", Email: "b@test.com"}
	if err := repo.Set(ctx, tr, dir, updated); err != nil {
		t.Fatalf("SetUser failed: %v", err)
	}

	got, _ := repo.Get(ctx, tr, dir, "u1")
	if got.Name != "Bob" || got.Email != "b@test.com" {
		t.Fatalf("update not applied: %+v", got)
	}
}

func TestSetUser_IndexUpdated(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	typeID := recordStore.Metadata()["User"]

	repo := store.NewUserRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "old@test.com"})
	oldIdx := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(2324124615), "old@test.com", "u1"}.Pack()
	if !kv.HasKey(oldIdx) {
		t.Fatal("old index should exist after create")
	}

	// Update email
	_ = repo.Set(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "new@test.com"})

	// Old index should be cleared
	if kv.HasKey(oldIdx) {
		t.Fatal("stale old index key was NOT cleared")
	}
	// New index should exist
	newIdx := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(2324124615), "new@test.com", "u1"}.Pack()
	if !kv.HasKey(newIdx) {
		t.Fatal("new index key not written")
	}
}

func TestSetUser_UnchangedIndexSkipsIndexWrite(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	typeID := recordStore.Metadata()["User"]

	repo := store.NewUserRepository(recordStore)
	if err := repo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "same@test.com"}); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	tr.ResetLog()
	// Update Name only while keeping Email unchanged
	if err := repo.Set(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice Updated", Email: "same@test.com"}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if len(tr.ClearCalls) != 0 {
		t.Fatalf("expected 0 Clear calls when indexed field is unchanged, got %d", len(tr.ClearCalls))
	}
	if len(tr.SetCalls) != 1 {
		t.Fatalf("expected 1 Set call (primary record only) when indexed field is unchanged, got %d", len(tr.SetCalls))
	}
	idxKey := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(2324124615), "same@test.com", "u1"}.Pack()
	if !kv.HasKey(idxKey) {
		t.Fatal("existing index key should remain present")
	}
}

func TestSetProduct_UpdatePrice(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	repo := store.NewProductRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.Product{Id: "p1", Name: "X", Category: "a", Price: 10})

	_ = repo.Set(ctx, tr, dir, &store.Product{Id: "p1", Name: "X", Category: "a", Price: 50})
	got, _ := repo.Get(ctx, tr, dir, "p1")
	if got.Price != 50 {
		t.Fatalf("expected price 50, got %d", got.Price)
	}
}

func TestSetProduct_IndexUpdated(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	typeID := recordStore.Metadata()["Product"]

	repo := store.NewProductRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.Product{Id: "p1", Name: "X", Category: "old_cat", Price: 1})
	oldIdx := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(3475980913), "old_cat", "p1"}.Pack()
	if !kv.HasKey(oldIdx) {
		t.Fatal("old index should exist after create")
	}

	_ = repo.Set(ctx, tr, dir, &store.Product{Id: "p1", Name: "X", Category: "new_cat", Price: 1})
	if kv.HasKey(oldIdx) {
		t.Fatal("stale old index was NOT cleared")
	}
	newIdx := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(3475980913), "new_cat", "p1"}.Pack()
	if !kv.HasKey(newIdx) {
		t.Fatal("new index key not written")
	}
}

// Delete
func TestDeleteUser_Success(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	typeID := recordStore.Metadata()["User"]
	repo := store.NewUserRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})

	if err := repo.Delete(ctx, tr, dir, "u1"); err != nil {
		t.Fatalf("DeleteUser failed: %v", err)
	}

	pk := tuple.Tuple{typeID, fdblayer.DataNamespace, "u1"}.Pack()
	if kv.HasKey(pk) {
		t.Fatal("primary key not cleared")
	}
	ik := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(2324124615), "a@test.com", "u1"}.Pack()
	if kv.HasKey(ik) {
		t.Fatal("index key not cleared")
	}
	// Get should fail
	_, err := repo.Get(ctx, tr, dir, "u1")
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestDeleteUser_NonExistent(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	repo := store.NewUserRepository(recordStore)
	// Should not error or panic
	if err := repo.Delete(ctx, tr, dir, "ghost"); err != nil {
		t.Fatalf("delete of non-existent should not error, got: %v", err)
	}
}

func TestDeleteProduct_ClearsIndex(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	typeID := recordStore.Metadata()["Product"]
	repo := store.NewProductRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.Product{Id: "p1", Name: "X", Category: "cat1", Price: 5})

	_ = repo.Delete(ctx, tr, dir, "p1")
	pk := tuple.Tuple{typeID, fdblayer.DataNamespace, "p1"}.Pack()
	ik := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(3475980913), "cat1", "p1"}.Pack()
	if kv.HasKey(pk) {
		t.Fatal("primary key not cleared")
	}
	if kv.HasKey(ik) {
		t.Fatal("category index not cleared")
	}
}

// Secondary Index Tests (empty range results)

func TestGetUserByEmail_NoResults(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()

	repo := store.NewUserRepository(recordStore)
	// GetRange returns empty → no results
	results, err := repo.GetByEmail(ctx, tr, dir, "nobody@test.com", store.UserPaginationOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results.Items) != 0 {
		t.Fatalf("expected empty results, got %d", len(results.Items))
	}
}

func TestGetProductByCategory_NoResults(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()

	repo := store.NewProductRepository(recordStore)
	results, err := repo.GetByCategory(ctx, tr, dir, "nonexistent", store.ProductPaginationOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results.Items) != 0 {
		t.Fatalf("expected empty, got %d", len(results.Items))
	}
}

func TestIndex_CrossTypeIsolation(t *testing.T) {
	recordStore, _, _, _ := tests.SyncAndSetup()
	meta := recordStore.Metadata()
	// Type IDs should be different, ensuring key-space isolation
	if meta["User"] == meta["Product"] {
		t.Fatal("User and Product share the same type ID — no isolation")
	}
}

// Batch & Pagination Tests

// BatchGet
func TestBatchGetUser_AllFound(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	repo := store.NewUserRepository(recordStore)
	for _, id := range []string{"u1", "u2", "u3"} {
		_ = repo.Create(ctx, tr, dir, &store.User{Id: id, Name: "Name" + id, Email: id + "@test.com"})
	}

	ids := []string{"u1", "u2", "u3"}
	result, err := repo.BatchGet(ctx, tr, dir, ids)
	if err != nil {
		t.Fatalf("BatchGetUser failed: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
}

func TestBatchGetUser_PartialFound(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	repo := store.NewUserRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "A", Email: "a@t.com"})
	_ = repo.Create(ctx, tr, dir, &store.User{Id: "u2", Name: "B", Email: "b@t.com"})

	ids := []string{"u1", "u2", "u3"} // u3 doesn't exist
	result, err := repo.BatchGet(ctx, tr, dir, ids)
	if err != nil {
		t.Fatalf("BatchGetUser failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestBatchGetUser_NoneFound(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()

	repo := store.NewUserRepository(recordStore)
	ids := []string{"x1", "x2"}
	result, err := repo.BatchGet(ctx, tr, dir, ids)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0, got %d", len(result))
	}
}

func TestBatchGetUser_EmptyInput(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()

	repo := store.NewUserRepository(recordStore)
	result, err := repo.BatchGet(ctx, tr, dir, []string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0, got %d", len(result))
	}
}

func TestBatchGetProduct_Success(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	repo := store.NewProductRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.Product{Id: "p1", Name: "A", Category: "c", Price: 1})
	_ = repo.Create(ctx, tr, dir, &store.Product{Id: "p2", Name: "B", Category: "c", Price: 2})

	ids := []string{"p1", "p2"}
	result, err := repo.BatchGet(ctx, tr, dir, ids)
	if err != nil {
		t.Fatalf("BatchGetProduct failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

// ==========================================================================
// Verify correct key structure via GetRangeSlice helper
// ==========================================================================

func TestCreateUser_IndexKeyStructure(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	typeID := recordStore.Metadata()["User"]
	repo := store.NewUserRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "A", Email: "a@t.com"})

	// Use the mock helper to scan for index keys
	prefix := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(2324124615), "a@t.com"}.Pack()
	kr, _ := fdb.PrefixRange(prefix)
	kvs := tr.GetRangeSlice(kr, fdb.RangeOptions{})
	if len(kvs) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(kvs))
	}
	// Unpack and verify PK is in the index key
	tpl, err := tuple.Unpack(kvs[0].Key)
	if err != nil {
		t.Fatalf("unpack failed: %v", err)
	}
	// tpl = {typeID, 1, indexID, email, pk}
	if len(tpl) != 5 {
		t.Fatalf("expected 5 elements in index key, got %d: %v", len(tpl), tpl)
	}
	if tpl[4].(string) != "u1" {
		t.Fatalf("expected PK 'u1' in index, got %v", tpl[4])
	}
}

func TestCreateProduct_MultipleInSameCategory(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	typeID := recordStore.Metadata()["Product"]
	repo := store.NewProductRepository(recordStore)
	_ = repo.Create(ctx, tr, dir, &store.Product{Id: "p1", Name: "A", Category: "tools", Price: 1})
	_ = repo.Create(ctx, tr, dir, &store.Product{Id: "p2", Name: "B", Category: "tools", Price: 2})

	prefix := tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(3475980913), "tools"}.Pack()
	kr, _ := fdb.PrefixRange(prefix)
	kvs := tr.GetRangeSlice(kr, fdb.RangeOptions{})
	if len(kvs) != 2 {
		t.Fatalf("expected 2 index entries for 'tools', got %d", len(kvs))
	}
}

func TestGenericRepository_User(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()

	// Instantiate the specific repository wrapper
	var repo store.UserRepository = store.NewUserRepository(recordStore)

	// Verify it also implements the GenericRepository interface
	var genRepo fdblayer.GenericRepository[*store.User, string] = repo

	// 1. Create
	user := &store.User{Id: "gen-1", Name: "Generic User", Email: "gen@example.com"}
	if err := genRepo.Create(ctx, tr, dir, user); err != nil {
		t.Fatalf("generic Create failed: %v", err)
	}

	typeID := recordStore.Metadata()["User"]
	pk := tuple.Tuple{typeID, fdblayer.DataNamespace, "gen-1"}.Pack()
	if !kv.HasKey(pk) {
		t.Fatal("generic Create did not write primary key")
	}

	// 2. Get
	got, err := genRepo.Get(ctx, tr, dir, "gen-1")
	if err != nil {
		t.Fatalf("generic Get failed: %v", err)
	}
	if got.Name != "Generic User" || got.Email != "gen@example.com" {
		t.Fatalf("generic Get returned unexpected user: %+v", got)
	}

	// 3. Set (Update)
	got.Name = "Updated Generic Name"
	if err := genRepo.Set(ctx, tr, dir, got); err != nil {
		t.Fatalf("generic Set failed: %v", err)
	}

	updated, err := genRepo.Get(ctx, tr, dir, "gen-1")
	if err != nil {
		t.Fatalf("generic Get after Set failed: %v", err)
	}
	if updated.Name != "Updated Generic Name" {
		t.Fatalf("generic Set did not update name: %s", updated.Name)
	}

	// 4. Delete
	if err := genRepo.Delete(ctx, tr, dir, "gen-1"); err != nil {
		t.Fatalf("generic Delete failed: %v", err)
	}

	if kv.HasKey(pk) {
		t.Fatal("generic Delete did not clear primary key")
	}

	_, err = genRepo.Get(ctx, tr, dir, "gen-1")
	if err == nil {
		t.Fatal("expected error getting deleted user")
	}
}

func TestSetAndDelete_CorruptedExistingRecordReturnsError(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	repo := store.NewUserRepository(recordStore)

	typeID := recordStore.Metadata()["User"]
	corruptedKey := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, "corrupted-1"})
	// Write invalid protobuf wire format bytes (field 0 is invalid in protobuf wire format)
	tr.Set(corruptedKey, []byte{0x00, 0xff, 0xff})

	// Set must return an unmarshal error rather than silently overwriting and orphaning indexes.
	err := repo.Set(ctx, tr, dir, &store.User{Id: "corrupted-1", Name: "New", Email: "new@example.com"})
	if err == nil {
		t.Fatal("expected Set on corrupted existing record to return an error, got nil")
	}

	// Delete must return an unmarshal error rather than clearing primary key and orphaning indexes.
	err = repo.Delete(ctx, tr, dir, "corrupted-1")
	if err == nil {
		t.Fatal("expected Delete on corrupted existing record to return an error, got nil")
	}
	if !kv.HasKey(corruptedKey) {
		t.Fatal("Delete cleared primary key despite failing to unmarshal existing record")
	}
}

func TestErrNotFound_SentinelWrapping(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	repo := store.NewUserRepository(recordStore)

	_, err := repo.Get(ctx, tr, dir, "missing-user")
	if !errors.Is(err, fdblayer.ErrNotFound) {
		t.Fatalf("expected errors.Is(err, fdblayer.ErrNotFound) == true, got err=%v", err)
	}
}

func TestUserVersionSequence_OverflowAndReset(t *testing.T) {
	recordStore := fdblayer.NewRecordStore()
	for i := 0; i < 65536; i++ {
		uv, err := recordStore.NextUserVersion()
		if err != nil {
			t.Fatalf("unexpected error at iteration %d: %v", i, err)
		}
		if int(uv) != i {
			t.Fatalf("expected UserVersion=%d, got %d", i, uv)
		}
	}
	if _, err := recordStore.NextUserVersion(); !errors.Is(err, fdblayer.ErrUserVersionOverflow) {
		t.Fatalf("expected ErrUserVersionOverflow at 65537th call, got: %v", err)
	}
	recordStore.ResetUserVersion()
	uv, err := recordStore.NextUserVersion()
	if err != nil || uv != 0 {
		t.Fatalf("expected UserVersion=0 after ResetUserVersion, got uv=%d, err=%v", uv, err)
	}
}

func TestNormalizeTupleElement_Int32Uint32Enum(t *testing.T) {
	if got := fdblayer.NormalizeTupleElement(int32(-42)); got != int64(-42) {
		t.Fatalf("expected int64(-42), got %T(%v)", got, got)
	}
	if got := fdblayer.NormalizeTupleElement(uint32(42)); got != uint64(42) {
		t.Fatalf("expected uint64(42), got %T(%v)", got, got)
	}
	if got := fdblayer.NormalizeTupleElement(store.OrderStatus_ORDER_STATUS_SHIPPED); got != int64(store.OrderStatus_ORDER_STATUS_SHIPPED) {
		t.Fatalf("expected int64 enum value, got %T(%v)", got, got)
	}
}

func TestGetProductByCategory_PaginatedAndReverse(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, _ := tests.SyncAndSetup()
	repo := store.NewProductRepository(recordStore)

	for _, id := range []string{"p1", "p2", "p3", "p4"} {
		if err := repo.Create(ctx, tr, dir, &store.Product{
			Id:       id,
			Name:     "Tool " + id,
			Category: "tools",
			Price:    10,
		}); err != nil {
			t.Fatalf("Create %s failed: %v", id, err)
		}
	}

	// Forward pagination (2 items per page)
	p1, err := repo.GetByCategory(ctx, tr, dir, "tools", store.ProductPaginationOptions{Limit: 2})
	if err != nil || len(p1.Items) != 2 || !p1.HasMore || p1.Items[0].Id != "p1" || p1.Items[1].Id != "p2" {
		t.Fatalf("unexpected forward page 1: %+v, err=%v", p1, err)
	}
	p2, err := repo.GetByCategory(ctx, tr, dir, "tools", store.ProductPaginationOptions{
		Begin: p1.NextKey,
		Limit: 2,
	})
	if err != nil || len(p2.Items) != 2 || p2.HasMore || p2.Items[0].Id != "p3" || p2.Items[1].Id != "p4" {
		t.Fatalf("unexpected forward page 2: %+v, err=%v", p2, err)
	}

	// Reverse pagination (2 items per page: p4, p3 -> p2, p1)
	rev1, err := repo.GetByCategory(ctx, tr, dir, "tools", store.ProductPaginationOptions{
		Limit:   2,
		Reverse: true,
	})
	if err != nil || len(rev1.Items) != 2 || !rev1.HasMore || rev1.Items[0].Id != "p4" || rev1.Items[1].Id != "p3" {
		t.Fatalf("unexpected reverse page 1: %+v, err=%v", rev1, err)
	}
	rev2, err := repo.GetByCategory(ctx, tr, dir, "tools", store.ProductPaginationOptions{
		Begin:   rev1.NextKey,
		Limit:   2,
		Reverse: true,
	})
	if err != nil || len(rev2.Items) != 2 || rev2.HasMore || rev2.Items[0].Id != "p2" || rev2.Items[1].Id != "p1" {
		t.Fatalf("unexpected reverse page 2: %+v, err=%v", rev2, err)
	}

	// Strongly-typed BatchGetProduct
	byPK, err := repo.BatchGet(ctx, tr, dir, []string{"p1", "p4", "nonexistent"})
	if err != nil || len(byPK) != 2 {
		t.Fatalf("BatchGetProduct failed: got %+v, err=%v", byPK, err)
	}

	// Out-of-bounds cursors in GetProductByCategory must be clamped without inverted range panic
	oobForwardAfter, err := repo.GetByCategory(ctx, tr, dir, "tools", store.ProductPaginationOptions{
		Begin: tuple.Tuple{"zzzzz\xff"},
		Limit: 2,
	})
	if err != nil || len(oobForwardAfter.Items) != 0 {
		t.Fatalf("expected 0 items for forward Begin past end of index range, got %+v, err=%v", oobForwardAfter, err)
	}
	oobReverseBefore, err := repo.GetByCategory(ctx, tr, dir, "tools", store.ProductPaginationOptions{
		Begin:   tuple.Tuple{""},
		Limit:   2,
		Reverse: true,
	})
	if err != nil {
		t.Fatalf("unexpected error for reverse Begin at start of index range: %v", err)
	}
	_ = oobReverseBefore
}




