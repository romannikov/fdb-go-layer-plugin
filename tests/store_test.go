package store

import (
	"strings"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"google.golang.org/protobuf/proto"
)

// RecordStore & Metadata Tests

func TestSyncMetadata_FreshStore(t *testing.T) {
	store, tr, _, _ := syncAndSetup()

	meta := store.Metadata()
	if meta["User"] == 0 || meta["Product"] == 0 {
		t.Fatalf("metadata not populated: %v", meta)
	}
	if meta["User"] == meta["Product"] {
		t.Fatalf("User and Product got the same type ID")
	}
	// Three metadata keys should have been written
	if len(tr.kv.Snapshot()) != 3 {
		t.Fatalf("expected 3 keys written, got %d", len(tr.kv.Snapshot()))
	}
}

func TestSyncMetadata_Idempotent(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	firstMeta := store.Metadata()

	// Sync again — IDs should remain stable, no extra writes
	err := store.SyncMetadata(tr, dir)
	if err != nil {
		t.Fatalf("second SyncMetadata failed: %v", err)
	}
	secondMeta := store.Metadata()
	if firstMeta["User"] != secondMeta["User"] || firstMeta["Product"] != secondMeta["Product"] {
		t.Fatalf("IDs changed on second sync: %v → %v", firstMeta, secondMeta)
	}
}

func TestMetadata_ReturnsCopy(t *testing.T) {
	store, _, _, _ := syncAndSetup()
	m := store.Metadata()
	m["User"] = 9999
	if store.Metadata()["User"] == 9999 {
		t.Fatal("Metadata() returned a reference, not a copy")
	}
}

func TestNewRecordStore_EmptyMetadata(t *testing.T) {
	store := NewRecordStore()
	m := store.Metadata()
	if len(m) != 0 {
		t.Fatalf("expected empty metadata, got %v", m)
	}
}

// CRUD Tests

// Create
func TestCreateUser_Success(t *testing.T) {
	store, tr, dir, kv := syncAndSetup()
	user := &User{Id: "u1", Name: "Alice", Email: "alice@example.com"}

	if err := store.CreateUser(tr, dir, user); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	typeID := store.Metadata()["User"]
	// Primary key
	pk := tuple.Tuple{typeID, "u1"}.Pack()
	if !kv.HasKey(pk) {
		t.Fatal("primary key not written")
	}
	// Index key
	ik := tuple.Tuple{typeID, "index", "Email", "alice@example.com", "u1"}.Pack()
	if !kv.HasKey(ik) {
		t.Fatal("index key not written")
	}
	// Deserialize and verify
	val := kv.get(pk)
	got := &User{}
	if err := proto.Unmarshal(val, got); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got.Id != "u1" || got.Name != "Alice" || got.Email != "alice@example.com" {
		t.Fatalf("unexpected user: %+v", got)
	}
}

func TestCreateProduct_Success(t *testing.T) {
	store, tr, dir, kv := syncAndSetup()
	p := &Product{Id: "p1", Name: "Widget", Category: "tools", Price: 42}

	if err := store.CreateProduct(tr, dir, p); err != nil {
		t.Fatalf("CreateProduct failed: %v", err)
	}

	typeID := store.Metadata()["Product"]
	pk := tuple.Tuple{typeID, "p1"}.Pack()
	if !kv.HasKey(pk) {
		t.Fatal("primary key not written")
	}
	ik := tuple.Tuple{typeID, "index", "Category", "tools", "p1"}.Pack()
	if !kv.HasKey(ik) {
		t.Fatal("category index key not written")
	}
	val := kv.get(pk)
	got := &Product{}
	if err := proto.Unmarshal(val, got); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got.Price != 42 {
		t.Fatalf("expected price 42, got %d", got.Price)
	}
}

func TestCreate_BeforeSync(t *testing.T) {
	kv := NewMockKV()
	tr := NewMockTransaction(kv)
	dir := &MockDirectorySubspace{}
	store := NewRecordStore() // no SyncMetadata

	err := store.CreateUser(tr, dir, &User{Id: "u1"})
	if err == nil {
		t.Fatal("expected error when metadata not synced")
	}
	if !strings.Contains(err.Error(), "not found in metadata") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Get
func TestGetUser_Success(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	user := &User{Id: "u1", Name: "Alice", Email: "alice@example.com"}
	_ = store.CreateUser(tr, dir, user)

	got, err := store.GetUser(tr, dir, "u1")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	if got.Name != "Alice" || got.Email != "alice@example.com" {
		t.Fatalf("unexpected user: %+v", got)
	}
}

func TestGetUser_NotFound(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()

	_, err := store.GetUser(tr, dir, "nonexistent")
	if err == nil {
		t.Fatal("expected not found error")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetProduct_Success(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	_ = store.CreateProduct(tr, dir, &Product{Id: "p1", Name: "Gizmo", Category: "tech", Price: 99})

	got, err := store.GetProduct(tr, dir, "p1")
	if err != nil {
		t.Fatalf("GetProduct failed: %v", err)
	}
	if got.Name != "Gizmo" || got.Price != 99 {
		t.Fatalf("unexpected product: %+v", got)
	}
}

func TestGet_BeforeSync(t *testing.T) {
	store := NewRecordStore()
	kv := NewMockKV()
	tr := NewMockTransaction(kv)
	dir := &MockDirectorySubspace{}

	_, err := store.GetUser(tr, dir, "u1")
	if err == nil {
		t.Fatal("expected error")
	}
}

// Set (Update)
func TestSetUser_UpdateFields(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	_ = store.CreateUser(tr, dir, &User{Id: "u1", Name: "Alice", Email: "a@test.com"})

	updated := &User{Id: "u1", Name: "Bob", Email: "b@test.com"}
	if err := store.SetUser(tr, dir, updated); err != nil {
		t.Fatalf("SetUser failed: %v", err)
	}

	got, _ := store.GetUser(tr, dir, "u1")
	if got.Name != "Bob" || got.Email != "b@test.com" {
		t.Fatalf("update not applied: %+v", got)
	}
}

func TestSetUser_IndexUpdated(t *testing.T) {
	store, tr, dir, kv := syncAndSetup()
	typeID := store.Metadata()["User"]

	_ = store.CreateUser(tr, dir, &User{Id: "u1", Name: "Alice", Email: "old@test.com"})
	oldIdx := tuple.Tuple{typeID, "index", "Email", "old@test.com", "u1"}.Pack()
	if !kv.HasKey(oldIdx) {
		t.Fatal("old index should exist after create")
	}

	// Update email
	_ = store.SetUser(tr, dir, &User{Id: "u1", Name: "Alice", Email: "new@test.com"})

	// Old index should be cleared
	if kv.HasKey(oldIdx) {
		t.Fatal("stale old index key was NOT cleared")
	}
	// New index should exist
	newIdx := tuple.Tuple{typeID, "index", "Email", "new@test.com", "u1"}.Pack()
	if !kv.HasKey(newIdx) {
		t.Fatal("new index key not written")
	}
}

func TestSetProduct_UpdatePrice(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	_ = store.CreateProduct(tr, dir, &Product{Id: "p1", Name: "X", Category: "a", Price: 10})

	_ = store.SetProduct(tr, dir, &Product{Id: "p1", Name: "X", Category: "a", Price: 50})
	got, _ := store.GetProduct(tr, dir, "p1")
	if got.Price != 50 {
		t.Fatalf("expected price 50, got %d", got.Price)
	}
}

func TestSetProduct_IndexUpdated(t *testing.T) {
	store, tr, dir, kv := syncAndSetup()
	typeID := store.Metadata()["Product"]

	_ = store.CreateProduct(tr, dir, &Product{Id: "p1", Name: "X", Category: "old_cat", Price: 1})
	oldIdx := tuple.Tuple{typeID, "index", "Category", "old_cat", "p1"}.Pack()
	if !kv.HasKey(oldIdx) {
		t.Fatal("old index should exist after create")
	}

	_ = store.SetProduct(tr, dir, &Product{Id: "p1", Name: "X", Category: "new_cat", Price: 1})
	if kv.HasKey(oldIdx) {
		t.Fatal("stale old index was NOT cleared")
	}
	newIdx := tuple.Tuple{typeID, "index", "Category", "new_cat", "p1"}.Pack()
	if !kv.HasKey(newIdx) {
		t.Fatal("new index key not written")
	}
}

// Delete
func TestDeleteUser_Success(t *testing.T) {
	store, tr, dir, kv := syncAndSetup()
	typeID := store.Metadata()["User"]
	_ = store.CreateUser(tr, dir, &User{Id: "u1", Name: "Alice", Email: "a@test.com"})

	if err := store.DeleteUser(tr, dir, "u1"); err != nil {
		t.Fatalf("DeleteUser failed: %v", err)
	}

	pk := tuple.Tuple{typeID, "u1"}.Pack()
	if kv.HasKey(pk) {
		t.Fatal("primary key not cleared")
	}
	ik := tuple.Tuple{typeID, "index", "Email", "a@test.com", "u1"}.Pack()
	if kv.HasKey(ik) {
		t.Fatal("index key not cleared")
	}
	// Get should fail
	_, err := store.GetUser(tr, dir, "u1")
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestDeleteUser_NonExistent(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	// Should not error or panic
	if err := store.DeleteUser(tr, dir, "ghost"); err != nil {
		t.Fatalf("delete of non-existent should not error, got: %v", err)
	}
}

func TestDeleteProduct_ClearsIndex(t *testing.T) {
	store, tr, dir, kv := syncAndSetup()
	typeID := store.Metadata()["Product"]
	_ = store.CreateProduct(tr, dir, &Product{Id: "p1", Name: "X", Category: "cat1", Price: 5})

	_ = store.DeleteProduct(tr, dir, "p1")
	pk := tuple.Tuple{typeID, "p1"}.Pack()
	ik := tuple.Tuple{typeID, "index", "Category", "cat1", "p1"}.Pack()
	if kv.HasKey(pk) {
		t.Fatal("primary key not cleared")
	}
	if kv.HasKey(ik) {
		t.Fatal("category index not cleared")
	}
}

// Secondary Index Tests (empty range results)

func TestGetUserByEmail_NoResults(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()

	// GetRange returns empty → no results
	results, err := store.GetUserByEmail(tr, dir, "nobody@test.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected empty results, got %d", len(results))
	}
}

func TestGetProductByCategory_NoResults(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()

	results, err := store.GetProductByCategory(tr, dir, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected empty, got %d", len(results))
	}
}

func TestIndex_CrossTypeIsolation(t *testing.T) {
	store, _, _, _ := syncAndSetup()
	meta := store.Metadata()
	// Type IDs should be different, ensuring key-space isolation
	if meta["User"] == meta["Product"] {
		t.Fatal("User and Product share the same type ID — no isolation")
	}
}

// Batch & Pagination Tests

// BatchGet
func TestBatchGetUser_AllFound(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	for _, id := range []string{"u1", "u2", "u3"} {
		_ = store.CreateUser(tr, dir, &User{Id: id, Name: "Name" + id, Email: id + "@test.com"})
	}

	ids := []tuple.Tuple{{"u1"}, {"u2"}, {"u3"}}
	result, err := store.BatchGetUser(tr, dir, ids)
	if err != nil {
		t.Fatalf("BatchGetUser failed: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
}

func TestBatchGetUser_PartialFound(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	_ = store.CreateUser(tr, dir, &User{Id: "u1", Name: "A", Email: "a@t.com"})
	_ = store.CreateUser(tr, dir, &User{Id: "u2", Name: "B", Email: "b@t.com"})

	ids := []tuple.Tuple{{"u1"}, {"u2"}, {"u3"}} // u3 doesn't exist
	result, err := store.BatchGetUser(tr, dir, ids)
	if err != nil {
		t.Fatalf("BatchGetUser failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestBatchGetUser_NoneFound(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()

	ids := []tuple.Tuple{{"x1"}, {"x2"}}
	result, err := store.BatchGetUser(tr, dir, ids)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0, got %d", len(result))
	}
}

func TestBatchGetUser_EmptyInput(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()

	result, err := store.BatchGetUser(tr, dir, []tuple.Tuple{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0, got %d", len(result))
	}
}

func TestBatchGetProduct_Success(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	_ = store.CreateProduct(tr, dir, &Product{Id: "p1", Name: "A", Category: "c", Price: 1})
	_ = store.CreateProduct(tr, dir, &Product{Id: "p2", Name: "B", Category: "c", Price: 2})

	ids := []tuple.Tuple{{"p1"}, {"p2"}}
	result, err := store.BatchGetProduct(tr, dir, ids)
	if err != nil {
		t.Fatalf("BatchGetProduct failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

// --- List (Pagination) — empty results only ---------------------------------
// NOTE: ListUser/ListProduct use GetRange().Iterator() which returns an
// fdb.RangeResult concrete struct. Since we cannot populate unexported fields,
// tests with actual data require an FDB integration test.
// These tests verify behavior with empty data.

// NOTE: ListUser/ListProduct tests with empty results panic because
// fdb.RangeResult.Iterator().Advance() returns true on a zero-value
// RangeResult but has nil kvs, causing MustGet() to panic.
// ListUser/ListProduct require integration tests with a real FDB instance.

// ==========================================================================
// Verify correct key structure via GetRangeSlice helper
// ==========================================================================

func TestCreateUser_IndexKeyStructure(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	typeID := store.Metadata()["User"]
	_ = store.CreateUser(tr, dir, &User{Id: "u1", Name: "A", Email: "a@t.com"})

	// Use the mock helper to scan for index keys
	prefix := tuple.Tuple{typeID, "index", "Email", "a@t.com"}.Pack()
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
	// tpl = {typeID, "index", "Email", email, pk}
	if len(tpl) != 5 {
		t.Fatalf("expected 5 elements in index key, got %d: %v", len(tpl), tpl)
	}
	if tpl[4].(string) != "u1" {
		t.Fatalf("expected PK 'u1' in index, got %v", tpl[4])
	}
}

func TestCreateProduct_MultipleInSameCategory(t *testing.T) {
	store, tr, dir, _ := syncAndSetup()
	typeID := store.Metadata()["Product"]
	_ = store.CreateProduct(tr, dir, &Product{Id: "p1", Name: "A", Category: "tools", Price: 1})
	_ = store.CreateProduct(tr, dir, &Product{Id: "p2", Name: "B", Category: "tools", Price: 2})

	prefix := tuple.Tuple{typeID, "index", "Category", "tools"}.Pack()
	kr, _ := fdb.PrefixRange(prefix)
	kvs := tr.GetRangeSlice(kr, fdb.RangeOptions{})
	if len(kvs) != 2 {
		t.Fatalf("expected 2 index entries for 'tools', got %d", len(kvs))
	}
}

func TestGenericRepository_User(t *testing.T) {
	store, tr, dir, kv := syncAndSetup()

	// Instantiate the specific repository wrapper
	var repo UserRepository = NewUserRepository(store)

	// Verify it also implements the GenericRepository interface
	var genRepo GenericRepository[*User, string] = repo

	// 1. Create
	user := &User{Id: "gen-1", Name: "Generic User", Email: "gen@example.com"}
	if err := genRepo.Create(tr, dir, user); err != nil {
		t.Fatalf("generic Create failed: %v", err)
	}

	typeID := store.Metadata()["User"]
	pk := tuple.Tuple{typeID, "gen-1"}.Pack()
	if !kv.HasKey(pk) {
		t.Fatal("generic Create did not write primary key")
	}

	// 2. Get
	got, err := genRepo.Get(tr, dir, "gen-1")
	if err != nil {
		t.Fatalf("generic Get failed: %v", err)
	}
	if got.Name != "Generic User" || got.Email != "gen@example.com" {
		t.Fatalf("generic Get returned unexpected user: %+v", got)
	}

	// 3. Set (Update)
	got.Name = "Updated Generic Name"
	if err := genRepo.Set(tr, dir, got); err != nil {
		t.Fatalf("generic Set failed: %v", err)
	}

	updated, err := genRepo.Get(tr, dir, "gen-1")
	if err != nil {
		t.Fatalf("generic Get after Set failed: %v", err)
	}
	if updated.Name != "Updated Generic Name" {
		t.Fatalf("generic Set did not update name: %s", updated.Name)
	}

	// 4. Delete
	if err := genRepo.Delete(tr, dir, "gen-1"); err != nil {
		t.Fatalf("generic Delete failed: %v", err)
	}

	if kv.HasKey(pk) {
		t.Fatal("generic Delete did not clear primary key")
	}

	_, err = genRepo.Get(tr, dir, "gen-1")
	if err == nil {
		t.Fatal("expected error getting deleted user")
	}
}

