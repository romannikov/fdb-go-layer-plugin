package basic_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"google.golang.org/protobuf/proto"

	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	tests "github.com/romannikov/fdb-layer/tests/go"
	"github.com/romannikov/fdb-layer/tests/go/store"
)

func init() {
	fdb.MustAPIVersion(710)
}

// Compile-Time TypeID & IndexID Tests

func TestCompileTimeTypeIDs_DistinctAndDeterministic(t *testing.T) {
	ids := map[string]int64{
		"User":        store.UserTypeID,
		"Product":     store.ProductTypeID,
		"Post":        store.PostTypeID,
		"TaskMessage": store.TaskMessageTypeID,
		"Order":       store.OrderTypeID,
		"AuditLog":    store.AuditLogTypeID,
	}
	seen := make(map[int64]string, len(ids))
	for name, id := range ids {
		if id <= 0 {
			t.Fatalf("expected positive compile-time TypeID for %s, got %d", name, id)
		}
		if prev, exists := seen[id]; exists {
			t.Fatalf("TypeID collision between %s and %s (%d)", prev, name, id)
		}
		seen[id] = name
	}
	if store.OrderTypeID != 100 {
		t.Fatalf("expected explicit OrderTypeID = 100, got %d", store.OrderTypeID)
	}
	if store.UserByEmail != 1 || store.ProductByCategory != 1 || store.OrderByStatusCreatedAt != 1001 {
		t.Fatalf("unexpected generated IndexID constants: UserByEmail=%d, ProductByCategory=%d, OrderByStatusCreatedAt=%d",
			store.UserByEmail, store.ProductByCategory, store.OrderByStatusCreatedAt)
	}
}

// CRUD Tests

// Insert
func TestInsertUser_Success(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	user := &store.User{Id: "u1", Name: "Alice", Email: "alice@example.com"}

	repo := store.NewUserRepository(dir)
	if err := repo.Insert(ctx, tr, user); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Primary key
	pk := tuple.Tuple{store.UserTypeID, fdblayer.DataNamespace, "u1"}.Pack()
	if !kv.HasKey(pk) {
		t.Fatal("primary key not written")
	}
	// Index key
	ik := tuple.Tuple{store.UserTypeID, fdblayer.IndexNamespace, int64(1), "alice@example.com", "u1"}.Pack()
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

func TestInsertProduct_Success(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	p := &store.Product{Id: "p1", Name: "Widget", Category: "tools", Price: 42}

	repo := store.NewProductRepository(dir)
	if err := repo.Insert(ctx, tr, p); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	pk := tuple.Tuple{store.ProductTypeID, fdblayer.DataNamespace, "p1"}.Pack()
	if !kv.HasKey(pk) {
		t.Fatal("primary key not written")
	}
	ik := tuple.Tuple{store.ProductTypeID, fdblayer.IndexNamespace, int64(1), "tools", "p1"}.Pack()
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

func TestInsertUser_AlreadyExists(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	repo := store.NewUserRepository(dir)

	first := &store.User{Id: "u1", Name: "Alice", Email: "alice@example.com"}
	if err := repo.Insert(ctx, tr, first); err != nil {
		t.Fatalf("initial Insert failed: %v", err)
	}

	// Attempting to Insert a second time with the same PK and a different email
	// must fail with fdblayer.ErrAlreadyExists and leave both the primary record
	// and secondary index untouched.
	duplicate := &store.User{Id: "u1", Name: "Bob", Email: "bob@example.com"}
	err := repo.Insert(ctx, tr, duplicate)
	if !errors.Is(err, fdblayer.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists on duplicate Insert, got: %v", err)
	}

	got, err := repo.ReadRow(ctx, tr, fdblayer.Key{"u1"})
	if err != nil {
		t.Fatalf("ReadRow after rejected Insert failed: %v", err)
	}
	if got.Name != "Alice" || got.Email != "alice@example.com" {
		t.Fatalf("existing record was overwritten by duplicate Insert: %+v", got)
	}

	bobIdx := tuple.Tuple{store.UserTypeID, fdblayer.IndexNamespace, int64(1), "bob@example.com", "u1"}.Pack()
	if kv.HasKey(bobIdx) {
		t.Fatal("duplicate Insert must not write secondary index entry")
	}
}

// ReadRow
func TestReadRowUser_Success(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	user := &store.User{Id: "u1", Name: "Alice", Email: "alice@example.com"}
	repo := store.NewUserRepository(dir)
	_ = repo.Insert(ctx, tr, user)

	got, err := repo.ReadRow(ctx, tr, fdblayer.Key{"u1"})
	if err != nil {
		t.Fatalf("ReadRow failed: %v", err)
	}
	if got.Name != "Alice" || got.Email != "alice@example.com" {
		t.Fatalf("unexpected user: %+v", got)
	}
}

func TestReadRowUser_NotFound(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()

	repo := store.NewUserRepository(dir)
	_, err := repo.ReadRow(ctx, tr, fdblayer.Key{"nonexistent"})
	if err == nil {
		t.Fatal("expected not found error")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadRowProduct_Success(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewProductRepository(dir)
	_ = repo.Insert(ctx, tr, &store.Product{Id: "p1", Name: "Gizmo", Category: "tech", Price: 99})

	got, err := repo.ReadRow(ctx, tr, fdblayer.Key{"p1"})
	if err != nil {
		t.Fatalf("ReadRow failed: %v", err)
	}
	if got.Name != "Gizmo" || got.Price != 99 {
		t.Fatalf("unexpected product: %+v", got)
	}
}

// Upsert (Update)
func TestUpsertUser_UpdateFields(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewUserRepository(dir)
	_ = repo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})

	updated := &store.User{Id: "u1", Name: "Bob", Email: "b@test.com"}
	if err := repo.Upsert(ctx, tr, updated); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	got, _ := repo.ReadRow(ctx, tr, fdblayer.Key{"u1"})
	if got.Name != "Bob" || got.Email != "b@test.com" {
		t.Fatalf("update not applied: %+v", got)
	}
}

func TestUpsertUser_IndexUpdated(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	repo := store.NewUserRepository(dir)
	_ = repo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "old@test.com"})
	oldIdx := tuple.Tuple{store.UserTypeID, fdblayer.IndexNamespace, int64(1), "old@test.com", "u1"}.Pack()
	if !kv.HasKey(oldIdx) {
		t.Fatal("old index should exist after insert")
	}

	// Update email
	_ = repo.Upsert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "new@test.com"})

	// Old index should be cleared
	if kv.HasKey(oldIdx) {
		t.Fatal("stale old index key was NOT cleared")
	}
	// New index should exist
	newIdx := tuple.Tuple{store.UserTypeID, fdblayer.IndexNamespace, int64(1), "new@test.com", "u1"}.Pack()
	if !kv.HasKey(newIdx) {
		t.Fatal("new index key not written")
	}
}

func TestUpsertUser_UnchangedIndexSkipsIndexWrite(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	repo := store.NewUserRepository(dir)
	if err := repo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "same@test.com"}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	tr.ResetLog()
	// Update Name only while keeping Email unchanged
	if err := repo.Upsert(ctx, tr, &store.User{Id: "u1", Name: "Alice Updated", Email: "same@test.com"}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	if len(tr.ClearCalls) != 0 {
		t.Fatalf("expected 0 Clear calls when indexed field is unchanged, got %d", len(tr.ClearCalls))
	}
	if len(tr.SetCalls) != 1 {
		t.Fatalf("expected 1 Set call (primary record only) when indexed field is unchanged, got %d", len(tr.SetCalls))
	}
	idxKey := tuple.Tuple{store.UserTypeID, fdblayer.IndexNamespace, int64(1), "same@test.com", "u1"}.Pack()
	if !kv.HasKey(idxKey) {
		t.Fatal("existing index key should remain present")
	}
}

func TestUpsertProduct_UpdatePrice(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewProductRepository(dir)
	_ = repo.Insert(ctx, tr, &store.Product{Id: "p1", Name: "X", Category: "a", Price: 10})

	_ = repo.Upsert(ctx, tr, &store.Product{Id: "p1", Name: "X", Category: "a", Price: 50})
	got, _ := repo.ReadRow(ctx, tr, fdblayer.Key{"p1"})
	if got.Price != 50 {
		t.Fatalf("expected price 50, got %d", got.Price)
	}
}

func TestUpsertProduct_IndexUpdated(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	repo := store.NewProductRepository(dir)
	_ = repo.Insert(ctx, tr, &store.Product{Id: "p1", Name: "X", Category: "old_cat", Price: 1})
	oldIdx := tuple.Tuple{store.ProductTypeID, fdblayer.IndexNamespace, int64(1), "old_cat", "p1"}.Pack()
	if !kv.HasKey(oldIdx) {
		t.Fatal("old index should exist after insert")
	}

	_ = repo.Upsert(ctx, tr, &store.Product{Id: "p1", Name: "X", Category: "new_cat", Price: 1})
	if kv.HasKey(oldIdx) {
		t.Fatal("stale old index was NOT cleared")
	}
	newIdx := tuple.Tuple{store.ProductTypeID, fdblayer.IndexNamespace, int64(1), "new_cat", "p1"}.Pack()
	if !kv.HasKey(newIdx) {
		t.Fatal("new index key not written")
	}
}

// Delete
func TestDeleteUser_Success(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	repo := store.NewUserRepository(dir)
	_ = repo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})

	if err := repo.Delete(ctx, tr, fdblayer.Key{"u1"}); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	pk := tuple.Tuple{store.UserTypeID, fdblayer.DataNamespace, "u1"}.Pack()
	if kv.HasKey(pk) {
		t.Fatal("primary key not cleared")
	}
	ik := tuple.Tuple{store.UserTypeID, fdblayer.IndexNamespace, int64(1), "a@test.com", "u1"}.Pack()
	if kv.HasKey(ik) {
		t.Fatal("index key not cleared")
	}
	// ReadRow should fail
	_, err := repo.ReadRow(ctx, tr, fdblayer.Key{"u1"})
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestDeleteUser_NonExistent(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewUserRepository(dir)
	// Should not error or panic
	if err := repo.Delete(ctx, tr, fdblayer.Key{"ghost"}); err != nil {
		t.Fatalf("delete of non-existent should not error, got: %v", err)
	}
}

func TestDeleteProduct_ClearsIndex(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	repo := store.NewProductRepository(dir)
	_ = repo.Insert(ctx, tr, &store.Product{Id: "p1", Name: "X", Category: "cat1", Price: 5})

	_ = repo.Delete(ctx, tr, fdblayer.Key{"p1"})
	pk := tuple.Tuple{store.ProductTypeID, fdblayer.DataNamespace, "p1"}.Pack()
	ik := tuple.Tuple{store.ProductTypeID, fdblayer.IndexNamespace, int64(1), "cat1", "p1"}.Pack()
	if kv.HasKey(pk) {
		t.Fatal("primary key not cleared")
	}
	if kv.HasKey(ik) {
		t.Fatal("category index not cleared")
	}
}

// Secondary Index Tests (empty range results)

func TestScanIndexUserByEmail_NoResults(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()

	repo := store.NewUserRepository(dir)
	results, err := repo.ScanIndex(ctx, tr, store.UserByEmail, fdblayer.Prefix("nobody@test.com"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results.Items) != 0 {
		t.Fatalf("expected empty results, got %d", len(results.Items))
	}
}

func TestScanIndexProductByCategory_NoResults(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()

	repo := store.NewProductRepository(dir)
	results, err := repo.ScanIndex(ctx, tr, store.ProductByCategory, fdblayer.Prefix("nonexistent"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results.Items) != 0 {
		t.Fatalf("expected empty, got %d", len(results.Items))
	}
}

func TestScanIndex_ErrUnknownIndex_And_ErrInvalidKey(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewUserRepository(dir)

	// Unknown index ID must return ErrUnknownIndex
	if _, err := repo.ScanIndex(ctx, tr, fdblayer.IndexID(999), fdblayer.AllKeys()); !errors.Is(err, fdblayer.ErrUnknownIndex) {
		t.Fatalf("expected ErrUnknownIndex, got %v", err)
	}

	// Wrong PK arity in ReadRow / Delete / ReadRows must return ErrInvalidKey
	if _, err := repo.ReadRow(ctx, tr, fdblayer.Key{"u1", "extra"}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on ReadRow arity mismatch, got %v", err)
	}
	if _, err := repo.ReadRow(ctx, tr, fdblayer.Key{123}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on ReadRow type mismatch, got %v", err)
	}
	if err := repo.Delete(ctx, tr, fdblayer.Key{}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on Delete arity mismatch, got %v", err)
	}
	if _, err := repo.ReadRows(ctx, tr, []fdblayer.Key{{"u1"}, {"u2", "extra"}}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on ReadRows arity mismatch, got %v", err)
	}
	// Exceeding key arity in Scan / ScanIndex must return ErrInvalidKey
	if _, err := repo.Scan(ctx, tr, fdblayer.Prefix("u1", "extra")); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on Scan prefix arity overflow, got %v", err)
	}
	if _, err := repo.ScanIndex(ctx, tr, store.UserByEmail, fdblayer.Prefix("a@b.com", "u1", "extra")); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on ScanIndex prefix arity overflow, got %v", err)
	}
}

func TestIndex_CrossTypeIsolation(t *testing.T) {
	// Type IDs should be different, ensuring key-space isolation
	if store.UserTypeID == store.ProductTypeID {
		t.Fatal("User and Product share the same type ID — no isolation")
	}
}

// ReadRows & Scan Tests

func TestReadRowsUser_AllFound(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewUserRepository(dir)
	for _, id := range []string{"u1", "u2", "u3"} {
		_ = repo.Insert(ctx, tr, &store.User{Id: id, Name: "Name" + id, Email: id + "@test.com"})
	}

	keys := []fdblayer.Key{{"u1"}, {"u2"}, {"u3"}}
	result, err := repo.ReadRows(ctx, tr, keys)
	if err != nil {
		t.Fatalf("ReadRows failed: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
}

func TestReadRowsUser_PartialFound(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewUserRepository(dir)
	_ = repo.Insert(ctx, tr, &store.User{Id: "u1", Name: "A", Email: "a@t.com"})
	_ = repo.Insert(ctx, tr, &store.User{Id: "u2", Name: "B", Email: "b@t.com"})

	keys := []fdblayer.Key{{"u1"}, {"u2"}, {"u3"}} // u3 doesn't exist
	result, err := repo.ReadRows(ctx, tr, keys)
	if err != nil {
		t.Fatalf("ReadRows failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestReadRowsUser_NoneFound(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()

	repo := store.NewUserRepository(dir)
	keys := []fdblayer.Key{{"x1"}, {"x2"}}
	result, err := repo.ReadRows(ctx, tr, keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0, got %d", len(result))
	}
}

func TestReadRowsUser_EmptyInput(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()

	repo := store.NewUserRepository(dir)
	result, err := repo.ReadRows(ctx, tr, []fdblayer.Key{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0, got %d", len(result))
	}
}

func TestReadRowsProduct_Success(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewProductRepository(dir)
	_ = repo.Insert(ctx, tr, &store.Product{Id: "p1", Name: "A", Category: "c", Price: 1})
	_ = repo.Insert(ctx, tr, &store.Product{Id: "p2", Name: "B", Category: "c", Price: 2})

	keys := []fdblayer.Key{{"p1"}, {"p2"}}
	result, err := repo.ReadRows(ctx, tr, keys)
	if err != nil {
		t.Fatalf("ReadRows failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

// Verify correct key structure via GetRangeSlice helper

func TestInsertUser_IndexKeyStructure(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewUserRepository(dir)
	_ = repo.Insert(ctx, tr, &store.User{Id: "u1", Name: "A", Email: "a@t.com"})

	// Use the mock helper to scan for index keys
	prefix := tuple.Tuple{store.UserTypeID, fdblayer.IndexNamespace, int64(1), "a@t.com"}.Pack()
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

func TestInsertProduct_MultipleInSameCategory(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewProductRepository(dir)
	_ = repo.Insert(ctx, tr, &store.Product{Id: "p1", Name: "A", Category: "tools", Price: 1})
	_ = repo.Insert(ctx, tr, &store.Product{Id: "p2", Name: "B", Category: "tools", Price: 2})

	prefix := tuple.Tuple{store.ProductTypeID, fdblayer.IndexNamespace, int64(1), "tools"}.Pack()
	kr, _ := fdb.PrefixRange(prefix)
	kvs := tr.GetRangeSlice(kr, fdb.RangeOptions{})
	if len(kvs) != 2 {
		t.Fatalf("expected 2 index entries for 'tools', got %d", len(kvs))
	}
}

func TestRepositoryInterface_User(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	// Instantiate the uniform Repository[*store.User]
	var repo fdblayer.Repository[*store.User] = store.NewUserRepository(dir)

	// 1. Insert
	user := &store.User{Id: "gen-1", Name: "Generic User", Email: "gen@example.com"}
	if err := repo.Insert(ctx, tr, user); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	pk := tuple.Tuple{store.UserTypeID, fdblayer.DataNamespace, "gen-1"}.Pack()
	if !kv.HasKey(pk) {
		t.Fatal("Insert did not write primary key")
	}

	// 2. ReadRow
	got, err := repo.ReadRow(ctx, tr, fdblayer.Key{"gen-1"})
	if err != nil {
		t.Fatalf("ReadRow failed: %v", err)
	}
	if got.Name != "Generic User" || got.Email != "gen@example.com" {
		t.Fatalf("ReadRow returned unexpected user: %+v", got)
	}

	// 3. Upsert (Update)
	got.Name = "Updated Generic Name"
	if err := repo.Upsert(ctx, tr, got); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	updated, err := repo.ReadRow(ctx, tr, fdblayer.Key{"gen-1"})
	if err != nil {
		t.Fatalf("ReadRow after Upsert failed: %v", err)
	}
	if updated.Name != "Updated Generic Name" {
		t.Fatalf("Upsert did not update name: %s", updated.Name)
	}

	// 4. Delete
	if err := repo.Delete(ctx, tr, fdblayer.Key{"gen-1"}); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if kv.HasKey(pk) {
		t.Fatal("Delete did not clear primary key")
	}

	_, err = repo.ReadRow(ctx, tr, fdblayer.Key{"gen-1"})
	if err == nil {
		t.Fatal("expected error getting deleted user")
	}
}

func TestUpsertAndDelete_CorruptedExistingRecordReturnsError(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	repo := store.NewUserRepository(dir)

	corruptedKey := dir.Pack(tuple.Tuple{store.UserTypeID, fdblayer.DataNamespace, "corrupted-1"})
	// Write invalid protobuf wire format bytes (field 0 is invalid in protobuf wire format)
	tr.Set(corruptedKey, []byte{0x00, 0xff, 0xff})

	// Upsert must return an unmarshal error rather than silently overwriting and orphaning indexes.
	err := repo.Upsert(ctx, tr, &store.User{Id: "corrupted-1", Name: "New", Email: "new@example.com"})
	if err == nil {
		t.Fatal("expected Upsert on corrupted existing record to return an error, got nil")
	}

	// Delete must return an unmarshal error rather than clearing primary key and orphaning indexes.
	err = repo.Delete(ctx, tr, fdblayer.Key{"corrupted-1"})
	if err == nil {
		t.Fatal("expected Delete on corrupted existing record to return an error, got nil")
	}
	if !kv.HasKey(corruptedKey) {
		t.Fatal("Delete cleared primary key despite failing to unmarshal existing record")
	}
}

func TestErrNotFound_SentinelWrapping(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewUserRepository(dir)

	_, err := repo.ReadRow(ctx, tr, fdblayer.Key{"missing-user"})
	if !errors.Is(err, fdblayer.ErrNotFound) {
		t.Fatalf("expected errors.Is(err, fdblayer.ErrNotFound) == true, got err=%v", err)
	}
}

func TestWriteAndScanOptions(t *testing.T) {
	if cfg := fdblayer.ApplyWriteOptions(nil); cfg.UserVersion != 0 {
		t.Fatalf("expected default UserVersion = 0, got %d", cfg.UserVersion)
	}
	if cfg := fdblayer.ApplyWriteOptions([]fdblayer.WriteOption{nil, fdblayer.WithUserVersion(42)}); cfg.UserVersion != 42 {
		t.Fatalf("expected UserVersion = 42, got %d", cfg.UserVersion)
	}
	scanCfg := fdblayer.ApplyScanOptions([]fdblayer.ScanOption{
		nil,
		fdblayer.WithLimit(15),
		fdblayer.WithReverse(),
		fdblayer.WithCursor(fdblayer.Key{"k1"}),
	})
	if scanCfg.Limit != 15 || !scanCfg.Reverse || len(scanCfg.Cursor) != 1 || scanCfg.Cursor[0] != "k1" {
		t.Fatalf("unexpected ScanConfig: %+v", scanCfg)
	}
}

func TestVersionstampUserVersion_MultiWriteAndConcurrency(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	// Single long-lived Repository instances reused across transactions.
	taskRepo := store.NewTaskMessageRepository(dir)
	auditRepo := store.NewAuditLogRepository(dir)

	// 1. Multiple versionstamped writes in the same transaction using fdblayer.WithUserVersion.
	if err := taskRepo.Enqueue(ctx, tr, &store.TaskMessage{
		QueueName: "q1",
		ShardId:   1,
		Payload:   []byte("first-in-tx"),
	}); err != nil {
		t.Fatalf("Enqueue 1 failed: %v", err)
	}
	if err := taskRepo.Enqueue(ctx, tr, &store.TaskMessage{
		QueueName: "q1",
		ShardId:   1,
		Payload:   []byte("second-in-tx"),
	}, fdblayer.WithUserVersion(1)); err != nil {
		t.Fatalf("Enqueue 2 failed: %v", err)
	}
	if err := auditRepo.Insert(ctx, tr, &store.AuditLog{
		Actor: "alice",
	}, fdblayer.WithUserVersion(2)); err != nil {
		t.Fatalf("AuditLog Insert failed: %v", err)
	}

	// Verify UserVersion 0 and 1 were assigned to the two TaskMessages and UserVersion 2 to AuditLog.
	vs0 := tuple.Versionstamp{TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, UserVersion: 0}
	vs1 := tuple.Versionstamp{TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, UserVersion: 1}
	vs2 := tuple.Versionstamp{TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, UserVersion: 2}

	if !kv.HasKey(dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "q1", vs0, uint64(1)})) {
		t.Fatal("expected first TaskMessage in tx to have UserVersion=0")
	}
	if !kv.HasKey(dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "q1", vs1, uint64(1)})) {
		t.Fatal("expected second TaskMessage in tx to have UserVersion=1")
	}
	if !kv.HasKey(dir.Pack(tuple.Tuple{store.AuditLogTypeID, fdblayer.DataNamespace, vs2})) {
		t.Fatal("expected AuditLog in tx to have UserVersion=2")
	}

	// 2. Another independent transaction defaults to UserVersion == 0 when omitted.
	kv3 := tests.NewMockKV()
	tr3 := tests.NewMockTransaction(kv3)
	if err := auditRepo.Insert(ctx, tr3, &store.AuditLog{Actor: "bob"}); err != nil {
		t.Fatalf("AuditLog Insert in tr3 failed: %v", err)
	}
	if !kv3.HasKey(dir.Pack(tuple.Tuple{store.AuditLogTypeID, fdblayer.DataNamespace, vs0})) {
		t.Fatal("expected AuditLog in tr3 to default to UserVersion=0")
	}

	// 3. Concurrent transactions sharing the same repositories are completely stateless.
	const numGoroutines = 16
	const writesPerTx = 5000
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localKV := tests.NewMockKV()
			localTr := tests.NewMockTransaction(localKV)
			for i := 0; i < writesPerTx; i++ {
				if err := taskRepo.Enqueue(ctx, localTr, &store.TaskMessage{
					QueueName: "concurrent_q",
					ShardId:   1,
					Payload:   []byte("msg"),
				}, fdblayer.WithUserVersion(uint16(i))); err != nil {
					errCh <- err
					return
				}
			}
			vsLast := tuple.Versionstamp{
				TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
				UserVersion:        uint16(writesPerTx - 1),
			}
			if !localKV.HasKey(dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "concurrent_q", vs0, uint64(1)})) {
				errCh <- errors.New("missing UserVersion 0 in concurrent tx")
				return
			}
			if !localKV.HasKey(dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "concurrent_q", vsLast, uint64(1)})) {
				errCh <- errors.New("missing last UserVersion in concurrent tx")
				return
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent userVersion test failed: %v", err)
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

func TestScanIndexProductByCategory_PaginatedAndReverse(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewProductRepository(dir)

	for _, id := range []string{"p1", "p2", "p3", "p4"} {
		if err := repo.Insert(ctx, tr, &store.Product{
			Id:       id,
			Name:     "Tool " + id,
			Category: "tools",
			Price:    10,
		}); err != nil {
			t.Fatalf("Insert %s failed: %v", id, err)
		}
	}

	// Forward pagination (2 items per page)
	p1, err := repo.ScanIndex(ctx, tr, store.ProductByCategory, fdblayer.Prefix("tools"), fdblayer.WithLimit(2))
	if err != nil || len(p1.Items) != 2 || !p1.HasMore || p1.Items[0].Id != "p1" || p1.Items[1].Id != "p2" {
		t.Fatalf("unexpected forward page 1: %+v, err=%v", p1, err)
	}
	p2, err := repo.ScanIndex(
		ctx,
		tr,
		store.ProductByCategory,
		fdblayer.Prefix("tools"),
		fdblayer.WithCursor(p1.NextCursor),
		fdblayer.WithLimit(2),
	)
	if err != nil || len(p2.Items) != 2 || p2.HasMore || p2.Items[0].Id != "p3" || p2.Items[1].Id != "p4" {
		t.Fatalf("unexpected forward page 2: %+v, err=%v", p2, err)
	}

	// Reverse pagination (2 items per page: p4, p3 -> p2, p1)
	rev1, err := repo.ScanIndex(
		ctx,
		tr,
		store.ProductByCategory,
		fdblayer.Prefix("tools"),
		fdblayer.WithLimit(2),
		fdblayer.WithReverse(),
	)
	if err != nil || len(rev1.Items) != 2 || !rev1.HasMore || rev1.Items[0].Id != "p4" || rev1.Items[1].Id != "p3" {
		t.Fatalf("unexpected reverse page 1: %+v, err=%v", rev1, err)
	}
	rev2, err := repo.ScanIndex(
		ctx,
		tr,
		store.ProductByCategory,
		fdblayer.Prefix("tools"),
		fdblayer.WithCursor(rev1.NextCursor),
		fdblayer.WithLimit(2),
		fdblayer.WithReverse(),
	)
	if err != nil || len(rev2.Items) != 2 || rev2.HasMore || rev2.Items[0].Id != "p2" || rev2.Items[1].Id != "p1" {
		t.Fatalf("unexpected reverse page 2: %+v, err=%v", rev2, err)
	}

	// ReadRows
	byPK, err := repo.ReadRows(ctx, tr, []fdblayer.Key{{"p1"}, {"p4"}, {"nonexistent"}})
	if err != nil || len(byPK) != 2 {
		t.Fatalf("ReadRows failed: got %+v, err=%v", byPK, err)
	}

	// Out-of-bounds cursors in ScanIndex must be clamped without inverted range panic
	oobForwardAfter, err := repo.ScanIndex(
		ctx,
		tr,
		store.ProductByCategory,
		fdblayer.Prefix("tools"),
		fdblayer.WithCursor(fdblayer.Key{"tools", "zzzzz\xff"}),
		fdblayer.WithLimit(2),
	)
	if err != nil || len(oobForwardAfter.Items) != 0 {
		t.Fatalf("expected 0 items for forward Cursor past end of index range, got %+v, err=%v", oobForwardAfter, err)
	}
	oobReverseBefore, err := repo.ScanIndex(
		ctx,
		tr,
		store.ProductByCategory,
		fdblayer.Prefix("tools"),
		fdblayer.WithCursor(fdblayer.Key{"tools", ""}),
		fdblayer.WithLimit(2),
		fdblayer.WithReverse(),
	)
	if err != nil {
		t.Fatalf("unexpected error for reverse Cursor at start of index range: %v", err)
	}
	_ = oobReverseBefore
}

func TestReversePagination_EmbeddedNulBytesInKeys(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewProductRepository(dir)

	for _, id := range []string{"a", "a\x00", "b"} {
		if err := repo.Insert(ctx, tr, &store.Product{
			Id:       id,
			Name:     "item-" + id,
			Category: "cat",
		}); err != nil {
			t.Fatalf("Insert(%q) failed: %v", id, err)
		}
	}

	// Verify reverse Scan pagination does not re-include "a\x00" when Cursor is {"a"}.
	listP1, err := repo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithLimit(1), fdblayer.WithReverse())
	if err != nil || len(listP1.Items) != 1 || listP1.Items[0].Id != "b" || !listP1.HasMore {
		t.Fatalf("unexpected reverse Scan page 1: %+v, err=%v", listP1, err)
	}
	listP2, err := repo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithLimit(1), fdblayer.WithReverse(), fdblayer.WithCursor(listP1.NextCursor))
	if err != nil || len(listP2.Items) != 1 || listP2.Items[0].Id != "a\x00" || !listP2.HasMore {
		t.Fatalf("unexpected reverse Scan page 2: %+v, err=%v", listP2, err)
	}
	listP3, err := repo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithLimit(1), fdblayer.WithReverse(), fdblayer.WithCursor(listP2.NextCursor))
	if err != nil || len(listP3.Items) != 1 || listP3.Items[0].Id != "a" || listP3.HasMore {
		t.Fatalf("unexpected reverse Scan page 3: %+v, err=%v", listP3, err)
	}

	// Verify reverse ScanIndex pagination does not re-include "a\x00" when Cursor is {"cat", "a"}.
	idxP1, err := repo.ScanIndex(ctx, tr, store.ProductByCategory, fdblayer.Prefix("cat"), fdblayer.WithLimit(1), fdblayer.WithReverse())
	if err != nil || len(idxP1.Items) != 1 || idxP1.Items[0].Id != "b" || !idxP1.HasMore {
		t.Fatalf("unexpected reverse ScanIndex page 1: %+v, err=%v", idxP1, err)
	}
	idxP2, err := repo.ScanIndex(ctx, tr, store.ProductByCategory, fdblayer.Prefix("cat"), fdblayer.WithLimit(1), fdblayer.WithReverse(), fdblayer.WithCursor(idxP1.NextCursor))
	if err != nil || len(idxP2.Items) != 1 || idxP2.Items[0].Id != "a\x00" || !idxP2.HasMore {
		t.Fatalf("unexpected reverse ScanIndex page 2: %+v, err=%v", idxP2, err)
	}
	idxP3, err := repo.ScanIndex(ctx, tr, store.ProductByCategory, fdblayer.Prefix("cat"), fdblayer.WithLimit(1), fdblayer.WithReverse(), fdblayer.WithCursor(idxP2.NextCursor))
	if err != nil || len(idxP3.Items) != 1 || idxP3.Items[0].Id != "a" || idxP3.HasMore {
		t.Fatalf("unexpected reverse ScanIndex page 3: %+v, err=%v", idxP3, err)
	}
}

func TestAggregateIndexes_Product_CountSumMinMax(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	repo := store.NewProductRepository(dir)

	// 1. Empty state: COUNT and SUM return (0, nil); MIN and MAX return ErrNotFound.
	if got, err := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{"books"}); err != nil || got != 0 {
		t.Fatalf("expected COUNT=0 for empty group, got %d, err=%v", got, err)
	}
	if got, err := repo.ReadAggregate(ctx, tr, store.ProductCount, fdblayer.Key{}); err != nil || got != 0 {
		t.Fatalf("expected ungrouped COUNT=0 for empty table, got %d, err=%v", got, err)
	}
	if got, err := repo.ReadAggregate(ctx, tr, store.ProductSumPriceByCategory, fdblayer.Key{"books"}); err != nil || got != 0 {
		t.Fatalf("expected SUM=0 for empty group, got %d, err=%v", got, err)
	}
	if _, err := repo.ReadAggregate(ctx, tr, store.ProductMinPriceByCategory, fdblayer.Key{"books"}); !errors.Is(err, fdblayer.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for MIN on empty group, got %v", err)
	}
	if _, err := repo.ReadAggregate(ctx, tr, store.ProductMaxPriceByCategory, fdblayer.Key{"books"}); !errors.Is(err, fdblayer.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for MAX on empty group, got %v", err)
	}

	// 2. Insert 3 products in "books" (including negative, zero, and positive prices) and 1 in "games".
	for _, p := range []*store.Product{
		{Id: "b1", Name: "Discount Coupon", Category: "books", Price: -15},
		{Id: "b2", Name: "Free Pamphlet", Category: "books", Price: 0},
		{Id: "b3", Name: "Hardcover", Category: "books", Price: 40},
		{Id: "g1", Name: "Board Game", Category: "games", Price: 60},
	} {
		if err := repo.Insert(ctx, tr, p); err != nil {
			t.Fatalf("Insert(%s) failed: %v", p.Id, err)
		}
	}

	if got, err := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{"books"}); err != nil || got != 3 {
		t.Fatalf("expected books COUNT=3, got %d, err=%v", got, err)
	}
	if got, err := repo.ReadAggregate(ctx, tr, store.ProductCount, fdblayer.Key{}); err != nil || got != 4 {
		t.Fatalf("expected total ProductCount=4, got %d, err=%v", got, err)
	}
	if got, err := repo.ReadAggregate(ctx, tr, store.ProductSumPriceByCategory, fdblayer.Key{"books"}); err != nil || got != 25 {
		t.Fatalf("expected books SUM=25 (-15 + 0 + 40), got %d, err=%v", got, err)
	}
	if got, err := repo.ReadAggregate(ctx, tr, store.ProductMinPriceByCategory, fdblayer.Key{"books"}); err != nil || got != -15 {
		t.Fatalf("expected books MIN=-15, got %d, err=%v", got, err)
	}
	if got, err := repo.ReadAggregate(ctx, tr, store.ProductMaxPriceByCategory, fdblayer.Key{"books"}); err != nil || got != 40 {
		t.Fatalf("expected books MAX=40, got %d, err=%v", got, err)
	}

	// 3. Upsert with unchanged Category and unchanged Price must emit 0 atomic mutations.
	tr.ResetLog()
	if err := repo.Upsert(ctx, tr, &store.Product{Id: "b3", Name: "Hardcover Revised", Category: "books", Price: 40}); err != nil {
		t.Fatalf("Upsert(b3 unchanged) failed: %v", err)
	}
	if len(tr.AddCalls) != 0 || len(tr.MinCalls) != 0 || len(tr.MaxCalls) != 0 {
		t.Fatalf("expected 0 atomic mutations on unchanged Upsert, got Add=%d Min=%d Max=%d", len(tr.AddCalls), len(tr.MinCalls), len(tr.MaxCalls))
	}

	// 4. Upsert with unchanged Category and changed Price (40 -> 55): delta (+15) applied to SUM, MIN/MAX updated, COUNT untouched.
	tr.ResetLog()
	if err := repo.Upsert(ctx, tr, &store.Product{Id: "b3", Name: "Collector Edition", Category: "books", Price: 55}); err != nil {
		t.Fatalf("Upsert(b3 price change) failed: %v", err)
	}
	if len(tr.AddCalls) != 1 {
		t.Fatalf("expected exactly 1 Add call (SUM delta) when only price changed, got %d", len(tr.AddCalls))
	}
	if got, err := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{"books"}); err != nil || got != 3 {
		t.Fatalf("expected books COUNT=3, got %d, err=%v", got, err)
	}
	if got, err := repo.ReadAggregate(ctx, tr, store.ProductSumPriceByCategory, fdblayer.Key{"books"}); err != nil || got != 40 {
		t.Fatalf("expected books SUM=40 (-15 + 0 + 55), got %d, err=%v", got, err)
	}
	if got, err := repo.ReadAggregate(ctx, tr, store.ProductMaxPriceByCategory, fdblayer.Key{"books"}); err != nil || got != 55 {
		t.Fatalf("expected books MAX=55, got %d, err=%v", got, err)
	}

	// 5. Upsert moving b3 from "books" to "games" with Price 70:
	//    books COUNT: 3 -> 2, books SUM: 40 -> -15
	//    games COUNT: 1 -> 2, games SUM: 60 -> 130, games MAX: 60 -> 70
	if err := repo.Upsert(ctx, tr, &store.Product{Id: "b3", Name: "Collector Edition", Category: "games", Price: 70}); err != nil {
		t.Fatalf("Upsert(b3 category move) failed: %v", err)
	}
	if got, _ := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{"books"}); got != 2 {
		t.Fatalf("expected books COUNT=2 after move, got %d", got)
	}
	if got, _ := repo.ReadAggregate(ctx, tr, store.ProductSumPriceByCategory, fdblayer.Key{"books"}); got != -15 {
		t.Fatalf("expected books SUM=-15 after move, got %d", got)
	}
	if got, _ := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{"games"}); got != 2 {
		t.Fatalf("expected games COUNT=2 after move, got %d", got)
	}
	if got, _ := repo.ReadAggregate(ctx, tr, store.ProductSumPriceByCategory, fdblayer.Key{"games"}); got != 130 {
		t.Fatalf("expected games SUM=130 after move, got %d", got)
	}
	if got, _ := repo.ReadAggregate(ctx, tr, store.ProductMaxPriceByCategory, fdblayer.Key{"games"}); got != 70 {
		t.Fatalf("expected games MAX=70 after move, got %d", got)
	}

	// 6. Delete b1 (Price -15 in "books"):
	//    books COUNT: 2 -> 1, books SUM: -15 -> 0, total ProductCount: 4 -> 3
	if err := repo.Delete(ctx, tr, fdblayer.Key{"b1"}); err != nil {
		t.Fatalf("Delete(b1) failed: %v", err)
	}
	if got, _ := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{"books"}); got != 1 {
		t.Fatalf("expected books COUNT=1 after Delete, got %d", got)
	}
	if got, _ := repo.ReadAggregate(ctx, tr, store.ProductSumPriceByCategory, fdblayer.Key{"books"}); got != 0 {
		t.Fatalf("expected books SUM=0 after Delete, got %d", got)
	}
	if got, _ := repo.ReadAggregate(ctx, tr, store.ProductCount, fdblayer.Key{}); got != 3 {
		t.Fatalf("expected total ProductCount=3 after Delete, got %d", got)
	}

	// 7. Error validation:
	//    - Passing non-aggregate index to ReadAggregate returns ErrUnknownIndex
	if _, err := repo.ReadAggregate(ctx, tr, store.ProductByCategory, fdblayer.Key{"books"}); !errors.Is(err, fdblayer.ErrUnknownIndex) {
		t.Fatalf("expected ErrUnknownIndex when calling ReadAggregate with standard index, got %v", err)
	}
	//    - Passing aggregate index to ScanIndex returns ErrUnknownIndex
	if _, err := repo.ScanIndex(ctx, tr, store.ProductCountByCategory, fdblayer.Prefix("books")); !errors.Is(err, fdblayer.ErrUnknownIndex) {
		t.Fatalf("expected ErrUnknownIndex when calling ScanIndex with aggregate index, got %v", err)
	}
	//    - Wrong groupKey length or element type returns ErrInvalidKey
	if _, err := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for empty groupKey on grouped aggregate, got %v", err)
	}
	if _, err := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{123}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for wrong element type in groupKey, got %v", err)
	}
}

func TestMutationIndexes_Post_CountAddMinMaxAndDelete(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	postRepo := store.NewPostRepository(dir)

	// 1. Empty state: COUNT and ADD return (0, nil); MIN and MAX return ErrNotFound.
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostCountById, fdblayer.Key{"post-1"}); err != nil || got != 0 {
		t.Fatalf("expected PostCountById=0 before Mutate, got %d, err=%v", got, err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostAddLikesById, fdblayer.Key{"post-1"}); err != nil || got != 0 {
		t.Fatalf("expected PostAddLikesById=0 before Mutate, got %d, err=%v", got, err)
	}
	if _, err := postRepo.ReadAggregate(ctx, tr, store.PostMinLatencyById, fdblayer.Key{"post-1"}); !errors.Is(err, fdblayer.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for PostMinLatencyById before Mutate, got %v", err)
	}
	if _, err := postRepo.ReadAggregate(ctx, tr, store.PostMaxLatencyById, fdblayer.Key{"post-1"}); !errors.Is(err, fdblayer.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for PostMaxLatencyById before Mutate, got %v", err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostCountGlobalViews, fdblayer.Key{}); err != nil || got != 0 {
		t.Fatalf("expected PostCountGlobalViews=0 before Mutate, got %d, err=%v", got, err)
	}

	// 2. Insert and Upsert do NOT touch manual mutation indexes.
	if err := postRepo.Insert(ctx, tr, &store.Post{Id: "post-1", Tags: []string{"go"}}); err != nil {
		t.Fatalf("Insert(post-1) failed: %v", err)
	}
	if err := postRepo.Upsert(ctx, tr, &store.Post{Id: "post-1", Tags: []string{"go", "fdb"}}); err != nil {
		t.Fatalf("Upsert(post-1) failed: %v", err)
	}
	if got, _ := postRepo.ReadAggregate(ctx, tr, store.PostCountById, fdblayer.Key{"post-1"}); got != 0 {
		t.Fatalf("expected Insert/Upsert not to modify PostCountById, got %d", got)
	}

	// 3. Mutate applies 0-RTT atomic operations (Add, Min, Max).
	tr.ResetLog()
	if err := postRepo.Mutate(ctx, tr, store.PostCountById, fdblayer.Key{"post-1"}, 1); err != nil {
		t.Fatalf("Mutate(PostCountById) failed: %v", err)
	}
	if err := postRepo.Mutate(ctx, tr, store.PostCountById, fdblayer.Key{"post-1"}, 2); err != nil {
		t.Fatalf("Mutate(PostCountById) failed: %v", err)
	}
	if err := postRepo.Mutate(ctx, tr, store.PostAddLikesById, fdblayer.Key{"post-1"}, 10); err != nil {
		t.Fatalf("Mutate(PostAddLikesById +10) failed: %v", err)
	}
	if err := postRepo.Mutate(ctx, tr, store.PostAddLikesById, fdblayer.Key{"post-1"}, -3); err != nil {
		t.Fatalf("Mutate(PostAddLikesById -3) failed: %v", err)
	}
	for _, lat := range []int64{45, -12, 120, 8} {
		if err := postRepo.Mutate(ctx, tr, store.PostMinLatencyById, fdblayer.Key{"post-1"}, lat); err != nil {
			t.Fatalf("Mutate(PostMinLatencyById %d) failed: %v", lat, err)
		}
		if err := postRepo.Mutate(ctx, tr, store.PostMaxLatencyById, fdblayer.Key{"post-1"}, lat); err != nil {
			t.Fatalf("Mutate(PostMaxLatencyById %d) failed: %v", lat, err)
		}
	}
	if err := postRepo.Mutate(ctx, tr, store.PostCountGlobalViews, fdblayer.Key{}, 5); err != nil {
		t.Fatalf("Mutate(PostCountGlobalViews) failed: %v", err)
	}
	if len(tr.AddCalls) != 5 || len(tr.MinCalls) != 4 || len(tr.MaxCalls) != 4 {
		t.Fatalf("expected 5 AddCalls, 4 MinCalls, 4 MaxCalls during Mutate, got Add=%d Min=%d Max=%d", len(tr.AddCalls), len(tr.MinCalls), len(tr.MaxCalls))
	}

	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostCountById, fdblayer.Key{"post-1"}); err != nil || got != 3 {
		t.Fatalf("expected PostCountById=3, got %d, err=%v", got, err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostAddLikesById, fdblayer.Key{"post-1"}); err != nil || got != 7 {
		t.Fatalf("expected PostAddLikesById=7, got %d, err=%v", got, err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostMinLatencyById, fdblayer.Key{"post-1"}); err != nil || got != -12 {
		t.Fatalf("expected PostMinLatencyById=-12, got %d, err=%v", got, err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostMaxLatencyById, fdblayer.Key{"post-1"}); err != nil || got != 120 {
		t.Fatalf("expected PostMaxLatencyById=120, got %d, err=%v", got, err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostCountGlobalViews, fdblayer.Key{}); err != nil || got != 5 {
		t.Fatalf("expected PostCountGlobalViews=5, got %d, err=%v", got, err)
	}

	// 4. Delete clears primary-key-scoped mutation indexes (PostCountById, PostAddLikesById, PostMinLatencyById, PostMaxLatencyById)
	//    while leaving shared/global mutation indexes (PostCountGlobalViews) intact.
	if err := postRepo.Delete(ctx, tr, fdblayer.Key{"post-1"}); err != nil {
		t.Fatalf("Delete(post-1) failed: %v", err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostCountById, fdblayer.Key{"post-1"}); err != nil || got != 0 {
		t.Fatalf("expected PostCountById=0 after Delete, got %d, err=%v", got, err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostAddLikesById, fdblayer.Key{"post-1"}); err != nil || got != 0 {
		t.Fatalf("expected PostAddLikesById=0 after Delete, got %d, err=%v", got, err)
	}
	if _, err := postRepo.ReadAggregate(ctx, tr, store.PostMinLatencyById, fdblayer.Key{"post-1"}); !errors.Is(err, fdblayer.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for PostMinLatencyById after Delete, got %v", err)
	}
	if _, err := postRepo.ReadAggregate(ctx, tr, store.PostMaxLatencyById, fdblayer.Key{"post-1"}); !errors.Is(err, fdblayer.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for PostMaxLatencyById after Delete, got %v", err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostCountGlobalViews, fdblayer.Key{}); err != nil || got != 5 {
		t.Fatalf("expected global PostCountGlobalViews=5 to remain intact after Delete, got %d, err=%v", got, err)
	}

	// 5. Validation errors:
	//    - Calling Mutate on a standard or automatic aggregate index returns ErrUnknownIndex
	if err := postRepo.Mutate(ctx, tr, store.PostByTags, fdblayer.Key{"go"}, 1); !errors.Is(err, fdblayer.ErrUnknownIndex) {
		t.Fatalf("expected ErrUnknownIndex when calling Mutate with standard index, got %v", err)
	}
	if err := postRepo.Mutate(ctx, tr, store.PostCountByTags, fdblayer.Key{"go"}, 1); !errors.Is(err, fdblayer.ErrUnknownIndex) {
		t.Fatalf("expected ErrUnknownIndex when calling Mutate with automatic aggregate index, got %v", err)
	}
	//    - Calling ScanIndex with a mutation index returns ErrUnknownIndex
	if _, err := postRepo.ScanIndex(ctx, tr, store.PostCountById, fdblayer.Prefix("post-1")); !errors.Is(err, fdblayer.ErrUnknownIndex) {
		t.Fatalf("expected ErrUnknownIndex when calling ScanIndex with mutation index, got %v", err)
	}
	//    - Calling Mutate with wrong key length or wrong element type returns ErrInvalidKey
	if err := postRepo.Mutate(ctx, tr, store.PostCountById, fdblayer.Key{}, 1); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for empty key on PostCountById, got %v", err)
	}
	if err := postRepo.Mutate(ctx, tr, store.PostCountById, fdblayer.Key{123}, 1); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for non-string key on PostCountById, got %v", err)
	}
}
