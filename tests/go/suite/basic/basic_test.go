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
	tx := fdblayer.NewTx(ctx, tr, dir)
	user := &store.User{Id: "u1", Name: "Alice", Email: "alice@example.com"}

	repo := store.NewUserRepository()
	if err := repo.Insert(tx, user); err != nil {
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	p := &store.Product{Id: "p1", Name: "Widget", Category: "tools", Price: 42}

	repo := store.NewProductRepository()
	if err := repo.Insert(tx, p); err != nil {
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewUserRepository()

	first := &store.User{Id: "u1", Name: "Alice", Email: "alice@example.com"}
	if err := repo.Insert(tx, first); err != nil {
		t.Fatalf("initial Insert failed: %v", err)
	}

	// Attempting to Insert a second time with the same PK and a different email
	// must fail with fdblayer.ErrAlreadyExists and leave both the primary record
	// and secondary index untouched.
	duplicate := &store.User{Id: "u1", Name: "Bob", Email: "bob@example.com"}
	err := repo.Insert(tx, duplicate)
	if !errors.Is(err, fdblayer.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists on duplicate Insert, got: %v", err)
	}

	got, err := repo.ReadRow(tx, fdblayer.Key{"u1"})
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	user := &store.User{Id: "u1", Name: "Alice", Email: "alice@example.com"}
	repo := store.NewUserRepository()
	_ = repo.Insert(tx, user)

	got, err := repo.ReadRow(tx, fdblayer.Key{"u1"})
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
	tx := fdblayer.NewTx(ctx, tr, dir)

	repo := store.NewUserRepository()
	_, err := repo.ReadRow(tx, fdblayer.Key{"nonexistent"})
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewProductRepository()
	_ = repo.Insert(tx, &store.Product{Id: "p1", Name: "Gizmo", Category: "tech", Price: 99})

	got, err := repo.ReadRow(tx, fdblayer.Key{"p1"})
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewUserRepository()
	_ = repo.Insert(tx, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})

	updated := &store.User{Id: "u1", Name: "Bob", Email: "b@test.com"}
	if err := repo.Upsert(tx, updated); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	got, _ := repo.ReadRow(tx, fdblayer.Key{"u1"})
	if got.Name != "Bob" || got.Email != "b@test.com" {
		t.Fatalf("update not applied: %+v", got)
	}
}

func TestUpsertUser_IndexUpdated(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	tx := fdblayer.NewTx(ctx, tr, dir)

	repo := store.NewUserRepository()
	_ = repo.Insert(tx, &store.User{Id: "u1", Name: "Alice", Email: "old@test.com"})
	oldIdx := tuple.Tuple{store.UserTypeID, fdblayer.IndexNamespace, int64(1), "old@test.com", "u1"}.Pack()
	if !kv.HasKey(oldIdx) {
		t.Fatal("old index should exist after insert")
	}

	// Update email
	_ = repo.Upsert(tx, &store.User{Id: "u1", Name: "Alice", Email: "new@test.com"})

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
	tx := fdblayer.NewTx(ctx, tr, dir)

	repo := store.NewUserRepository()
	if err := repo.Insert(tx, &store.User{Id: "u1", Name: "Alice", Email: "same@test.com"}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	tr.ResetLog()
	// Update Name only while keeping Email unchanged
	if err := repo.Upsert(tx, &store.User{Id: "u1", Name: "Alice Updated", Email: "same@test.com"}); err != nil {
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewProductRepository()
	_ = repo.Insert(tx, &store.Product{Id: "p1", Name: "X", Category: "a", Price: 10})

	_ = repo.Upsert(tx, &store.Product{Id: "p1", Name: "X", Category: "a", Price: 50})
	got, _ := repo.ReadRow(tx, fdblayer.Key{"p1"})
	if got.Price != 50 {
		t.Fatalf("expected price 50, got %d", got.Price)
	}
}

func TestUpsertProduct_IndexUpdated(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	tx := fdblayer.NewTx(ctx, tr, dir)

	repo := store.NewProductRepository()
	_ = repo.Insert(tx, &store.Product{Id: "p1", Name: "X", Category: "old_cat", Price: 1})
	oldIdx := tuple.Tuple{store.ProductTypeID, fdblayer.IndexNamespace, int64(1), "old_cat", "p1"}.Pack()
	if !kv.HasKey(oldIdx) {
		t.Fatal("old index should exist after insert")
	}

	_ = repo.Upsert(tx, &store.Product{Id: "p1", Name: "X", Category: "new_cat", Price: 1})
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewUserRepository()
	_ = repo.Insert(tx, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})

	if err := repo.Delete(tx, fdblayer.Key{"u1"}); err != nil {
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
	_, err := repo.ReadRow(tx, fdblayer.Key{"u1"})
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestDeleteUser_NonExistent(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewUserRepository()
	// Should not error or panic
	if err := repo.Delete(tx, fdblayer.Key{"ghost"}); err != nil {
		t.Fatalf("delete of non-existent should not error, got: %v", err)
	}
}

func TestDeleteProduct_ClearsIndex(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewProductRepository()
	_ = repo.Insert(tx, &store.Product{Id: "p1", Name: "X", Category: "cat1", Price: 5})

	_ = repo.Delete(tx, fdblayer.Key{"p1"})
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
	tx := fdblayer.NewTx(ctx, tr, dir)

	repo := store.NewUserRepository()
	results, err := repo.ScanIndex(tx, store.UserByEmail, fdblayer.Prefix("nobody@test.com"), fdblayer.ScanOptions{})
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
	tx := fdblayer.NewTx(ctx, tr, dir)

	repo := store.NewProductRepository()
	results, err := repo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("nonexistent"), fdblayer.ScanOptions{})
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewUserRepository()

	// Unknown index ID must return ErrUnknownIndex
	if _, err := repo.ScanIndex(tx, fdblayer.IndexID(999), fdblayer.AllKeys(), fdblayer.ScanOptions{}); !errors.Is(err, fdblayer.ErrUnknownIndex) {
		t.Fatalf("expected ErrUnknownIndex, got %v", err)
	}

	// Wrong PK arity in ReadRow / Delete / ReadRows must return ErrInvalidKey
	if _, err := repo.ReadRow(tx, fdblayer.Key{"u1", "extra"}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on ReadRow arity mismatch, got %v", err)
	}
	if _, err := repo.ReadRow(tx, fdblayer.Key{123}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on ReadRow type mismatch, got %v", err)
	}
	if err := repo.Delete(tx, fdblayer.Key{}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on Delete arity mismatch, got %v", err)
	}
	if _, err := repo.ReadRows(tx, []fdblayer.Key{{"u1"}, {"u2", "extra"}}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on ReadRows arity mismatch, got %v", err)
	}
	// Exceeding key arity in Scan / ScanIndex must return ErrInvalidKey
	if _, err := repo.Scan(tx, fdblayer.Prefix("u1", "extra"), fdblayer.ScanOptions{}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on Scan prefix arity overflow, got %v", err)
	}
	if _, err := repo.ScanIndex(tx, store.UserByEmail, fdblayer.Prefix("a@b.com", "u1", "extra"), fdblayer.ScanOptions{}); !errors.Is(err, fdblayer.ErrInvalidKey) {
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewUserRepository()
	for _, id := range []string{"u1", "u2", "u3"} {
		_ = repo.Insert(tx, &store.User{Id: id, Name: "Name" + id, Email: id + "@test.com"})
	}

	keys := []fdblayer.Key{{"u1"}, {"u2"}, {"u3"}}
	result, err := repo.ReadRows(tx, keys)
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewUserRepository()
	_ = repo.Insert(tx, &store.User{Id: "u1", Name: "A", Email: "a@t.com"})
	_ = repo.Insert(tx, &store.User{Id: "u2", Name: "B", Email: "b@t.com"})

	keys := []fdblayer.Key{{"u1"}, {"u2"}, {"u3"}} // u3 doesn't exist
	result, err := repo.ReadRows(tx, keys)
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
	tx := fdblayer.NewTx(ctx, tr, dir)

	repo := store.NewUserRepository()
	keys := []fdblayer.Key{{"x1"}, {"x2"}}
	result, err := repo.ReadRows(tx, keys)
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
	tx := fdblayer.NewTx(ctx, tr, dir)

	repo := store.NewUserRepository()
	result, err := repo.ReadRows(tx, []fdblayer.Key{})
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewProductRepository()
	_ = repo.Insert(tx, &store.Product{Id: "p1", Name: "A", Category: "c", Price: 1})
	_ = repo.Insert(tx, &store.Product{Id: "p2", Name: "B", Category: "c", Price: 2})

	keys := []fdblayer.Key{{"p1"}, {"p2"}}
	result, err := repo.ReadRows(tx, keys)
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewUserRepository()
	_ = repo.Insert(tx, &store.User{Id: "u1", Name: "A", Email: "a@t.com"})

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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewProductRepository()
	_ = repo.Insert(tx, &store.Product{Id: "p1", Name: "A", Category: "tools", Price: 1})
	_ = repo.Insert(tx, &store.Product{Id: "p2", Name: "B", Category: "tools", Price: 2})

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
	tx := fdblayer.NewTx(ctx, tr, dir)

	// Instantiate the uniform Repository[*store.User]
	var repo fdblayer.Repository[*store.User] = store.NewUserRepository()

	// 1. Insert
	user := &store.User{Id: "gen-1", Name: "Generic User", Email: "gen@example.com"}
	if err := repo.Insert(tx, user); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	pk := tuple.Tuple{store.UserTypeID, fdblayer.DataNamespace, "gen-1"}.Pack()
	if !kv.HasKey(pk) {
		t.Fatal("Insert did not write primary key")
	}

	// 2. ReadRow
	got, err := repo.ReadRow(tx, fdblayer.Key{"gen-1"})
	if err != nil {
		t.Fatalf("ReadRow failed: %v", err)
	}
	if got.Name != "Generic User" || got.Email != "gen@example.com" {
		t.Fatalf("ReadRow returned unexpected user: %+v", got)
	}

	// 3. Upsert (Update)
	got.Name = "Updated Generic Name"
	if err := repo.Upsert(tx, got); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	updated, err := repo.ReadRow(tx, fdblayer.Key{"gen-1"})
	if err != nil {
		t.Fatalf("ReadRow after Upsert failed: %v", err)
	}
	if updated.Name != "Updated Generic Name" {
		t.Fatalf("Upsert did not update name: %s", updated.Name)
	}

	// 4. Delete
	if err := repo.Delete(tx, fdblayer.Key{"gen-1"}); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if kv.HasKey(pk) {
		t.Fatal("Delete did not clear primary key")
	}

	_, err = repo.ReadRow(tx, fdblayer.Key{"gen-1"})
	if err == nil {
		t.Fatal("expected error getting deleted user")
	}
}

func TestUpsertAndDelete_CorruptedExistingRecordReturnsError(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewUserRepository()

	corruptedKey := dir.Pack(tuple.Tuple{store.UserTypeID, fdblayer.DataNamespace, "corrupted-1"})
	// Write invalid protobuf wire format bytes (field 0 is invalid in protobuf wire format)
	tr.Set(corruptedKey, []byte{0x00, 0xff, 0xff})

	// Upsert must return an unmarshal error rather than silently overwriting and orphaning indexes.
	err := repo.Upsert(tx, &store.User{Id: "corrupted-1", Name: "New", Email: "new@example.com"})
	if err == nil {
		t.Fatal("expected Upsert on corrupted existing record to return an error, got nil")
	}

	// Delete must return an unmarshal error rather than clearing primary key and orphaning indexes.
	err = repo.Delete(tx, fdblayer.Key{"corrupted-1"})
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewUserRepository()

	_, err := repo.ReadRow(tx, fdblayer.Key{"missing-user"})
	if !errors.Is(err, fdblayer.ErrNotFound) {
		t.Fatalf("expected errors.Is(err, fdblayer.ErrNotFound) == true, got err=%v", err)
	}
}

func TestUserVersionSequence_OverflowAndReset(t *testing.T) {
	tr, dir, _ := tests.Setup()
	tx := fdblayer.NewTx(context.Background(), tr, dir)
	if tx.Transaction() != tr {
		t.Fatal("expected tx.Transaction() to return underlying transaction")
	}
	if tx.Dir() != dir {
		t.Fatal("expected tx.Dir() to return underlying directory subspace")
	}
	for i := 0; i < 65536; i++ {
		uv, err := tx.NextUserVersion()
		if err != nil {
			t.Fatalf("unexpected error at iteration %d: %v", i, err)
		}
		if int(uv) != i {
			t.Fatalf("expected UserVersion=%d, got %d", i, uv)
		}
	}
	if _, err := tx.NextUserVersion(); !errors.Is(err, fdblayer.ErrUserVersionOverflow) {
		t.Fatalf("expected ErrUserVersionOverflow at 65537th call, got: %v", err)
	}
	tx.ResetUserVersion()
	uv, err := tx.NextUserVersion()
	if err != nil || uv != 0 {
		t.Fatalf("expected UserVersion=0 after ResetUserVersion, got uv=%d, err=%v", uv, err)
	}
}

func TestTx_PerTransactionUserVersionIsolation(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	// Single long-lived Repository instances reused across transactions.
	taskRepo := store.NewTaskMessageRepository()
	auditRepo := store.NewAuditLogRepository()

	// 1. Simulate a transaction attempt that exhausts all 65,536 user versions and then retries.
	exhaustedTx := fdblayer.NewTx(ctx, tests.NewMockTransaction(tests.NewMockKV()), dir)
	for i := 0; i < 65536; i++ {
		if _, err := exhaustedTx.NextUserVersion(); err != nil {
			t.Fatalf("unexpected error on exhaustedTx at %d: %v", i, err)
		}
	}
	if err := taskRepo.Enqueue(exhaustedTx, &store.TaskMessage{
		QueueName: "q1",
		ShardId:   1,
		Payload:   []byte("overflow"),
	}); !errors.Is(err, fdblayer.ErrUserVersionOverflow) {
		t.Fatalf("expected ErrUserVersionOverflow on exhaustedTx, got %v", err)
	}

	// 2. A new transaction (or retry attempt) creates a fresh *fdblayer.Tx against the SAME
	// Repository and starts cleanly at UserVersion == 0.
	tx2 := fdblayer.NewTx(ctx, tr, dir)
	if err := taskRepo.Enqueue(tx2, &store.TaskMessage{
		QueueName: "q1",
		ShardId:   1,
		Payload:   []byte("first-in-tx2"),
	}); err != nil {
		t.Fatalf("Enqueue 1 in tx2 failed: %v", err)
	}
	if err := taskRepo.Enqueue(tx2, &store.TaskMessage{
		QueueName: "q1",
		ShardId:   1,
		Payload:   []byte("second-in-tx2"),
	}); err != nil {
		t.Fatalf("Enqueue 2 in tx2 failed: %v", err)
	}
	if err := auditRepo.Insert(tx2, &store.AuditLog{
		Actor: "alice",
	}); err != nil {
		t.Fatalf("AuditLog Insert in tx2 failed: %v", err)
	}

	// Verify UserVersion 0 and 1 were assigned to the two TaskMessages and UserVersion 2 to AuditLog.
	vs0 := tuple.Versionstamp{TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, UserVersion: 0}
	vs1 := tuple.Versionstamp{TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, UserVersion: 1}
	vs2 := tuple.Versionstamp{TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, UserVersion: 2}

	if !kv.HasKey(dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "q1", vs0, uint64(1)})) {
		t.Fatal("expected first TaskMessage in tx2 to have UserVersion=0")
	}
	if !kv.HasKey(dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "q1", vs1, uint64(1)})) {
		t.Fatal("expected second TaskMessage in tx2 to have UserVersion=1")
	}
	if !kv.HasKey(dir.Pack(tuple.Tuple{store.AuditLogTypeID, fdblayer.DataNamespace, vs2})) {
		t.Fatal("expected AuditLog in tx2 to have UserVersion=2")
	}

	// 3. Another independent transaction tx3 starts its own sequence at UserVersion == 0.
	kv3 := tests.NewMockKV()
	tx3 := fdblayer.NewTx(ctx, tests.NewMockTransaction(kv3), dir)
	if err := auditRepo.Insert(tx3, &store.AuditLog{Actor: "bob"}); err != nil {
		t.Fatalf("AuditLog Insert in tx3 failed: %v", err)
	}
	if !kv3.HasKey(dir.Pack(tuple.Tuple{store.AuditLogTypeID, fdblayer.DataNamespace, vs0})) {
		t.Fatal("expected AuditLog in tx3 to start at UserVersion=0")
	}

	// 4. Concurrent transactions sharing the same repositories never leak
	// or collide UserVersions across transactions, and cumulative writes > 65,536 across
	// transactions succeed without ErrUserVersionOverflow.
	const numGoroutines = 16
	const writesPerTx = 5000 // 16 * 5000 = 80,000 > 65,536
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localKV := tests.NewMockKV()
			localTx := fdblayer.NewTx(ctx, tests.NewMockTransaction(localKV), dir)
			for i := 0; i < writesPerTx; i++ {
				if err := taskRepo.Enqueue(localTx, &store.TaskMessage{
					QueueName: "concurrent_q",
					ShardId:   1,
					Payload:   []byte("msg"),
				}); err != nil {
					errCh <- err
					return
				}
			}
			// Verify the first and last UserVersion in this transaction are 0 and writesPerTx-1.
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
		t.Fatalf("concurrent per-Tx userVersion test failed: %v", err)
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
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewProductRepository()

	for _, id := range []string{"p1", "p2", "p3", "p4"} {
		if err := repo.Insert(tx, &store.Product{
			Id:       id,
			Name:     "Tool " + id,
			Category: "tools",
			Price:    10,
		}); err != nil {
			t.Fatalf("Insert %s failed: %v", id, err)
		}
	}

	// Forward pagination (2 items per page)
	p1, err := repo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("tools"), fdblayer.ScanOptions{Limit: 2})
	if err != nil || len(p1.Items) != 2 || !p1.HasMore || p1.Items[0].Id != "p1" || p1.Items[1].Id != "p2" {
		t.Fatalf("unexpected forward page 1: %+v, err=%v", p1, err)
	}
	p2, err := repo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("tools"), fdblayer.ScanOptions{
		Cursor: p1.NextCursor,
		Limit:  2,
	})
	if err != nil || len(p2.Items) != 2 || p2.HasMore || p2.Items[0].Id != "p3" || p2.Items[1].Id != "p4" {
		t.Fatalf("unexpected forward page 2: %+v, err=%v", p2, err)
	}

	// Reverse pagination (2 items per page: p4, p3 -> p2, p1)
	rev1, err := repo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("tools"), fdblayer.ScanOptions{
		Limit:   2,
		Reverse: true,
	})
	if err != nil || len(rev1.Items) != 2 || !rev1.HasMore || rev1.Items[0].Id != "p4" || rev1.Items[1].Id != "p3" {
		t.Fatalf("unexpected reverse page 1: %+v, err=%v", rev1, err)
	}
	rev2, err := repo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("tools"), fdblayer.ScanOptions{
		Cursor:  rev1.NextCursor,
		Limit:   2,
		Reverse: true,
	})
	if err != nil || len(rev2.Items) != 2 || rev2.HasMore || rev2.Items[0].Id != "p2" || rev2.Items[1].Id != "p1" {
		t.Fatalf("unexpected reverse page 2: %+v, err=%v", rev2, err)
	}

	// ReadRows
	byPK, err := repo.ReadRows(tx, []fdblayer.Key{{"p1"}, {"p4"}, {"nonexistent"}})
	if err != nil || len(byPK) != 2 {
		t.Fatalf("ReadRows failed: got %+v, err=%v", byPK, err)
	}

	// Out-of-bounds cursors in ScanIndex must be clamped without inverted range panic
	oobForwardAfter, err := repo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("tools"), fdblayer.ScanOptions{
		Cursor: fdblayer.Key{"tools", "zzzzz\xff"},
		Limit:  2,
	})
	if err != nil || len(oobForwardAfter.Items) != 0 {
		t.Fatalf("expected 0 items for forward Cursor past end of index range, got %+v, err=%v", oobForwardAfter, err)
	}
	oobReverseBefore, err := repo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("tools"), fdblayer.ScanOptions{
		Cursor:  fdblayer.Key{"tools", ""},
		Limit:   2,
		Reverse: true,
	})
	if err != nil {
		t.Fatalf("unexpected error for reverse Cursor at start of index range: %v", err)
	}
	_ = oobReverseBefore
}

func TestReversePagination_EmbeddedNulBytesInKeys(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	tx := fdblayer.NewTx(ctx, tr, dir)
	repo := store.NewProductRepository()

	for _, id := range []string{"a", "a\x00", "b"} {
		if err := repo.Insert(tx, &store.Product{
			Id:       id,
			Name:     "item-" + id,
			Category: "cat",
		}); err != nil {
			t.Fatalf("Insert(%q) failed: %v", id, err)
		}
	}

	// Verify reverse Scan pagination does not re-include "a\x00" when Cursor is {"a"}.
	listP1, err := repo.Scan(tx, fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 1, Reverse: true})
	if err != nil || len(listP1.Items) != 1 || listP1.Items[0].Id != "b" || !listP1.HasMore {
		t.Fatalf("unexpected reverse Scan page 1: %+v, err=%v", listP1, err)
	}
	listP2, err := repo.Scan(tx, fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 1, Reverse: true, Cursor: listP1.NextCursor})
	if err != nil || len(listP2.Items) != 1 || listP2.Items[0].Id != "a\x00" || !listP2.HasMore {
		t.Fatalf("unexpected reverse Scan page 2: %+v, err=%v", listP2, err)
	}
	listP3, err := repo.Scan(tx, fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 1, Reverse: true, Cursor: listP2.NextCursor})
	if err != nil || len(listP3.Items) != 1 || listP3.Items[0].Id != "a" || listP3.HasMore {
		t.Fatalf("unexpected reverse Scan page 3: %+v, err=%v", listP3, err)
	}

	// Verify reverse ScanIndex pagination does not re-include "a\x00" when Cursor is {"cat", "a"}.
	idxP1, err := repo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("cat"), fdblayer.ScanOptions{Limit: 1, Reverse: true})
	if err != nil || len(idxP1.Items) != 1 || idxP1.Items[0].Id != "b" || !idxP1.HasMore {
		t.Fatalf("unexpected reverse ScanIndex page 1: %+v, err=%v", idxP1, err)
	}
	idxP2, err := repo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("cat"), fdblayer.ScanOptions{Limit: 1, Reverse: true, Cursor: idxP1.NextCursor})
	if err != nil || len(idxP2.Items) != 1 || idxP2.Items[0].Id != "a\x00" || !idxP2.HasMore {
		t.Fatalf("unexpected reverse ScanIndex page 2: %+v, err=%v", idxP2, err)
	}
	idxP3, err := repo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("cat"), fdblayer.ScanOptions{Limit: 1, Reverse: true, Cursor: idxP2.NextCursor})
	if err != nil || len(idxP3.Items) != 1 || idxP3.Items[0].Id != "a" || idxP3.HasMore {
		t.Fatalf("unexpected reverse ScanIndex page 3: %+v, err=%v", idxP3, err)
	}
}
