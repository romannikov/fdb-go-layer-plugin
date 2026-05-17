//go:build integration

package basic_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	fdblayer "github.com/romannikov/fdb-go-layer-plugin/fdb-layer"
	"github.com/romannikov/fdb-go-layer-plugin/tests"
	"github.com/romannikov/fdb-go-layer-plugin/tests/store"
)

func init() {
	fdb.MustAPIVersion(710)
}

// Metadata
func TestIntegration_SyncMetadata(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"})
	})

	meta := recordStore.Metadata()
	if meta["User"] == 0 || meta["Product"] == 0 {
		t.Fatalf("metadata not populated: %v", meta)
	}
	if meta["User"] == meta["Product"] {
		t.Fatal("User and Product got the same type ID")
	}
}

func TestIntegration_SyncMetadata_Idempotent(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"})
	})
	first := recordStore.Metadata()

	// Second sync should produce identical IDs
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"})
	})
	second := recordStore.Metadata()

	if first["User"] != second["User"] || first["Product"] != second["Product"] {
		t.Fatalf("IDs changed: %v → %v", first, second)
	}
}

// CRUD Round-Trip
func TestIntegration_CreateAndGetUser(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		return userRepo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"})
	})

	var user *store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		user, err = userRepo.Get(ctx, tr, dir, "u1")
		return err
	})
	if user.Name != "Alice" || user.Email != "alice@test.com" {
		t.Fatalf("unexpected user: %+v", user)
	}
}

func TestIntegration_CreateAndGetProduct(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	productRepo := store.NewProductRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		return productRepo.Create(ctx, tr, dir, &store.Product{Id: "p1", Name: "Widget", Category: "tools", Price: 42})
	})

	var product *store.Product
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		product, err = productRepo.Get(ctx, tr, dir, "p1")
		return err
	})
	if product.Name != "Widget" || product.Price != 42 {
		t.Fatalf("unexpected product: %+v", product)
	}
}

func TestIntegration_GetUser_NotFound(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"})
	})

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return userRepo.Get(ctx, tr, dir, "nonexistent")
	})
	if err == nil {
		t.Fatal("expected not found error")
	}
}

// Set (Update)
func TestIntegration_SetUser_UpdateFields(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		return userRepo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Set(ctx, tr, dir, &store.User{Id: "u1", Name: "Bob", Email: "b@test.com"})
	})

	var user *store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		user, err = userRepo.Get(ctx, tr, dir, "u1")
		return err
	})
	if user.Name != "Bob" || user.Email != "b@test.com" {
		t.Fatalf("update not applied: %+v", user)
	}
}

// Delete
func TestIntegration_DeleteUser(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		return userRepo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Delete(ctx, tr, dir, "u1")
	})

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return userRepo.Get(ctx, tr, dir, "u1")
	})
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

// Secondary Index Lookups
func TestIntegration_GetUserByEmail(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		if err := userRepo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"}); err != nil {
			return err
		}
		if err := userRepo.Create(ctx, tr, dir, &store.User{Id: "u2", Name: "Bob", Email: "bob@test.com"}); err != nil {
			return err
		}
		return userRepo.Create(ctx, tr, dir, &store.User{Id: "u3", Name: "Charlie", Email: "alice@test.com"})
	})

	// Should find two users with email "alice@test.com"
	var users []*store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetUserByEmail(ctx, tr, dir, "alice@test.com")
		return err
	})
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	// Should find one user with email "bob@test.com"
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetUserByEmail(ctx, tr, dir, "bob@test.com")
		return err
	})
	if len(users) != 1 || users[0].Name != "Bob" {
		t.Fatalf("unexpected result: %+v", users)
	}

	// Should find zero for nonexistent email
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetUserByEmail(ctx, tr, dir, "nobody@test.com")
		return err
	})
	if len(users) != 0 {
		t.Fatalf("expected 0, got %d", len(users))
	}
}

func TestIntegration_GetProductByCategory(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	productRepo := store.NewProductRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		if err := productRepo.Create(ctx, tr, dir, &store.Product{Id: "p1", Name: "Hammer", Category: "tools", Price: 15}); err != nil {
			return err
		}
		if err := productRepo.Create(ctx, tr, dir, &store.Product{Id: "p2", Name: "Drill", Category: "tools", Price: 80}); err != nil {
			return err
		}
		return productRepo.Create(ctx, tr, dir, &store.Product{Id: "p3", Name: "Laptop", Category: "electronics", Price: 999})
	})

	var products []*store.Product
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		products, err = productRepo.GetProductByCategory(ctx, tr, dir, "tools")
		return err
	})
	if len(products) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(products))
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		products, err = productRepo.GetProductByCategory(ctx, tr, dir, "electronics")
		return err
	})
	if len(products) != 1 || products[0].Name != "Laptop" {
		t.Fatalf("unexpected: %+v", products)
	}
}

func TestIntegration_SetUser_IndexUpdated(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		return userRepo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "old@test.com"})
	})

	// Update email
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Set(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "new@test.com"})
	})

	// Old email should return empty
	var users []*store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetUserByEmail(ctx, tr, dir, "old@test.com")
		return err
	})
	if len(users) != 0 {
		t.Fatal("stale index: old email still returns results")
	}

	// New email should return the user
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetUserByEmail(ctx, tr, dir, "new@test.com")
		return err
	})
	if len(users) != 1 || users[0].Name != "Alice" {
		t.Fatalf("expected Alice at new email, got %+v", users)
	}
}

func TestIntegration_DeleteUser_ClearsIndex(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		return userRepo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Delete(ctx, tr, dir, "u1")
	})

	var users []*store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetUserByEmail(ctx, tr, dir, "alice@test.com")
		return err
	})
	if len(users) != 0 {
		t.Fatal("index not cleared after delete")
	}
}

// List / Pagination
func TestIntegration_ListUser_Basic(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("u%d", i)
			if err := userRepo.Create(ctx, tr, dir, &store.User{
				Id: id, Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("u%d@test.com", i),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	var result *store.UserPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.ListUser(ctx, tr, dir, store.UserPaginationOptions{Limit: 10})
		return err
	})
	if len(result.Items) != 5 {
		t.Fatalf("expected 5 users, got %d", len(result.Items))
	}
	if result.HasMore {
		t.Fatal("should not have more pages")
	}
}

func TestIntegration_ListUser_Pagination(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("u%d", i)
			if err := userRepo.Create(ctx, tr, dir, &store.User{
				Id: id, Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("u%d@test.com", i),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// First page: limit 2
	var page1 *store.UserPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page1, err = userRepo.ListUser(ctx, tr, dir, store.UserPaginationOptions{Limit: 2})
		return err
	})
	if len(page1.Items) != 2 {
		t.Fatalf("page1: expected 2 items, got %d", len(page1.Items))
	}
	if !page1.HasMore {
		t.Fatal("page1: expected HasMore=true")
	}
	if page1.NextKey == nil {
		t.Fatal("page1: NextKey should not be nil")
	}

	// Second page: use NextKey
	var page2 *store.UserPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page2, err = userRepo.ListUser(ctx, tr, dir, store.UserPaginationOptions{Begin: page1.NextKey, Limit: 2})
		return err
	})
	if len(page2.Items) != 2 {
		t.Fatalf("page2: expected 2 items, got %d", len(page2.Items))
	}
	if !page2.HasMore {
		t.Fatal("page2: expected HasMore=true")
	}

	// Third page: should have 1 remaining
	var page3 *store.UserPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page3, err = userRepo.ListUser(ctx, tr, dir, store.UserPaginationOptions{Begin: page2.NextKey, Limit: 2})
		return err
	})
	if len(page3.Items) != 1 {
		t.Fatalf("page3: expected 1 item, got %d", len(page3.Items))
	}
	if page3.HasMore {
		t.Fatal("page3: expected HasMore=false")
	}
}

func TestIntegration_ListProduct_Pagination(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	productRepo := store.NewProductRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		for i := 0; i < 7; i++ {
			if err := productRepo.Create(ctx, tr, dir, &store.Product{
				Id: fmt.Sprintf("p%d", i), Name: fmt.Sprintf("Product%d", i),
				Category: "cat", Price: int32(i * 10),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// Page through with limit 3
	var page1 *store.ProductPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page1, err = productRepo.ListProduct(ctx, tr, dir, store.ProductPaginationOptions{Limit: 3})
		return err
	})
	if len(page1.Items) != 3 {
		t.Fatalf("expected 3, got %d", len(page1.Items))
	}
	if !page1.HasMore {
		t.Fatal("expected more pages")
	}

	var page2 *store.ProductPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page2, err = productRepo.ListProduct(ctx, tr, dir, store.ProductPaginationOptions{Begin: page1.NextKey, Limit: 3})
		return err
	})
	if len(page2.Items) != 3 {
		t.Fatalf("expected 3, got %d", len(page2.Items))
	}

	var page3 *store.ProductPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page3, err = productRepo.ListProduct(ctx, tr, dir, store.ProductPaginationOptions{Begin: page2.NextKey, Limit: 3})
		return err
	})
	if len(page3.Items) != 1 {
		t.Fatalf("expected 1, got %d", len(page3.Items))
	}
	if page3.HasMore {
		t.Fatal("should not have more")
	}
}

func TestIntegration_ListUser_Empty(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"})
	})

	var result *store.UserPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.ListUser(ctx, tr, dir, store.UserPaginationOptions{Limit: 10})
		return err
	})
	if len(result.Items) != 0 {
		t.Fatalf("expected empty list, got %d", len(result.Items))
	}
}

// BatchGet
func TestIntegration_BatchGetUser(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		for _, id := range []string{"u1", "u2", "u3"} {
			if err := userRepo.Create(ctx, tr, dir, &store.User{Id: id, Name: "Name" + id, Email: id + "@test.com"}); err != nil {
				return err
			}
		}
		return nil
	})

	var result map[string]*store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.BatchGetUser(ctx, tr, dir, []tuple.Tuple{{"u1"}, {"u2"}, {"u3"}})
		return err
	})
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
}

func TestIntegration_GenericRepository(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"})
	})

	// Wrap store in the generated UserRepository
	var repo store.UserRepository = store.NewUserRepository(recordStore)
	var genRepo fdblayer.GenericRepository[*store.User, string] = repo

	// 1. Create via generic repository using real FDB transaction
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return genRepo.Create(ctx, tr, dir, &store.User{Id: "g1", Name: "Generic Alice", Email: "g-alice@test.com"})
	})

	// 2. Get via generic repository
	var got *store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = genRepo.Get(ctx, tr, dir, "g1")
		return err
	})
	if got == nil || got.Name != "Generic Alice" {
		t.Fatalf("unexpected user: %+v", got)
	}

	// 3. Set via generic repository
	got.Name = "Updated Generic Alice"
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return genRepo.Set(ctx, tr, dir, got)
	})

	// Verify update
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = genRepo.Get(ctx, tr, dir, "g1")
		return err
	})
	if got == nil || got.Name != "Updated Generic Alice" {
		t.Fatalf("update not applied: %+v", got)
	}

	// 4. Delete via generic repository
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return genRepo.Delete(ctx, tr, dir, "g1")
	})

	// Verify deletion
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return genRepo.Get(ctx, tr, dir, "g1")
	})
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}
