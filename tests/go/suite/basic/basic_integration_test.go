//go:build integration

package basic_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	tests "github.com/romannikov/fdb-layer/tests/go"
	"github.com/romannikov/fdb-layer/tests/go/store"
)

func init() {
	fdb.MustAPIVersion(710)
}

// CRUD Round-Trip
func TestIntegration_InsertAndReadRowUser(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	if err := fdblayer.Transact(ctx, db, dir, func(tx *fdblayer.Tx) error {
		return userRepo.Insert(tx, &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"})
	}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	user, err := fdblayer.TransactVal(ctx, db, dir, func(tx *fdblayer.Tx) (*store.User, error) {
		return userRepo.ReadRow(tx, fdblayer.Key{"u1"})
	})
	if err != nil {
		t.Fatalf("ReadRow failed: %v", err)
	}
	if user.Name != "Alice" || user.Email != "alice@test.com" {
		t.Fatalf("unexpected user: %+v", user)
	}
}

func TestIntegration_InsertUser_AlreadyExists(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Insert(fdblayer.NewTx(ctx, tr, dir), &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"})
	})

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return nil, userRepo.Insert(fdblayer.NewTx(ctx, tr, dir), &store.User{Id: "u1", Name: "Bob", Email: "bob@test.com"})
	})
	if !errors.Is(err, fdblayer.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists on duplicate Insert, got: %v", err)
	}

	var byBob []*store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		res, err := userRepo.ScanIndex(fdblayer.NewTx(ctx, tr, dir), store.UserByEmail, fdblayer.Prefix("bob@test.com"), fdblayer.ScanOptions{})
		if err != nil {
			return err
		}
		byBob = res.Items
		return nil
	})
	if len(byBob) != 0 {
		t.Fatalf("expected 0 users for bob@test.com after rejected Insert, got %d", len(byBob))
	}
}

func TestIntegration_InsertAndReadRowProduct(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	productRepo := store.NewProductRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return productRepo.Insert(fdblayer.NewTx(ctx, tr, dir), &store.Product{Id: "p1", Name: "Widget", Category: "tools", Price: 42})
	})

	var product *store.Product
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		product, err = productRepo.ReadRow(fdblayer.NewTx(ctx, tr, dir), fdblayer.Key{"p1"})
		return err
	})
	if product.Name != "Widget" || product.Price != 42 {
		t.Fatalf("unexpected product: %+v", product)
	}
}

func TestIntegration_ReadRowUser_NotFound(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return userRepo.ReadRow(fdblayer.NewTx(ctx, tr, dir), fdblayer.Key{"nonexistent"})
	})
	if err == nil {
		t.Fatal("expected not found error")
	}
}

// Upsert (Update)
func TestIntegration_UpsertUser_UpdateFields(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Insert(fdblayer.NewTx(ctx, tr, dir), &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Upsert(fdblayer.NewTx(ctx, tr, dir), &store.User{Id: "u1", Name: "Bob", Email: "b@test.com"})
	})

	var user *store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		user, err = userRepo.ReadRow(fdblayer.NewTx(ctx, tr, dir), fdblayer.Key{"u1"})
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

	userRepo := store.NewUserRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Insert(fdblayer.NewTx(ctx, tr, dir), &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Delete(fdblayer.NewTx(ctx, tr, dir), fdblayer.Key{"u1"})
	})

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return userRepo.ReadRow(fdblayer.NewTx(ctx, tr, dir), fdblayer.Key{"u1"})
	})
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

// Secondary Index Lookups
func TestIntegration_ScanIndexUserByEmail(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(ctx, tr, dir)
		if err := userRepo.Insert(tx, &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"}); err != nil {
			return err
		}
		if err := userRepo.Insert(tx, &store.User{Id: "u2", Name: "Bob", Email: "bob@test.com"}); err != nil {
			return err
		}
		return userRepo.Insert(tx, &store.User{Id: "u3", Name: "Charlie", Email: "alice@test.com"})
	})

	// Should find two users with email "alice@test.com"
	var users *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(fdblayer.NewTx(ctx, tr, dir), store.UserByEmail, fdblayer.Prefix("alice@test.com"), fdblayer.ScanOptions{})
		return err
	})
	if len(users.Items) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users.Items))
	}

	// Should find one user with email "bob@test.com"
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(fdblayer.NewTx(ctx, tr, dir), store.UserByEmail, fdblayer.Prefix("bob@test.com"), fdblayer.ScanOptions{})
		return err
	})
	if len(users.Items) != 1 || users.Items[0].Name != "Bob" {
		t.Fatalf("unexpected result: %+v", users.Items)
	}

	// Should find zero for nonexistent email
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(fdblayer.NewTx(ctx, tr, dir), store.UserByEmail, fdblayer.Prefix("nobody@test.com"), fdblayer.ScanOptions{})
		return err
	})
	if len(users.Items) != 0 {
		t.Fatalf("expected 0, got %d", len(users.Items))
	}
}

func TestIntegration_ScanIndexProductByCategory(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	productRepo := store.NewProductRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(ctx, tr, dir)
		if err := productRepo.Insert(tx, &store.Product{Id: "p1", Name: "Hammer", Category: "tools", Price: 15}); err != nil {
			return err
		}
		if err := productRepo.Insert(tx, &store.Product{Id: "p2", Name: "Drill", Category: "tools", Price: 80}); err != nil {
			return err
		}
		return productRepo.Insert(tx, &store.Product{Id: "p3", Name: "Laptop", Category: "electronics", Price: 999})
	})

	var products *fdblayer.Page[*store.Product]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		products, err = productRepo.ScanIndex(fdblayer.NewTx(ctx, tr, dir), store.ProductByCategory, fdblayer.Prefix("tools"), fdblayer.ScanOptions{})
		return err
	})
	if len(products.Items) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(products.Items))
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		products, err = productRepo.ScanIndex(fdblayer.NewTx(ctx, tr, dir), store.ProductByCategory, fdblayer.Prefix("electronics"), fdblayer.ScanOptions{})
		return err
	})
	if len(products.Items) != 1 || products.Items[0].Name != "Laptop" {
		t.Fatalf("unexpected: %+v", products.Items)
	}
}

func TestIntegration_UpsertUser_IndexUpdated(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Insert(fdblayer.NewTx(ctx, tr, dir), &store.User{Id: "u1", Name: "Alice", Email: "old@test.com"})
	})

	// Update email
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Upsert(fdblayer.NewTx(ctx, tr, dir), &store.User{Id: "u1", Name: "Alice", Email: "new@test.com"})
	})

	// Old email should return empty
	var users *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(fdblayer.NewTx(ctx, tr, dir), store.UserByEmail, fdblayer.Prefix("old@test.com"), fdblayer.ScanOptions{})
		return err
	})
	if len(users.Items) != 0 {
		t.Fatal("stale index: old email still returns results")
	}

	// New email should return the user
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(fdblayer.NewTx(ctx, tr, dir), store.UserByEmail, fdblayer.Prefix("new@test.com"), fdblayer.ScanOptions{})
		return err
	})
	if len(users.Items) != 1 || users.Items[0].Name != "Alice" {
		t.Fatalf("expected Alice at new email, got %+v", users.Items)
	}
}

func TestIntegration_DeleteUser_ClearsIndex(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Insert(fdblayer.NewTx(ctx, tr, dir), &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Delete(fdblayer.NewTx(ctx, tr, dir), fdblayer.Key{"u1"})
	})

	var users *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(fdblayer.NewTx(ctx, tr, dir), store.UserByEmail, fdblayer.Prefix("alice@test.com"), fdblayer.ScanOptions{})
		return err
	})
	if len(users.Items) != 0 {
		t.Fatal("index not cleared after delete")
	}
}

// Scan / Pagination
func TestIntegration_ScanUser_Basic(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(ctx, tr, dir)
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("u%d", i)
			if err := userRepo.Insert(tx, &store.User{
				Id: id, Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("u%d@test.com", i),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	var result *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.Scan(fdblayer.NewTx(ctx, tr, dir), fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 10})
		return err
	})
	if len(result.Items) != 5 {
		t.Fatalf("expected 5 users, got %d", len(result.Items))
	}
	if result.HasMore {
		t.Fatal("should not have more pages")
	}
}

func TestIntegration_ScanUser_Pagination(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(ctx, tr, dir)
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("u%d", i)
			if err := userRepo.Insert(tx, &store.User{
				Id: id, Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("u%d@test.com", i),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// First page: limit 2
	var page1 *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page1, err = userRepo.Scan(fdblayer.NewTx(ctx, tr, dir), fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 2})
		return err
	})
	if len(page1.Items) != 2 {
		t.Fatalf("page1: expected 2 items, got %d", len(page1.Items))
	}
	if !page1.HasMore {
		t.Fatal("page1: expected HasMore=true")
	}
	if page1.NextCursor == nil {
		t.Fatal("page1: NextCursor should not be nil")
	}

	// Second page: use NextCursor
	var page2 *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page2, err = userRepo.Scan(fdblayer.NewTx(ctx, tr, dir), fdblayer.AllKeys(), fdblayer.ScanOptions{Cursor: page1.NextCursor, Limit: 2})
		return err
	})
	if len(page2.Items) != 2 {
		t.Fatalf("page2: expected 2 items, got %d", len(page2.Items))
	}
	if !page2.HasMore {
		t.Fatal("page2: expected HasMore=true")
	}

	// Third page: should have 1 remaining
	var page3 *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page3, err = userRepo.Scan(fdblayer.NewTx(ctx, tr, dir), fdblayer.AllKeys(), fdblayer.ScanOptions{Cursor: page2.NextCursor, Limit: 2})
		return err
	})
	if len(page3.Items) != 1 {
		t.Fatalf("page3: expected 1 item, got %d", len(page3.Items))
	}
	if page3.HasMore {
		t.Fatal("page3: expected HasMore=false")
	}
}

func TestIntegration_ScanProduct_Pagination(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	productRepo := store.NewProductRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(ctx, tr, dir)
		for i := 0; i < 7; i++ {
			if err := productRepo.Insert(tx, &store.Product{
				Id: fmt.Sprintf("p%d", i), Name: fmt.Sprintf("Product%d", i),
				Category: "cat", Price: int32(i * 10),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// Page through with limit 3
	var page1 *fdblayer.Page[*store.Product]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page1, err = productRepo.Scan(fdblayer.NewTx(ctx, tr, dir), fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 3})
		return err
	})
	if len(page1.Items) != 3 {
		t.Fatalf("expected 3, got %d", len(page1.Items))
	}
	if !page1.HasMore {
		t.Fatal("expected more pages")
	}

	var page2 *fdblayer.Page[*store.Product]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page2, err = productRepo.Scan(fdblayer.NewTx(ctx, tr, dir), fdblayer.AllKeys(), fdblayer.ScanOptions{Cursor: page1.NextCursor, Limit: 3})
		return err
	})
	if len(page2.Items) != 3 {
		t.Fatalf("expected 3, got %d", len(page2.Items))
	}

	var page3 *fdblayer.Page[*store.Product]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page3, err = productRepo.Scan(fdblayer.NewTx(ctx, tr, dir), fdblayer.AllKeys(), fdblayer.ScanOptions{Cursor: page2.NextCursor, Limit: 3})
		return err
	})
	if len(page3.Items) != 1 {
		t.Fatalf("expected 1, got %d", len(page3.Items))
	}
	if page3.HasMore {
		t.Fatal("should not have more")
	}
}

func TestIntegration_ScanUser_Empty(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	var result *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.Scan(fdblayer.NewTx(ctx, tr, dir), fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 10})
		return err
	})
	if len(result.Items) != 0 {
		t.Fatalf("expected empty list, got %d", len(result.Items))
	}
}

// ReadRows
func TestIntegration_ReadRowsUser(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository()

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(ctx, tr, dir)
		for _, id := range []string{"u1", "u2", "u3"} {
			if err := userRepo.Insert(tx, &store.User{Id: id, Name: "Name" + id, Email: id + "@test.com"}); err != nil {
				return err
			}
		}
		return nil
	})

	var result []*store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.ReadRows(fdblayer.NewTx(ctx, tr, dir), []fdblayer.Key{{"u1"}, {"u2"}, {"u3"}})
		return err
	})
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
}

func TestIntegration_RepositoryInterface(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	var repo fdblayer.Repository[*store.User] = store.NewUserRepository()

	// 1. Insert via Repository using real FDB transaction
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return repo.Insert(fdblayer.NewTx(ctx, tr, dir), &store.User{Id: "g1", Name: "Generic Alice", Email: "g-alice@test.com"})
	})

	// 2. ReadRow via Repository
	var got *store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = repo.ReadRow(fdblayer.NewTx(ctx, tr, dir), fdblayer.Key{"g1"})
		return err
	})
	if got == nil || got.Name != "Generic Alice" {
		t.Fatalf("unexpected user: %+v", got)
	}

	// 3. Upsert via Repository
	got.Name = "Updated Generic Alice"
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return repo.Upsert(fdblayer.NewTx(ctx, tr, dir), got)
	})

	// Verify update
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = repo.ReadRow(fdblayer.NewTx(ctx, tr, dir), fdblayer.Key{"g1"})
		return err
	})
	if got == nil || got.Name != "Updated Generic Alice" {
		t.Fatalf("update not applied: %+v", got)
	}

	// 4. Delete via Repository
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return repo.Delete(fdblayer.NewTx(ctx, tr, dir), fdblayer.Key{"g1"})
	})

	// Verify deletion
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return repo.ReadRow(fdblayer.NewTx(ctx, tr, dir), fdblayer.Key{"g1"})
	})
	if !errors.Is(err, fdblayer.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestIntegration_PaginatedIndexQuery(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	productRepo := store.NewProductRepository()
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(ctx, tr, dir)
		for i := 1; i <= 4; i++ {
			if err := productRepo.Insert(tx, &store.Product{
				Id:       fmt.Sprintf("p%d", i),
				Name:     fmt.Sprintf("Item %d", i),
				Category: "hardware",
				Price:    int32(i * 10),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// Reverse pagination on secondary index
	var revPage1, revPage2 *fdblayer.Page[*store.Product]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		tx := fdblayer.NewTx(ctx, tr, dir)
		var err error
		revPage1, err = productRepo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("hardware"), fdblayer.ScanOptions{
			Limit:   2,
			Reverse: true,
		})
		if err != nil {
			return err
		}
		revPage2, err = productRepo.ScanIndex(tx, store.ProductByCategory, fdblayer.Prefix("hardware"), fdblayer.ScanOptions{
			Cursor:  revPage1.NextCursor,
			Limit:   2,
			Reverse: true,
		})
		return err
	})
	if len(revPage1.Items) != 2 || !revPage1.HasMore || revPage1.Items[0].Id != "p4" || revPage1.Items[1].Id != "p3" {
		t.Fatalf("unexpected reverse index page 1: %+v", revPage1)
	}
	if len(revPage2.Items) != 2 || revPage2.HasMore || revPage2.Items[0].Id != "p2" || revPage2.Items[1].Id != "p1" {
		t.Fatalf("unexpected reverse index page 2: %+v", revPage2)
	}
}
