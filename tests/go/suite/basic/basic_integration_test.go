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

	userRepo := store.NewUserRepository(dir)

	if _, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		return nil, userRepo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"})
	}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	raw, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		return userRepo.ReadRow(ctx, tr, fdblayer.Key{"u1"})
	})
	if err != nil {
		t.Fatalf("ReadRow failed: %v", err)
	}
	user := raw.(*store.User)
	if user.Name != "Alice" || user.Email != "alice@test.com" {
		t.Fatalf("unexpected user: %+v", user)
	}
}

func TestIntegration_InsertUser_AlreadyExists(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	userRepo := store.NewUserRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"})
	})

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return nil, userRepo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Bob", Email: "bob@test.com"})
	})
	if !errors.Is(err, fdblayer.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists on duplicate Insert, got: %v", err)
	}

	var byBob []*store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		res, err := userRepo.ScanIndex(ctx, tr, store.UserByEmail, fdblayer.Prefix("bob@test.com"))
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

	productRepo := store.NewProductRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return productRepo.Insert(ctx, tr, &store.Product{Id: "p1", Name: "Widget", Category: "tools", Price: 42})
	})

	var product *store.Product
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		product, err = productRepo.ReadRow(ctx, tr, fdblayer.Key{"p1"})
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

	userRepo := store.NewUserRepository(dir)

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return userRepo.ReadRow(ctx, tr, fdblayer.Key{"nonexistent"})
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

	userRepo := store.NewUserRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Upsert(ctx, tr, &store.User{Id: "u1", Name: "Bob", Email: "b@test.com"})
	})

	var user *store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		user, err = userRepo.ReadRow(ctx, tr, fdblayer.Key{"u1"})
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

	userRepo := store.NewUserRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Delete(ctx, tr, fdblayer.Key{"u1"})
	})

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return userRepo.ReadRow(ctx, tr, fdblayer.Key{"u1"})
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

	userRepo := store.NewUserRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := userRepo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"}); err != nil {
			return err
		}
		if err := userRepo.Insert(ctx, tr, &store.User{Id: "u2", Name: "Bob", Email: "bob@test.com"}); err != nil {
			return err
		}
		return userRepo.Insert(ctx, tr, &store.User{Id: "u3", Name: "Charlie", Email: "alice@test.com"})
	})

	// Should find two users with email "alice@test.com"
	var users *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(ctx, tr, store.UserByEmail, fdblayer.Prefix("alice@test.com"))
		return err
	})
	if len(users.Items) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users.Items))
	}

	// Should find one user with email "bob@test.com"
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(ctx, tr, store.UserByEmail, fdblayer.Prefix("bob@test.com"))
		return err
	})
	if len(users.Items) != 1 || users.Items[0].Name != "Bob" {
		t.Fatalf("unexpected result: %+v", users.Items)
	}

	// Should find zero for nonexistent email
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(ctx, tr, store.UserByEmail, fdblayer.Prefix("nobody@test.com"))
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

	productRepo := store.NewProductRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := productRepo.Insert(ctx, tr, &store.Product{Id: "p1", Name: "Hammer", Category: "tools", Price: 15}); err != nil {
			return err
		}
		if err := productRepo.Insert(ctx, tr, &store.Product{Id: "p2", Name: "Drill", Category: "tools", Price: 80}); err != nil {
			return err
		}
		return productRepo.Insert(ctx, tr, &store.Product{Id: "p3", Name: "Laptop", Category: "electronics", Price: 999})
	})

	var products *fdblayer.Page[*store.Product]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		products, err = productRepo.ScanIndex(ctx, tr, store.ProductByCategory, fdblayer.Prefix("tools"))
		return err
	})
	if len(products.Items) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(products.Items))
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		products, err = productRepo.ScanIndex(ctx, tr, store.ProductByCategory, fdblayer.Prefix("electronics"))
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

	userRepo := store.NewUserRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "old@test.com"})
	})

	// Update email
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Upsert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "new@test.com"})
	})

	// Old email should return empty
	var users *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(ctx, tr, store.UserByEmail, fdblayer.Prefix("old@test.com"))
		return err
	})
	if len(users.Items) != 0 {
		t.Fatal("stale index: old email still returns results")
	}

	// New email should return the user
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(ctx, tr, store.UserByEmail, fdblayer.Prefix("new@test.com"))
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

	userRepo := store.NewUserRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Insert(ctx, tr, &store.User{Id: "u1", Name: "Alice", Email: "alice@test.com"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Delete(ctx, tr, fdblayer.Key{"u1"})
	})

	var users *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.ScanIndex(ctx, tr, store.UserByEmail, fdblayer.Prefix("alice@test.com"))
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

	userRepo := store.NewUserRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("u%d", i)
			if err := userRepo.Insert(ctx, tr, &store.User{
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
		result, err = userRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithLimit(10))
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

	userRepo := store.NewUserRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("u%d", i)
			if err := userRepo.Insert(ctx, tr, &store.User{
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
		page1, err = userRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithLimit(2))
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
		page2, err = userRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithCursor(page1.NextCursor), fdblayer.WithLimit(2))
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
		page3, err = userRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithCursor(page2.NextCursor), fdblayer.WithLimit(2))
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

	productRepo := store.NewProductRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		for i := 0; i < 7; i++ {
			if err := productRepo.Insert(ctx, tr, &store.Product{
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
		page1, err = productRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithLimit(3))
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
		page2, err = productRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithCursor(page1.NextCursor), fdblayer.WithLimit(3))
		return err
	})
	if len(page2.Items) != 3 {
		t.Fatalf("expected 3, got %d", len(page2.Items))
	}

	var page3 *fdblayer.Page[*store.Product]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page3, err = productRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithCursor(page2.NextCursor), fdblayer.WithLimit(3))
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

	userRepo := store.NewUserRepository(dir)

	var result *fdblayer.Page[*store.User]
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithLimit(10))
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

	userRepo := store.NewUserRepository(dir)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		for _, id := range []string{"u1", "u2", "u3"} {
			if err := userRepo.Insert(ctx, tr, &store.User{Id: id, Name: "Name" + id, Email: id + "@test.com"}); err != nil {
				return err
			}
		}
		return nil
	})

	var result []*store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.ReadRows(ctx, tr, []fdblayer.Key{{"u1"}, {"u2"}, {"u3"}})
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

	var repo fdblayer.Repository[*store.User] = store.NewUserRepository(dir)

	// 1. Insert via Repository using real FDB transaction
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return repo.Insert(ctx, tr, &store.User{Id: "g1", Name: "Generic Alice", Email: "g-alice@test.com"})
	})

	// 2. ReadRow via Repository
	var got *store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = repo.ReadRow(ctx, tr, fdblayer.Key{"g1"})
		return err
	})
	if got == nil || got.Name != "Generic Alice" {
		t.Fatalf("unexpected user: %+v", got)
	}

	// 3. Upsert via Repository
	got.Name = "Updated Generic Alice"
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return repo.Upsert(ctx, tr, got)
	})

	// Verify update
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = repo.ReadRow(ctx, tr, fdblayer.Key{"g1"})
		return err
	})
	if got == nil || got.Name != "Updated Generic Alice" {
		t.Fatalf("update not applied: %+v", got)
	}

	// 4. Delete via Repository
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return repo.Delete(ctx, tr, fdblayer.Key{"g1"})
	})

	// Verify deletion
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return repo.ReadRow(ctx, tr, fdblayer.Key{"g1"})
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

	productRepo := store.NewProductRepository(dir)
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		for i := 1; i <= 4; i++ {
			if err := productRepo.Insert(ctx, tr, &store.Product{
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
		var err error
		revPage1, err = productRepo.ScanIndex(
			ctx,
			tr,
			store.ProductByCategory,
			fdblayer.Prefix("hardware"),
			fdblayer.WithLimit(2),
			fdblayer.WithReverse(),
		)
		if err != nil {
			return err
		}
		revPage2, err = productRepo.ScanIndex(
			ctx,
			tr,
			store.ProductByCategory,
			fdblayer.Prefix("hardware"),
			fdblayer.WithCursor(revPage1.NextCursor),
			fdblayer.WithLimit(2),
			fdblayer.WithReverse(),
		)
		return err
	})
	if len(revPage1.Items) != 2 || !revPage1.HasMore || revPage1.Items[0].Id != "p4" || revPage1.Items[1].Id != "p3" {
		t.Fatalf("unexpected reverse index page 1: %+v", revPage1)
	}
	if len(revPage2.Items) != 2 || revPage2.HasMore || revPage2.Items[0].Id != "p2" || revPage2.Items[1].Id != "p1" {
		t.Fatalf("unexpected reverse index page 2: %+v", revPage2)
	}
}

func TestIntegration_AggregateSecondaryIndexes_Product(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	repo := store.NewProductRepository(dir)

	// 1. Empty group returns (0, nil) for COUNT/SUM and ErrNotFound for MIN/MAX
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if cnt, err := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{"books"}); err != nil || cnt != 0 {
			t.Fatalf("expected COUNT=0 for empty group, got %d, err=%v", cnt, err)
		}
		if sum, err := repo.ReadAggregate(ctx, tr, store.ProductSumPriceByCategory, fdblayer.Key{"books"}); err != nil || sum != 0 {
			t.Fatalf("expected SUM=0 for empty group, got %d, err=%v", sum, err)
		}
		if _, err := repo.ReadAggregate(ctx, tr, store.ProductMinPriceByCategory, fdblayer.Key{"books"}); !errors.Is(err, fdblayer.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for MIN on empty group, got %v", err)
		}
		if _, err := repo.ReadAggregate(ctx, tr, store.ProductMaxPriceByCategory, fdblayer.Key{"books"}); !errors.Is(err, fdblayer.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for MAX on empty group, got %v", err)
		}
		return nil
	})

	// 2. Insert records with negative, zero, and positive prices
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		for _, p := range []*store.Product{
			{Id: "b1", Name: "Coupon", Category: "books", Price: -20},
			{Id: "b2", Name: "Pamphlet", Category: "books", Price: 0},
			{Id: "b3", Name: "Textbook", Category: "books", Price: 50},
			{Id: "g1", Name: "Chess", Category: "games", Price: 35},
		} {
			if err := repo.Insert(ctx, tr, p); err != nil {
				return err
			}
		}
		return nil
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		cnt, err := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{"books"})
		if err != nil || cnt != 3 {
			t.Fatalf("expected books COUNT=3, got %d, err=%v", cnt, err)
		}
		totalCnt, err := repo.ReadAggregate(ctx, tr, store.ProductCount, fdblayer.Key{})
		if err != nil || totalCnt != 4 {
			t.Fatalf("expected total ProductCount=4, got %d, err=%v", totalCnt, err)
		}
		sum, err := repo.ReadAggregate(ctx, tr, store.ProductSumPriceByCategory, fdblayer.Key{"books"})
		if err != nil || sum != 30 {
			t.Fatalf("expected books SUM=30 (-20 + 0 + 50), got %d, err=%v", sum, err)
		}
		minVal, err := repo.ReadAggregate(ctx, tr, store.ProductMinPriceByCategory, fdblayer.Key{"books"})
		if err != nil || minVal != -20 {
			t.Fatalf("expected books MIN=-20, got %d, err=%v", minVal, err)
		}
		maxVal, err := repo.ReadAggregate(ctx, tr, store.ProductMaxPriceByCategory, fdblayer.Key{"books"})
		if err != nil || maxVal != 50 {
			t.Fatalf("expected books MAX=50, got %d, err=%v", maxVal, err)
		}
		return nil
	})

	// 3. Upsert b3: move from "books" to "games" with Price 80, then Delete b1
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := repo.Upsert(ctx, tr, &store.Product{Id: "b3", Name: "Textbook", Category: "games", Price: 80}); err != nil {
			return err
		}
		return repo.Delete(ctx, tr, fdblayer.Key{"b1"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if cnt, err := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{"books"}); err != nil || cnt != 1 {
			t.Fatalf("expected books COUNT=1 after move+delete, got %d, err=%v", cnt, err)
		}
		if sum, err := repo.ReadAggregate(ctx, tr, store.ProductSumPriceByCategory, fdblayer.Key{"books"}); err != nil || sum != 0 {
			t.Fatalf("expected books SUM=0 after move+delete, got %d, err=%v", sum, err)
		}
		if cnt, err := repo.ReadAggregate(ctx, tr, store.ProductCountByCategory, fdblayer.Key{"games"}); err != nil || cnt != 2 {
			t.Fatalf("expected games COUNT=2 after move, got %d, err=%v", cnt, err)
		}
		if sum, err := repo.ReadAggregate(ctx, tr, store.ProductSumPriceByCategory, fdblayer.Key{"games"}); err != nil || sum != 115 {
			t.Fatalf("expected games SUM=115 (35 + 80), got %d, err=%v", sum, err)
		}
		if maxVal, err := repo.ReadAggregate(ctx, tr, store.ProductMaxPriceByCategory, fdblayer.Key{"games"}); err != nil || maxVal != 80 {
			t.Fatalf("expected games MAX=80, got %d, err=%v", maxVal, err)
		}
		if totalCnt, err := repo.ReadAggregate(ctx, tr, store.ProductCount, fdblayer.Key{}); err != nil || totalCnt != 3 {
			t.Fatalf("expected total ProductCount=3, got %d, err=%v", totalCnt, err)
		}
		return nil
	})
}

func TestIntegration_MutationSecondaryIndexes_Post(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	postRepo := store.NewPostRepository(dir)

	// 1. Mutate without inserting the entity first (0-RTT blind atomic mutations)
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := postRepo.Mutate(ctx, tr, store.PostCountById, fdblayer.Key{"p1"}, 1); err != nil {
			return err
		}
		if err := postRepo.Mutate(ctx, tr, store.PostCountById, fdblayer.Key{"p1"}, 3); err != nil {
			return err
		}
		if err := postRepo.Mutate(ctx, tr, store.PostAddLikesById, fdblayer.Key{"p1"}, 15); err != nil {
			return err
		}
		if err := postRepo.Mutate(ctx, tr, store.PostAddLikesById, fdblayer.Key{"p1"}, -4); err != nil {
			return err
		}
		for _, lat := range []int64{30, -5, 95, 12} {
			if err := postRepo.Mutate(ctx, tr, store.PostMinLatencyById, fdblayer.Key{"p1"}, lat); err != nil {
				return err
			}
			if err := postRepo.Mutate(ctx, tr, store.PostMaxLatencyById, fdblayer.Key{"p1"}, lat); err != nil {
				return err
			}
		}
		return postRepo.Mutate(ctx, tr, store.PostCountGlobalViews, fdblayer.Key{}, 7)
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if v, err := postRepo.ReadAggregate(ctx, tr, store.PostCountById, fdblayer.Key{"p1"}); err != nil || v != 4 {
			t.Fatalf("expected PostCountById('p1')=4, got %d, err=%v", v, err)
		}
		if v, err := postRepo.ReadAggregate(ctx, tr, store.PostAddLikesById, fdblayer.Key{"p1"}); err != nil || v != 11 {
			t.Fatalf("expected PostAddLikesById('p1')=11, got %d, err=%v", v, err)
		}
		if v, err := postRepo.ReadAggregate(ctx, tr, store.PostMinLatencyById, fdblayer.Key{"p1"}); err != nil || v != -5 {
			t.Fatalf("expected PostMinLatencyById('p1')=-5, got %d, err=%v", v, err)
		}
		if v, err := postRepo.ReadAggregate(ctx, tr, store.PostMaxLatencyById, fdblayer.Key{"p1"}); err != nil || v != 95 {
			t.Fatalf("expected PostMaxLatencyById('p1')=95, got %d, err=%v", v, err)
		}
		if v, err := postRepo.ReadAggregate(ctx, tr, store.PostCountGlobalViews, fdblayer.Key{}); err != nil || v != 7 {
			t.Fatalf("expected PostCountGlobalViews=7, got %d, err=%v", v, err)
		}
		return nil
	})

	// 2. Delete("p1") clears primary-key-scoped mutation indexes and leaves global counter intact
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return postRepo.Delete(ctx, tr, fdblayer.Key{"p1"})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if v, err := postRepo.ReadAggregate(ctx, tr, store.PostCountById, fdblayer.Key{"p1"}); err != nil || v != 0 {
			t.Fatalf("expected PostCountById('p1')=0 after Delete, got %d, err=%v", v, err)
		}
		if _, err := postRepo.ReadAggregate(ctx, tr, store.PostMaxLatencyById, fdblayer.Key{"p1"}); !errors.Is(err, fdblayer.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for PostMaxLatencyById('p1') after Delete, got %v", err)
		}
		if v, err := postRepo.ReadAggregate(ctx, tr, store.PostCountGlobalViews, fdblayer.Key{}); err != nil || v != 7 {
			t.Fatalf("expected PostCountGlobalViews=7 after Delete('p1'), got %d, err=%v", v, err)
		}
		return nil
	})
}
