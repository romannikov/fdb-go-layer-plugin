//go:build integration

package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func init() {
	fdb.MustAPIVersion(710)
}

// testDir creates a unique directory subspace for test isolation and returns
// a cleanup function that removes it.
func testDir(t *testing.T, db fdb.Database) (directory.DirectorySubspace, func()) {
	t.Helper()
	path := []string{"test", fmt.Sprintf("%s_%d", t.Name(), time.Now().UnixNano())}
	dir, err := directory.CreateOrOpen(db, path, nil)
	if err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	cleanup := func() {
		_, _ = directory.Root().Remove(db, path)
	}
	return dir, cleanup
}

// withTx runs a read-write transaction and fails the test on error.
func withTx(t *testing.T, db fdb.Database, fn func(tr fdb.Transaction) error) {
	t.Helper()
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return nil, fn(tr)
	})
	if err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}

// Metadata
func TestIntegration_SyncMetadata(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	withTx(t, db, func(tr fdb.Transaction) error {
		return store.SyncMetadata(tr, dir)
	})

	meta := store.Metadata()
	if meta["User"] == 0 || meta["Product"] == 0 {
		t.Fatalf("metadata not populated: %v", meta)
	}
	if meta["User"] == meta["Product"] {
		t.Fatal("User and Product got the same type ID")
	}
}

func TestIntegration_SyncMetadata_Idempotent(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	withTx(t, db, func(tr fdb.Transaction) error {
		return store.SyncMetadata(tr, dir)
	})
	first := store.Metadata()

	// Second sync should produce identical IDs
	withTx(t, db, func(tr fdb.Transaction) error {
		return store.SyncMetadata(tr, dir)
	})
	second := store.Metadata()

	if first["User"] != second["User"] || first["Product"] != second["Product"] {
		t.Fatalf("IDs changed: %v → %v", first, second)
	}
}

// CRUD Round-Trip
func TestIntegration_CreateAndGetUser(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		return userRepo.Create(tr, dir, &User{Id: "u1", Name: "Alice", Email: "alice@test.com"})
	})

	var user *User
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		user, err = userRepo.Get(tr, dir, tuple.Tuple{"u1"})
		return err
	})
	if user.Name != "Alice" || user.Email != "alice@test.com" {
		t.Fatalf("unexpected user: %+v", user)
	}
}

func TestIntegration_CreateAndGetProduct(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	productRepo := NewProductRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		return productRepo.Create(tr, dir, &Product{Id: "p1", Name: "Widget", Category: "tools", Price: 42})
	})

	var product *Product
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		product, err = productRepo.Get(tr, dir, tuple.Tuple{"p1"})
		return err
	})
	if product.Name != "Widget" || product.Price != 42 {
		t.Fatalf("unexpected product: %+v", product)
	}
}

func TestIntegration_GetUser_NotFound(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		return store.SyncMetadata(tr, dir)
	})

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return userRepo.Get(tr, dir, tuple.Tuple{"nonexistent"})
	})
	if err == nil {
		t.Fatal("expected not found error")
	}
}

// Set (Update)
func TestIntegration_SetUser_UpdateFields(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		return userRepo.Create(tr, dir, &User{Id: "u1", Name: "Alice", Email: "a@test.com"})
	})

	withTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Set(tr, dir, &User{Id: "u1", Name: "Bob", Email: "b@test.com"})
	})

	var user *User
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		user, err = userRepo.Get(tr, dir, tuple.Tuple{"u1"})
		return err
	})
	if user.Name != "Bob" || user.Email != "b@test.com" {
		t.Fatalf("update not applied: %+v", user)
	}
}

// Delete
func TestIntegration_DeleteUser(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		return userRepo.Create(tr, dir, &User{Id: "u1", Name: "Alice", Email: "a@test.com"})
	})

	withTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Delete(tr, dir, tuple.Tuple{"u1"})
	})

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return userRepo.Get(tr, dir, tuple.Tuple{"u1"})
	})
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

// Secondary Index Lookups
func TestIntegration_GetUserByEmail(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		if err := userRepo.Create(tr, dir, &User{Id: "u1", Name: "Alice", Email: "alice@test.com"}); err != nil {
			return err
		}
		if err := userRepo.Create(tr, dir, &User{Id: "u2", Name: "Bob", Email: "bob@test.com"}); err != nil {
			return err
		}
		return userRepo.Create(tr, dir, &User{Id: "u3", Name: "Charlie", Email: "alice@test.com"})
	})

	// Should find two users with email "alice@test.com"
	var users []*User
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetByEmail(tr, dir, "alice@test.com")
		return err
	})
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	// Should find one user with email "bob@test.com"
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetByEmail(tr, dir, "bob@test.com")
		return err
	})
	if len(users) != 1 || users[0].Name != "Bob" {
		t.Fatalf("unexpected result: %+v", users)
	}

	// Should find zero for nonexistent email
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetByEmail(tr, dir, "nobody@test.com")
		return err
	})
	if len(users) != 0 {
		t.Fatalf("expected 0, got %d", len(users))
	}
}

func TestIntegration_GetProductByCategory(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	productRepo := NewProductRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		if err := productRepo.Create(tr, dir, &Product{Id: "p1", Name: "Hammer", Category: "tools", Price: 15}); err != nil {
			return err
		}
		if err := productRepo.Create(tr, dir, &Product{Id: "p2", Name: "Drill", Category: "tools", Price: 80}); err != nil {
			return err
		}
		return productRepo.Create(tr, dir, &Product{Id: "p3", Name: "Laptop", Category: "electronics", Price: 999})
	})

	var products []*Product
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		products, err = productRepo.GetByCategory(tr, dir, "tools")
		return err
	})
	if len(products) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(products))
	}

	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		products, err = productRepo.GetByCategory(tr, dir, "electronics")
		return err
	})
	if len(products) != 1 || products[0].Name != "Laptop" {
		t.Fatalf("unexpected: %+v", products)
	}
}

func TestIntegration_SetUser_IndexUpdated(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		return userRepo.Create(tr, dir, &User{Id: "u1", Name: "Alice", Email: "old@test.com"})
	})

	// Update email
	withTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Set(tr, dir, &User{Id: "u1", Name: "Alice", Email: "new@test.com"})
	})

	// Old email should return empty
	var users []*User
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetByEmail(tr, dir, "old@test.com")
		return err
	})
	if len(users) != 0 {
		t.Fatal("stale index: old email still returns results")
	}

	// New email should return the user
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetByEmail(tr, dir, "new@test.com")
		return err
	})
	if len(users) != 1 || users[0].Name != "Alice" {
		t.Fatalf("expected Alice at new email, got %+v", users)
	}
}

func TestIntegration_DeleteUser_ClearsIndex(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		return userRepo.Create(tr, dir, &User{Id: "u1", Name: "Alice", Email: "alice@test.com"})
	})

	withTx(t, db, func(tr fdb.Transaction) error {
		return userRepo.Delete(tr, dir, tuple.Tuple{"u1"})
	})

	var users []*User
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetByEmail(tr, dir, "alice@test.com")
		return err
	})
	if len(users) != 0 {
		t.Fatal("index not cleared after delete")
	}
}

// List / Pagination
func TestIntegration_ListUser_Basic(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("u%d", i)
			if err := userRepo.Create(tr, dir, &User{
				Id: id, Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("u%d@test.com", i),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	var result *PaginatedResult[User]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.List(tr, dir, PaginationOptions{Limit: 10})
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
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("u%d", i)
			if err := userRepo.Create(tr, dir, &User{
				Id: id, Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("u%d@test.com", i),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// First page: limit 2
	var page1 *PaginatedResult[User]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page1, err = userRepo.List(tr, dir, PaginationOptions{Limit: 2})
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
	var page2 *PaginatedResult[User]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page2, err = userRepo.List(tr, dir, PaginationOptions{Begin: page1.NextKey, Limit: 2})
		return err
	})
	if len(page2.Items) != 2 {
		t.Fatalf("page2: expected 2 items, got %d", len(page2.Items))
	}
	if !page2.HasMore {
		t.Fatal("page2: expected HasMore=true")
	}

	// Third page: should have 1 remaining
	var page3 *PaginatedResult[User]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page3, err = userRepo.List(tr, dir, PaginationOptions{Begin: page2.NextKey, Limit: 2})
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
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	productRepo := NewProductRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		for i := 0; i < 7; i++ {
			if err := productRepo.Create(tr, dir, &Product{
				Id: fmt.Sprintf("p%d", i), Name: fmt.Sprintf("Product%d", i),
				Category: "cat", Price: int32(i * 10),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// Page through with limit 3
	var page1 *PaginatedResult[Product]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page1, err = productRepo.List(tr, dir, PaginationOptions{Limit: 3})
		return err
	})
	if len(page1.Items) != 3 {
		t.Fatalf("expected 3, got %d", len(page1.Items))
	}
	if !page1.HasMore {
		t.Fatal("expected more pages")
	}

	var page2 *PaginatedResult[Product]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page2, err = productRepo.List(tr, dir, PaginationOptions{Begin: page1.NextKey, Limit: 3})
		return err
	})
	if len(page2.Items) != 3 {
		t.Fatalf("expected 3, got %d", len(page2.Items))
	}

	var page3 *PaginatedResult[Product]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page3, err = productRepo.List(tr, dir, PaginationOptions{Begin: page2.NextKey, Limit: 3})
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
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		return store.SyncMetadata(tr, dir)
	})

	var result *PaginatedResult[User]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.List(tr, dir, PaginationOptions{Limit: 10})
		return err
	})
	if len(result.Items) != 0 {
		t.Fatalf("expected empty list, got %d", len(result.Items))
	}
}

// BatchGet
func TestIntegration_BatchGetUser(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		for _, id := range []string{"u1", "u2", "u3"} {
			if err := userRepo.Create(tr, dir, &User{Id: id, Name: "Name" + id, Email: id + "@test.com"}); err != nil {
				return err
			}
		}
		return nil
	})

	var result map[string]*User
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = userRepo.BatchGet(tr, dir, []tuple.Tuple{{"u1"}, {"u2"}, {"u3"}})
		return err
	})
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
}

// Fan-Out Index (Post / Tags)
func TestIntegration_CreateAndGetPost(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	postRepo := NewPostRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		return postRepo.Create(tr, dir, &Post{Id: "p1", Tags: []string{"go", "fdb", "testing"}})
	})

	var post *Post
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		post, err = postRepo.Get(tr, dir, tuple.Tuple{"p1"})
		return err
	})
	if post.Id != "p1" {
		t.Fatalf("unexpected post id: %s", post.Id)
	}
	if len(post.Tags) != 3 {
		t.Fatalf("expected 3 tags, got %d", len(post.Tags))
	}
}

func TestIntegration_GetPostByTags_FanOut(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	postRepo := NewPostRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		if err := postRepo.Create(tr, dir, &Post{Id: "p1", Tags: []string{"go", "fdb"}}); err != nil {
			return err
		}
		if err := postRepo.Create(tr, dir, &Post{Id: "p2", Tags: []string{"go", "rust"}}); err != nil {
			return err
		}
		return postRepo.Create(tr, dir, &Post{Id: "p3", Tags: []string{"rust", "wasm"}})
	})

	// "go" should match p1 and p2
	var posts []*Post
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetByTags(tr, dir, "go")
		return err
	})
	if len(posts) != 2 {
		t.Fatalf("expected 2 posts tagged 'go', got %d", len(posts))
	}

	// "rust" should match p2 and p3
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetByTags(tr, dir, "rust")
		return err
	})
	if len(posts) != 2 {
		t.Fatalf("expected 2 posts tagged 'rust', got %d", len(posts))
	}

	// "fdb" should match only p1
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetByTags(tr, dir, "fdb")
		return err
	})
	if len(posts) != 1 || posts[0].Id != "p1" {
		t.Fatalf("expected p1 for 'fdb', got %+v", posts)
	}

	// "wasm" should match only p3
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetByTags(tr, dir, "wasm")
		return err
	})
	if len(posts) != 1 || posts[0].Id != "p3" {
		t.Fatalf("expected p3 for 'wasm', got %+v", posts)
	}

	// Nonexistent tag
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetByTags(tr, dir, "python")
		return err
	})
	if len(posts) != 0 {
		t.Fatalf("expected 0 posts for 'python', got %d", len(posts))
	}
}

func TestIntegration_SetPost_FanOutIndexUpdated(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	postRepo := NewPostRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		return postRepo.Create(tr, dir, &Post{Id: "p1", Tags: []string{"alpha", "beta"}})
	})

	// Update tags: remove "alpha", keep "beta", add "gamma"
	withTx(t, db, func(tr fdb.Transaction) error {
		return postRepo.Set(tr, dir, &Post{Id: "p1", Tags: []string{"beta", "gamma"}})
	})

	// "alpha" should no longer return any results (stale index cleared)
	var posts []*Post
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetByTags(tr, dir, "alpha")
		return err
	})
	if len(posts) != 0 {
		t.Fatal("stale fan-out index: 'alpha' still returns results after update")
	}

	// "beta" should still find the post
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetByTags(tr, dir, "beta")
		return err
	})
	if len(posts) != 1 || posts[0].Id != "p1" {
		t.Fatalf("expected p1 for 'beta', got %+v", posts)
	}

	// "gamma" should find the post (new index entry)
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetByTags(tr, dir, "gamma")
		return err
	})
	if len(posts) != 1 || posts[0].Id != "p1" {
		t.Fatalf("expected p1 for 'gamma', got %+v", posts)
	}
}

func TestIntegration_DeletePost_ClearsFanOutIndex(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	postRepo := NewPostRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		return postRepo.Create(tr, dir, &Post{Id: "p1", Tags: []string{"x", "y", "z"}})
	})

	withTx(t, db, func(tr fdb.Transaction) error {
		return postRepo.Delete(tr, dir, tuple.Tuple{"p1"})
	})

	// All tag index entries should be cleared
	for _, tag := range []string{"x", "y", "z"} {
		var posts []*Post
		withTx(t, db, func(tr fdb.Transaction) error {
			var err error
			posts, err = postRepo.GetByTags(tr, dir, tag)
			return err
		})
		if len(posts) != 0 {
			t.Fatalf("fan-out index not cleared for tag %q after delete", tag)
		}
	}

	// Get should fail
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return postRepo.Get(tr, dir, tuple.Tuple{"p1"})
	})
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestIntegration_ListPost_WithFanOutIndex(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	postRepo := NewPostRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		// Create posts with multiple tags each — generates many index entries
		for i := 0; i < 5; i++ {
			if err := postRepo.Create(tr, dir, &Post{
				Id:   fmt.Sprintf("p%d", i),
				Tags: []string{"common", fmt.Sprintf("unique%d", i)},
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// List should return all 5 posts, skipping index entries
	var result *PaginatedResult[Post]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = postRepo.List(tr, dir, PaginationOptions{Limit: 10})
		return err
	})
	if len(result.Items) != 5 {
		t.Fatalf("expected 5 posts, got %d", len(result.Items))
	}
	if result.HasMore {
		t.Fatal("should not have more pages")
	}
}

func TestIntegration_ListPost_Pagination(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	postRepo := NewPostRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			if err := postRepo.Create(tr, dir, &Post{
				Id:   fmt.Sprintf("p%d", i),
				Tags: []string{"t1", "t2", "t3"},
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// Page through with limit 2
	var page1 *PaginatedResult[Post]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page1, err = postRepo.List(tr, dir, PaginationOptions{Limit: 2})
		return err
	})
	if len(page1.Items) != 2 {
		t.Fatalf("page1: expected 2 items, got %d", len(page1.Items))
	}
	if !page1.HasMore {
		t.Fatal("page1: expected HasMore=true")
	}

	var page2 *PaginatedResult[Post]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page2, err = postRepo.List(tr, dir, PaginationOptions{Begin: page1.NextKey, Limit: 2})
		return err
	})
	if len(page2.Items) != 2 {
		t.Fatalf("page2: expected 2 items, got %d", len(page2.Items))
	}

	var page3 *PaginatedResult[Post]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page3, err = postRepo.List(tr, dir, PaginationOptions{Begin: page2.NextKey, Limit: 2})
		return err
	})
	if len(page3.Items) != 1 {
		t.Fatalf("page3: expected 1 item, got %d", len(page3.Items))
	}
	if page3.HasMore {
		t.Fatal("page3: expected HasMore=false")
	}
}

func TestIntegration_Post_EmptyTags(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	postRepo := NewPostRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		return postRepo.Create(tr, dir, &Post{Id: "p1", Tags: []string{}})
	})

	var post *Post
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		post, err = postRepo.Get(tr, dir, tuple.Tuple{"p1"})
		return err
	})
	if post.Id != "p1" {
		t.Fatalf("unexpected post: %+v", post)
	}
	if len(post.Tags) != 0 {
		t.Fatalf("expected empty tags, got %v", post.Tags)
	}
}

func TestIntegration_BatchGetPost(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	postRepo := NewPostRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		for _, id := range []string{"p1", "p2", "p3"} {
			if err := postRepo.Create(tr, dir, &Post{Id: id, Tags: []string{"tag"}}); err != nil {
				return err
			}
		}
		return nil
	})

	var result map[string]*Post
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = postRepo.BatchGet(tr, dir, []tuple.Tuple{{"p1"}, {"p2"}, {"p3"}})
		return err
	})
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
}

// ==========================================================================
// Cross-Type Isolation with Complex Indexes
// ==========================================================================

func TestIntegration_CrossTypeIsolation_ComplexIndexes(t *testing.T) {
	db := fdb.MustOpenDefault()
	dir, cleanup := testDir(t, db)
	defer cleanup()

	store := NewRecordStore()
	userRepo := NewUserRepository(store)
	productRepo := NewProductRepository(store)
	postRepo := NewPostRepository(store)
	withTx(t, db, func(tr fdb.Transaction) error {
		if err := store.SyncMetadata(tr, dir); err != nil {
			return err
		}
		// Create entities of all types simultaneously
		if err := userRepo.Create(tr, dir, &User{Id: "u1", Name: "Alice", Email: "a@test.com"}); err != nil {
			return err
		}
		if err := productRepo.Create(tr, dir, &Product{Id: "pr1", Name: "Widget", Category: "tools", Price: 10}); err != nil {
			return err
		}
		return postRepo.Create(tr, dir, &Post{Id: "po1", Tags: []string{"go"}})
	})

	// Each type's operations should be completely isolated
	var users []*User
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetByEmail(tr, dir, "a@test.com")
		return err
	})
	if len(users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(users))
	}

	var posts []*Post
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetByTags(tr, dir, "go")
		return err
	})
	if len(posts) != 1 {
		t.Fatalf("expected 1 post, got %d", len(posts))
	}

	// Verify listing each type returns exactly 1 entity
	var userResult *PaginatedResult[User]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		userResult, err = userRepo.List(tr, dir, PaginationOptions{Limit: 10})
		return err
	})
	if len(userResult.Items) != 1 {
		t.Fatalf("expected 1 user in list, got %d", len(userResult.Items))
	}

	var postResult *PaginatedResult[Post]
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		postResult, err = postRepo.List(tr, dir, PaginationOptions{Limit: 10})
		return err
	})
	if len(postResult.Items) != 1 {
		t.Fatalf("expected 1 post in list, got %d", len(postResult.Items))
	}
}
