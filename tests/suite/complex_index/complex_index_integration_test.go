//go:build integration

package complex_index_test

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

// Fan-Out Index (Post / Tags)
func TestIntegration_CreateAndGetPost(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	postRepo := store.NewPostRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		return postRepo.Create(ctx, tr, dir, &store.Post{Id: "p1", Tags: []string{"go", "fdb", "testing"}})
	})

	var post *store.Post
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		post, err = postRepo.Get(ctx, tr, dir, "p1")
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
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	postRepo := store.NewPostRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		if err := postRepo.Create(ctx, tr, dir, &store.Post{Id: "p1", Tags: []string{"go", "fdb"}}); err != nil {
			return err
		}
		if err := postRepo.Create(ctx, tr, dir, &store.Post{Id: "p2", Tags: []string{"go", "rust"}}); err != nil {
			return err
		}
		return postRepo.Create(ctx, tr, dir, &store.Post{Id: "p3", Tags: []string{"rust", "wasm"}})
	})

	// "go" should match p1 and p2
	var posts []*store.Post
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetPostByTags(ctx, tr, dir, "go")
		return err
	})
	if len(posts) != 2 {
		t.Fatalf("expected 2 posts tagged 'go', got %d", len(posts))
	}

	// "rust" should match p2 and p3
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetPostByTags(ctx, tr, dir, "rust")
		return err
	})
	if len(posts) != 2 {
		t.Fatalf("expected 2 posts tagged 'rust', got %d", len(posts))
	}

	// "fdb" should match only p1
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetPostByTags(ctx, tr, dir, "fdb")
		return err
	})
	if len(posts) != 1 || posts[0].Id != "p1" {
		t.Fatalf("expected p1 for 'fdb', got %+v", posts)
	}

	// "wasm" should match only p3
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetPostByTags(ctx, tr, dir, "wasm")
		return err
	})
	if len(posts) != 1 || posts[0].Id != "p3" {
		t.Fatalf("expected p3 for 'wasm', got %+v", posts)
	}

	// Nonexistent tag
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetPostByTags(ctx, tr, dir, "python")
		return err
	})
	if len(posts) != 0 {
		t.Fatalf("expected 0 posts for 'python', got %d", len(posts))
	}
}

func TestIntegration_SetPost_FanOutIndexUpdated(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	postRepo := store.NewPostRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		return postRepo.Create(ctx, tr, dir, &store.Post{Id: "p1", Tags: []string{"alpha", "beta"}})
	})

	// Update tags: remove "alpha", keep "beta", add "gamma"
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return postRepo.Set(ctx, tr, dir, &store.Post{Id: "p1", Tags: []string{"beta", "gamma"}})
	})

	// "alpha" should no longer return any results (stale index cleared)
	var posts []*store.Post
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetPostByTags(ctx, tr, dir, "alpha")
		return err
	})
	if len(posts) != 0 {
		t.Fatal("stale fan-out index: 'alpha' still returns results after update")
	}

	// "beta" should still find the post
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetPostByTags(ctx, tr, dir, "beta")
		return err
	})
	if len(posts) != 1 || posts[0].Id != "p1" {
		t.Fatalf("expected p1 for 'beta', got %+v", posts)
	}

	// "gamma" should find the post (new index entry)
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetPostByTags(ctx, tr, dir, "gamma")
		return err
	})
	if len(posts) != 1 || posts[0].Id != "p1" {
		t.Fatalf("expected p1 for 'gamma', got %+v", posts)
	}
}

func TestIntegration_DeletePost_ClearsFanOutIndex(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	postRepo := store.NewPostRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		return postRepo.Create(ctx, tr, dir, &store.Post{Id: "p1", Tags: []string{"x", "y", "z"}})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return postRepo.Delete(ctx, tr, dir, "p1")
	})

	// All tag index entries should be cleared
	for _, tag := range []string{"x", "y", "z"} {
		var posts []*store.Post
		tests.WithTx(t, db, func(tr fdb.Transaction) error {
			var err error
			posts, err = postRepo.GetPostByTags(ctx, tr, dir, tag)
			return err
		})
		if len(posts) != 0 {
			t.Fatalf("fan-out index not cleared for tag %q after delete", tag)
		}
	}

	// Get should fail
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return postRepo.Get(ctx, tr, dir, "p1")
	})
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestIntegration_ListPost_WithFanOutIndex(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	postRepo := store.NewPostRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		// Create posts with multiple tags each — generates many index entries
		for i := 0; i < 5; i++ {
			if err := postRepo.Create(ctx, tr, dir, &store.Post{
				Id:   fmt.Sprintf("p%d", i),
				Tags: []string{"common", fmt.Sprintf("unique%d", i)},
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// List should return all 5 posts, skipping index entries
	var result *store.PostPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = postRepo.ListPost(ctx, tr, dir, store.PostPaginationOptions{Limit: 10})
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
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	postRepo := store.NewPostRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			if err := postRepo.Create(ctx, tr, dir, &store.Post{
				Id:   fmt.Sprintf("p%d", i),
				Tags: []string{"t1", "t2", "t3"},
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// Page through with limit 2
	var page1 *store.PostPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page1, err = postRepo.ListPost(ctx, tr, dir, store.PostPaginationOptions{Limit: 2})
		return err
	})
	if len(page1.Items) != 2 {
		t.Fatalf("page1: expected 2 items, got %d", len(page1.Items))
	}
	if !page1.HasMore {
		t.Fatal("page1: expected HasMore=true")
	}

	var page2 *store.PostPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page2, err = postRepo.ListPost(ctx, tr, dir, store.PostPaginationOptions{Begin: page1.NextKey, Limit: 2})
		return err
	})
	if len(page2.Items) != 2 {
		t.Fatalf("page2: expected 2 items, got %d", len(page2.Items))
	}

	var page3 *store.PostPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page3, err = postRepo.ListPost(ctx, tr, dir, store.PostPaginationOptions{Begin: page2.NextKey, Limit: 2})
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
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	postRepo := store.NewPostRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		return postRepo.Create(ctx, tr, dir, &store.Post{Id: "p1", Tags: []string{}})
	})

	var post *store.Post
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		post, err = postRepo.Get(ctx, tr, dir, "p1")
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
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	postRepo := store.NewPostRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		for _, id := range []string{"p1", "p2", "p3"} {
			if err := postRepo.Create(ctx, tr, dir, &store.Post{Id: id, Tags: []string{"tag"}}); err != nil {
				return err
			}
		}
		return nil
	})

	var result map[string]*store.Post
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		result, err = postRepo.BatchGetPost(ctx, tr, dir, []tuple.Tuple{{"p1"}, {"p2"}, {"p3"}})
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
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)
	productRepo := store.NewProductRepository(recordStore)
	postRepo := store.NewPostRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"}); err != nil {
			return err
		}
		// Create entities of all types simultaneously
		if err := userRepo.Create(ctx, tr, dir, &store.User{Id: "u1", Name: "Alice", Email: "a@test.com"}); err != nil {
			return err
		}
		if err := productRepo.Create(ctx, tr, dir, &store.Product{Id: "pr1", Name: "Widget", Category: "tools", Price: 10}); err != nil {
			return err
		}
		return postRepo.Create(ctx, tr, dir, &store.Post{Id: "po1", Tags: []string{"go"}})
	})

	// Each type's operations should be completely isolated
	var users []*store.User
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		users, err = userRepo.GetUserByEmail(ctx, tr, dir, "a@test.com")
		return err
	})
	if len(users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(users))
	}

	var posts []*store.Post
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		posts, err = postRepo.GetPostByTags(ctx, tr, dir, "go")
		return err
	})
	if len(posts) != 1 {
		t.Fatalf("expected 1 post, got %d", len(posts))
	}

	// Verify listing each type returns exactly 1 entity
	var userResult *store.UserPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		userResult, err = userRepo.ListUser(ctx, tr, dir, store.UserPaginationOptions{Limit: 10})
		return err
	})
	if len(userResult.Items) != 1 {
		t.Fatalf("expected 1 user in list, got %d", len(userResult.Items))
	}

	var postResult *store.PostPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		postResult, err = postRepo.ListPost(ctx, tr, dir, store.PostPaginationOptions{Limit: 10})
		return err
	})
	if len(postResult.Items) != 1 {
		t.Fatalf("expected 1 post in list, got %d", len(postResult.Items))
	}
}

func TestIntegration_VersionstampedPrimaryKey(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	taskRepo := store.NewTaskMessageRepository(recordStore)

	task := &store.TaskMessage{
		QueueName: "email_queue",
		ShardId:   1,
		Payload:   []byte("send email"),
	}

	// 1. Create a versionstamped task message
	var commitVer fdb.FutureKey
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"TaskMessage"}); err != nil {
			return err
		}
		if err := taskRepo.Create(ctx, tr, dir, task); err != nil {
			return err
		}
		commitVer = tr.GetVersionstamp()
		return nil
	})

	// Wait for transaction commit to complete and get the transaction versionstamp
	vsBytes, err := commitVer.Get()
	if err != nil {
		t.Fatalf("failed to get commit versionstamp: %v", err)
	}

	// In FDB, the returned transaction versionstamp is 10 bytes long.
	// We wrap it in a tuple.Versionstamp to retrieve the record.
	var vs tuple.Versionstamp
	copy(vs.TransactionVersion[:], vsBytes)
	vs.UserVersion = 0

	// 2. Fetch the entity using the committed versionstamp
	var fetched *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		pk := store.TaskMessagePrimaryKey{
			QueueName:    "email_queue",
			ShardId:      1,
			Versionstamp: vs,
		}
		fetched, err = taskRepo.Get(ctx, tr, dir, pk)
		return err
	})

	if fetched == nil {
		t.Fatalf("expected to fetch task message but got nil")
	}
	if fetched.QueueName != "email_queue" {
		t.Errorf("expected queue name email_queue, got %s", fetched.QueueName)
	}
	if fetched.ShardId != 1 {
		t.Errorf("expected shard id 1, got %d", fetched.ShardId)
	}
	if string(fetched.Payload) != "send email" {
		t.Errorf("expected payload 'send email', got %s", fetched.Payload)
	}
}
