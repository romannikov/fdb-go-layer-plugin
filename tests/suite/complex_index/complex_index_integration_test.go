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

func TestIntegration_QueueEnqueueDequeue(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	taskRepo := store.NewTaskMessageRepository(recordStore)

	// Sync metadata
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"TaskMessage"})
	})

	// 1. Enqueue three tasks sequentially in separate transactions to ensure distinct versionstamps
	task1 := &store.TaskMessage{QueueName: "test_queue", ShardId: 1, Payload: []byte("task 1")}
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return taskRepo.Enqueue(ctx, tr, dir, task1)
	})

	task2 := &store.TaskMessage{QueueName: "test_queue", ShardId: 2, Payload: []byte("task 2")}
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return taskRepo.Enqueue(ctx, tr, dir, task2)
	})

	task3 := &store.TaskMessage{QueueName: "test_queue", ShardId: 3, Payload: []byte("task 3")}
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return taskRepo.Enqueue(ctx, tr, dir, task3)
	})

	// 2. Dequeue tasks and verify FIFO order
	var dequeued1 *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		dequeued1, err = taskRepo.Dequeue(ctx, tr, dir, "test_queue")
		return err
	})
	if dequeued1 == nil || string(dequeued1.Payload) != "task 1" {
		t.Fatalf("expected 'task 1', got %+v", dequeued1)
	}

	var dequeued2 *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		dequeued2, err = taskRepo.Dequeue(ctx, tr, dir, "test_queue")
		return err
	})
	if dequeued2 == nil || string(dequeued2.Payload) != "task 2" {
		t.Fatalf("expected 'task 2', got %+v", dequeued2)
	}

	var dequeued3 *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		dequeued3, err = taskRepo.Dequeue(ctx, tr, dir, "test_queue")
		return err
	})
	if dequeued3 == nil || string(dequeued3.Payload) != "task 3" {
		t.Fatalf("expected 'task 3', got %+v", dequeued3)
	}

	// 3. Dequeue on empty queue should return nil, nil
	var emptyResult *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		emptyResult, err = taskRepo.Dequeue(ctx, tr, dir, "test_queue")
		return err
	})
	if emptyResult != nil {
		t.Fatalf("expected nil for empty queue Dequeue, got %+v", emptyResult)
	}
}

func TestIntegration_MultipleQueues(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	taskRepo := store.NewTaskMessageRepository(recordStore)

	// Sync metadata
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"TaskMessage"})
	})

	// 1. Enqueue task A to queue_A
	taskA := &store.TaskMessage{QueueName: "queue_A", ShardId: 1, Payload: []byte("task A")}
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return taskRepo.Enqueue(ctx, tr, dir, taskA)
	})

	// 2. Enqueue task B to queue_B
	taskB := &store.TaskMessage{QueueName: "queue_B", ShardId: 1, Payload: []byte("task B")}
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return taskRepo.Enqueue(ctx, tr, dir, taskB)
	})

	// 3. Dequeue from queue_A and verify it is task A
	var dequeuedA *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		dequeuedA, err = taskRepo.Dequeue(ctx, tr, dir, "queue_A")
		return err
	})
	if dequeuedA == nil || string(dequeuedA.Payload) != "task A" {
		t.Fatalf("expected 'task A' from queue_A, got %+v", dequeuedA)
	}

	// 4. Dequeue from queue_B and verify it is task B
	var dequeuedB *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		dequeuedB, err = taskRepo.Dequeue(ctx, tr, dir, "queue_B")
		return err
	})
	if dequeuedB == nil || string(dequeuedB.Payload) != "task B" {
		t.Fatalf("expected 'task B' from queue_B, got %+v", dequeuedB)
	}

	// 5. Verify both are now empty
	var emptyA *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		emptyA, err = taskRepo.Dequeue(ctx, tr, dir, "queue_A")
		return err
	})
	if emptyA != nil {
		t.Fatalf("expected queue_A to be empty, got %+v", emptyA)
	}

	var emptyB *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		emptyB, err = taskRepo.Dequeue(ctx, tr, dir, "queue_B")
		return err
	})
	if emptyB != nil {
		t.Fatalf("expected queue_B to be empty, got %+v", emptyB)
	}
}
