//go:build integration

package complex_index_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	tests "github.com/romannikov/fdb-layer/tests/go"
	"github.com/romannikov/fdb-layer/tests/go/store"
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

	// 1. Enqueue three tasks sequentially in separate transactions with out-of-order ShardIds (3, 1, 2)
	// to verify global FIFO ordering by versionstamp rather than shard_id.
	task1 := &store.TaskMessage{QueueName: "test_queue", ShardId: 3, Payload: []byte("task 1")}
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return taskRepo.Enqueue(ctx, tr, dir, task1)
	})

	task2 := &store.TaskMessage{QueueName: "test_queue", ShardId: 1, Payload: []byte("task 2")}
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return taskRepo.Enqueue(ctx, tr, dir, task2)
	})

	task3 := &store.TaskMessage{QueueName: "test_queue", ShardId: 2, Payload: []byte("task 3")}
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return taskRepo.Enqueue(ctx, tr, dir, task3)
	})

	// 2. Dequeue tasks and verify FIFO order across shards
	var dequeued1 *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		dequeued1, err = taskRepo.Dequeue(ctx, tr, dir, "test_queue")
		return err
	})
	if dequeued1 == nil || string(dequeued1.Payload) != "task 1" || dequeued1.ShardId != 3 {
		t.Fatalf("expected 'task 1' (ShardId 3), got %+v", dequeued1)
	}

	var dequeued2 *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		dequeued2, err = taskRepo.Dequeue(ctx, tr, dir, "test_queue")
		return err
	})
	if dequeued2 == nil || string(dequeued2.Payload) != "task 2" || dequeued2.ShardId != 1 {
		t.Fatalf("expected 'task 2' (ShardId 1), got %+v", dequeued2)
	}

	var dequeued3 *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		dequeued3, err = taskRepo.Dequeue(ctx, tr, dir, "test_queue")
		return err
	})
	if dequeued3 == nil || string(dequeued3.Payload) != "task 3" || dequeued3.ShardId != 2 {
		t.Fatalf("expected 'task 3' (ShardId 2), got %+v", dequeued3)
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

func TestIntegration_QueueSameTransactionEnqueue(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	taskRepo := store.NewTaskMessageRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"TaskMessage"})
	})

	// Enqueue two tasks with the SAME QueueName and ShardId in a SINGLE transaction.
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := taskRepo.Enqueue(ctx, tr, dir, &store.TaskMessage{
			QueueName: "batch_q",
			ShardId:   1,
			Payload:   []byte("first"),
		}); err != nil {
			return err
		}
		return taskRepo.Enqueue(ctx, tr, dir, &store.TaskMessage{
			QueueName: "batch_q",
			ShardId:   1,
			Payload:   []byte("second"),
		})
	})

	var d1, d2 *store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		d1, err = taskRepo.Dequeue(ctx, tr, dir, "batch_q")
		return err
	})
	if d1 == nil || string(d1.Payload) != "first" || len(d1.Versionstamp) != 12 {
		t.Fatalf("expected first task with 12-byte versionstamp, got %+v", d1)
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		d2, err = taskRepo.Dequeue(ctx, tr, dir, "batch_q")
		return err
	})
	if d2 == nil || string(d2.Payload) != "second" || len(d2.Versionstamp) != 12 {
		t.Fatalf("expected second task with 12-byte versionstamp, got %+v", d2)
	}
}

func TestIntegration_Order_CompoundPKAndIndexes(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	orderRepo := store.NewOrderRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"Order"})
	})

	order1 := &store.Order{
		TenantId:         "acme",
		OrderSeq:         101,
		Status:           store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt:        1700000000,
		ReceiptHash:      []byte{0xAA, 0xBB, 0x01},
		AttachmentHashes: [][]byte{{0x10, 0x20}, {0x30, 0x40}},
		Priority:         store.Order_PRIORITY_HIGH,
		Address: &store.Order_ShippingAddress{
			City:    "Zurich",
			Country: "Switzerland",
		},
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return orderRepo.Create(ctx, tr, dir, order1)
	})

	pk := store.OrderPrimaryKey{TenantId: "acme", OrderSeq: 101}
	var got *store.Order
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = orderRepo.Get(ctx, tr, dir, pk)
		return err
	})
	if got.Priority != store.Order_PRIORITY_HIGH || got.Address.GetCity() != "Zurich" {
		t.Fatalf("unexpected Order after Get: %+v", got)
	}

	// Compound index lookup
	var byStatus []*store.Order
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		byStatus, err = orderRepo.GetOrderByStatusAndCreatedAt(ctx, tr, dir, store.OrderStatus_ORDER_STATUS_PENDING, 1700000000)
		return err
	})
	if len(byStatus) != 1 || byStatus[0].OrderSeq != 101 {
		t.Fatalf("GetOrderByStatusAndCreatedAt failed: %+v", byStatus)
	}

	// Scalar bytes index lookup
	var byReceipt []*store.Order
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		byReceipt, err = orderRepo.GetOrderByReceiptHash(ctx, tr, dir, []byte{0xAA, 0xBB, 0x01})
		return err
	})
	if len(byReceipt) != 1 || byReceipt[0].OrderSeq != 101 {
		t.Fatalf("GetOrderByReceiptHash failed: %+v", byReceipt)
	}

	// Fan-out repeated bytes index lookup
	var byAttach []*store.Order
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		byAttach, err = orderRepo.GetOrderByAttachmentHashes(ctx, tr, dir, []byte{0x10, 0x20})
		return err
	})
	if len(byAttach) != 1 || byAttach[0].OrderSeq != 101 {
		t.Fatalf("GetOrderByAttachmentHashes failed: %+v", byAttach)
	}

	// Update via Set
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return orderRepo.Set(ctx, tr, dir, &store.Order{
			TenantId:         "acme",
			OrderSeq:         101,
			Status:           store.OrderStatus_ORDER_STATUS_SHIPPED,
			CreatedAt:        1700000000,
			ReceiptHash:      []byte{0xCC, 0xDD, 0x02},
			AttachmentHashes: [][]byte{{0x30, 0x40}, {0x50, 0x60}},
			Priority:         store.Order_PRIORITY_LOW,
		})
	})

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		byStatus, err = orderRepo.GetOrderByStatusAndCreatedAt(ctx, tr, dir, store.OrderStatus_ORDER_STATUS_PENDING, 1700000000)
		return err
	})
	if len(byStatus) != 0 {
		t.Fatalf("stale compound index after Set: %+v", byStatus)
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		byAttach, err = orderRepo.GetOrderByAttachmentHashes(ctx, tr, dir, []byte{0x50, 0x60})
		return err
	})
	if len(byAttach) != 1 || byAttach[0].OrderSeq != 101 {
		t.Fatalf("missing new fan-out bytes index after Set: %+v", byAttach)
	}

	// Delete
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return orderRepo.Delete(ctx, tr, dir, pk)
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		byStatus, err = orderRepo.GetOrderByStatusAndCreatedAt(ctx, tr, dir, store.OrderStatus_ORDER_STATUS_SHIPPED, 1700000000)
		return err
	})
	if len(byStatus) != 0 {
		t.Fatalf("compound index not cleared after Delete: %+v", byStatus)
	}
}

func TestIntegration_AuditLog_NonQueueVersionstampPK(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	auditRepo := store.NewAuditLogRepository(recordStore)

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{"AuditLog"})
	})

	// 1. Create two AuditLog entities in the same transaction with initial atomic Views,
	// standard index (Actor), and fan-out index (Tags).
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := auditRepo.Create(ctx, tr, dir, &store.AuditLog{
			Actor: "alice",
			Tags:  []string{"auth", "login"},
			Views: 10,
		}); err != nil {
			return err
		}
		return auditRepo.Create(ctx, tr, dir, &store.AuditLog{
			Actor: "bob",
			Tags:  []string{"auth", "logout"},
			Views: 5,
		})
	})

	// 2. ListAuditLog unpacks the 12-byte versionstamp and populates atomic Views
	var listed *store.AuditLogPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		listed, err = auditRepo.ListAuditLog(ctx, tr, dir, store.AuditLogPaginationOptions{Limit: 10})
		return err
	})
	if len(listed.Items) != 2 || len(listed.Items[0].LogId) != 12 || len(listed.Items[1].LogId) != 12 {
		t.Fatalf("expected 2 AuditLogs with 12-byte LogId, got %+v", listed)
	}
	firstID := listed.Items[0].LogId
	secondID := listed.Items[1].LogId
	if bytes.Equal(firstID, secondID) {
		t.Fatalf("expected distinct 12-byte versionstamps in same transaction, got %x", firstID)
	}
	if listed.Items[0].Views != 10 || listed.Items[1].Views != 5 {
		t.Fatalf("expected Views 10 and 5 from ListAuditLog, got %d and %d", listed.Items[0].Views, listed.Items[1].Views)
	}

	// 3. Get by 12-byte versionstamp
	var got *store.AuditLog
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		got, err = auditRepo.Get(ctx, tr, dir, firstID)
		return err
	})
	if got.Actor != "alice" || !bytes.Equal(got.LogId, firstID) || got.Views != 10 {
		t.Fatalf("unexpected AuditLog from Get: %+v", got)
	}

	// 4. Query by standard index (GetAuditLogByActor) and fan-out index (GetAuditLogByTags)
	var byActor, byTagAuth []*store.AuditLog
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		byActor, err = auditRepo.GetAuditLogByActor(ctx, tr, dir, "alice")
		if err != nil {
			return err
		}
		byTagAuth, err = auditRepo.GetAuditLogByTags(ctx, tr, dir, "auth")
		return err
	})
	if len(byActor) != 1 || !bytes.Equal(byActor[0].LogId, firstID) || byActor[0].Views != 10 {
		t.Fatalf("unexpected GetAuditLogByActor('alice') result: %+v", byActor)
	}
	if len(byTagAuth) != 2 || !bytes.Equal(byTagAuth[0].LogId, firstID) || !bytes.Equal(byTagAuth[1].LogId, secondID) {
		t.Fatalf("unexpected GetAuditLogByTags('auth') result: %+v", byTagAuth)
	}

	// 5. BatchGetAuditLog with both tuple.Versionstamp and raw []byte elements
	var batchRes map[string]*store.AuditLog
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		batchRes, err = auditRepo.BatchGetAuditLog(ctx, tr, dir, []tuple.Tuple{
			{fdblayer.BytesToVersionstamp(firstID)},
			{secondID},
		})
		return err
	})
	if len(batchRes) != 2 {
		t.Fatalf("expected 2 results from BatchGetAuditLog, got %+v", batchRes)
	}
	for _, item := range batchRes {
		if len(item.LogId) != 12 {
			t.Fatalf("BatchGetAuditLog did not populate 12-byte LogId: %+v", item)
		}
		if item.Actor == "alice" && item.Views != 10 {
			t.Fatalf("BatchGetAuditLog expected Views=10 for alice, got %d", item.Views)
		}
	}

	// 6. AddAuditLogViews atomic mutator
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return auditRepo.AddAuditLogViews(ctx, tr, dir, firstID, 7)
	})

	// 7. Set on existing entity updates in-place (preserving LogId and Views) and updates indexes;
	// Set on empty LogId creates a new entity.
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := auditRepo.Set(ctx, tr, dir, &store.AuditLog{
			LogId: firstID,
			Actor: "alice_admin",
			Tags:  []string{"auth", "elevated"},
		}); err != nil {
			return err
		}
		return auditRepo.Set(ctx, tr, dir, &store.AuditLog{
			Actor: "charlie",
			Tags:  []string{"audit"},
			Views: 3,
		})
	})

	var oldActor, newActor, oldTag, newTag, charlieLogs []*store.AuditLog
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		if got, err = auditRepo.Get(ctx, tr, dir, firstID); err != nil {
			return err
		}
		if oldActor, err = auditRepo.GetAuditLogByActor(ctx, tr, dir, "alice"); err != nil {
			return err
		}
		if newActor, err = auditRepo.GetAuditLogByActor(ctx, tr, dir, "alice_admin"); err != nil {
			return err
		}
		if oldTag, err = auditRepo.GetAuditLogByTags(ctx, tr, dir, "login"); err != nil {
			return err
		}
		if newTag, err = auditRepo.GetAuditLogByTags(ctx, tr, dir, "elevated"); err != nil {
			return err
		}
		charlieLogs, err = auditRepo.GetAuditLogByActor(ctx, tr, dir, "charlie")
		return err
	})
	if got.Actor != "alice_admin" || got.Views != 17 || !bytes.Equal(got.LogId, firstID) {
		t.Fatalf("unexpected AuditLog after Set: %+v", got)
	}
	if len(oldActor) != 0 || len(newActor) != 1 || !bytes.Equal(newActor[0].LogId, firstID) || newActor[0].Views != 17 {
		t.Fatalf("Actor index mismatch after Set: old=%+v, new=%+v", oldActor, newActor)
	}
	if len(oldTag) != 0 || len(newTag) != 1 || !bytes.Equal(newTag[0].LogId, firstID) {
		t.Fatalf("Tags fan-out index mismatch after Set: old=%+v, new=%+v", oldTag, newTag)
	}
	if len(charlieLogs) != 1 || len(charlieLogs[0].LogId) != 12 || charlieLogs[0].Views != 3 {
		t.Fatalf("unexpected charlie AuditLog created via Set(empty LogId): %+v", charlieLogs)
	}

	// 7b. ListAuditLog pagination with raw []byte Begin cursor and invalid Set length check
	var pageFromSecond *store.AuditLogPaginatedResult
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		if err := auditRepo.Set(ctx, tr, dir, &store.AuditLog{
			LogId: []byte{1, 2, 3},
			Actor: "invalid",
		}); err == nil {
			t.Fatalf("expected Set with 3-byte LogId to fail")
		}
		var err error
		pageFromSecond, err = auditRepo.ListAuditLog(ctx, tr, dir, store.AuditLogPaginationOptions{
			Begin: tuple.Tuple{secondID},
			Limit: 1,
		})
		return err
	})
	if len(pageFromSecond.Items) != 1 || !bytes.Equal(pageFromSecond.Items[0].LogId, secondID) || !pageFromSecond.HasMore {
		t.Fatalf("expected ListAuditLog from secondID ([]byte) to return bob with HasMore=true, got %+v", pageFromSecond)
	}

	// 8. Delete clears primary record, standard index, fan-out index, and atomic field
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		return auditRepo.Delete(ctx, tr, dir, firstID)
	})
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		if newActor, err = auditRepo.GetAuditLogByActor(ctx, tr, dir, "alice_admin"); err != nil {
			return err
		}
		if newTag, err = auditRepo.GetAuditLogByTags(ctx, tr, dir, "elevated"); err != nil {
			return err
		}
		byTagAuth, err = auditRepo.GetAuditLogByTags(ctx, tr, dir, "auth")
		return err
	})
	if len(newActor) != 0 || len(newTag) != 0 {
		t.Fatalf("expected indexes for deleted AuditLog to be cleared, got actor=%+v tag=%+v", newActor, newTag)
	}
	if len(byTagAuth) != 1 || byTagAuth[0].Actor != "bob" {
		t.Fatalf("expected only bob on 'auth' tag after Delete, got %+v", byTagAuth)
	}
}

func TestIntegration_DequeueBatch_AndPrefixReverseListOrder(t *testing.T) {
	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir, cleanup := tests.TestDir(t, db)
	defer cleanup()

	recordStore := fdblayer.NewRecordStore()
	if err := recordStore.EnsureMetadata(ctx, db, dir, []string{"TaskMessage", "Order"}); err != nil {
		t.Fatalf("EnsureMetadata failed: %v", err)
	}

	taskRepo := store.NewTaskMessageRepository(recordStore)
	orderRepo := store.NewOrderRepository(recordStore)

	// 1. Enqueue 3 messages in the same transaction (including pre-filled Versionstamp to test BUG-4)
	// and dequeue in batches of 2 then 2 (testing PERF-5 and BUG-2 automatic cross-tx reset).
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		for i := 1; i <= 3; i++ {
			if err := taskRepo.Enqueue(ctx, tr, dir, &store.TaskMessage{
				QueueName:    "batch_live",
				ShardId:      uint32(i),
				Versionstamp: []byte{9, 9, 9},
				Payload:      []byte(fmt.Sprintf("msg-%d", i)),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	var batch1, batch2 []*store.TaskMessage
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		batch1, err = taskRepo.DequeueBatch(ctx, tr, dir, "batch_live", 2)
		return err
	})
	if len(batch1) != 2 || string(batch1[0].Payload) != "msg-1" || string(batch1[1].Payload) != "msg-2" || len(batch1[0].Versionstamp) != 12 {
		t.Fatalf("unexpected DequeueBatch 1 result: %+v", batch1)
	}

	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		batch2, err = taskRepo.DequeueBatch(ctx, tr, dir, "batch_live", 2)
		return err
	})
	if len(batch2) != 1 || string(batch2[0].Payload) != "msg-3" {
		t.Fatalf("unexpected DequeueBatch 2 result: %+v", batch2)
	}

	// 2. Prefix + Reverse pagination on compound PK Order in live FDB (PERF-1, PERF-3, BUG-5)
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		for _, seq := range []int32{10, 20, 30} {
			if err := orderRepo.Create(ctx, tr, dir, &store.Order{
				TenantId: "tenant_a",
				OrderSeq: seq,
				Status:   store.OrderStatus_ORDER_STATUS_PENDING,
			}); err != nil {
				return err
			}
		}
		return orderRepo.Create(ctx, tr, dir, &store.Order{
			TenantId: "tenant_b",
			OrderSeq: 99,
			Status:   store.OrderStatus_ORDER_STATUS_PENDING,
		})
	})

	var page1, page2 *store.OrderPaginatedResult
	var byPK []*store.Order
	tests.WithTx(t, db, func(tr fdb.Transaction) error {
		var err error
		page1, err = orderRepo.ListOrder(ctx, tr, dir, store.OrderPaginationOptions{
			Prefix:  tuple.Tuple{"tenant_a"},
			Limit:   2,
			Reverse: true,
		})
		if err != nil {
			return err
		}
		page2, err = orderRepo.ListOrder(ctx, tr, dir, store.OrderPaginationOptions{
			Prefix:  tuple.Tuple{"tenant_a"},
			Begin:   page1.NextKey,
			Limit:   2,
			Reverse: true,
		})
		if err != nil {
			return err
		}
		byPK, err = orderRepo.BatchGetOrderByPK(ctx, tr, dir, []store.OrderPrimaryKey{
			{TenantId: "tenant_a", OrderSeq: 10},
			{TenantId: "tenant_b", OrderSeq: 99},
		})
		return err
	})
	if len(page1.Items) != 2 || !page1.HasMore || page1.Items[0].OrderSeq != 30 || page1.Items[1].OrderSeq != 20 {
		t.Fatalf("unexpected live ListOrder(Prefix:tenant_a, Reverse:true) page 1: %+v", page1)
	}
	if len(page2.Items) != 1 || page2.HasMore || page2.Items[0].OrderSeq != 10 {
		t.Fatalf("unexpected live ListOrder(Prefix:tenant_a, Reverse:true) page 2: %+v", page2)
	}
	if len(byPK) != 2 {
		t.Fatalf("expected 2 orders from BatchGetOrderByPK, got %+v", byPK)
	}
}




