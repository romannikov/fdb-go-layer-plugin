//go:build integration

package conformance_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	"github.com/romannikov/fdb-layer/tests/go/atomic"
	"github.com/romannikov/fdb-layer/tests/go/store"
)

func init() {
	fdb.MustAPIVersion(710)
}

// sharedSubspaceDir adapts a deterministic tuple-prefixed subspace.Subspace to
// directory.DirectorySubspace so Go and Rust can address the exact same key prefix.
type sharedSubspaceDir struct {
	directory.DirectorySubspace
	sub subspace.Subspace
}

func newSharedSubspaceDir(prefix ...tuple.TupleElement) *sharedSubspaceDir {
	return &sharedSubspaceDir{
		sub: subspace.FromBytes(tuple.Tuple(prefix).Pack()),
	}
}

func (s *sharedSubspaceDir) Pack(t tuple.Tuple) fdb.Key {
	return s.sub.Pack(t)
}

func (s *sharedSubspaceDir) PackWithVersionstamp(t tuple.Tuple) (fdb.Key, error) {
	return s.sub.PackWithVersionstamp(t)
}

func (s *sharedSubspaceDir) Unpack(k fdb.KeyConvertible) (tuple.Tuple, error) {
	return s.sub.Unpack(k)
}

func (s *sharedSubspaceDir) FDBKey() fdb.Key {
	return s.sub.FDBKey()
}

func (s *sharedSubspaceDir) FDBRangeKeys() (fdb.KeyConvertible, fdb.KeyConvertible) {
	return s.sub.FDBRangeKeys()
}

func (s *sharedSubspaceDir) FDBRangeKeySelectors() (fdb.Selectable, fdb.Selectable) {
	return s.sub.FDBRangeKeySelectors()
}

func withTx(t *testing.T, db fdb.Database, fn func(tr fdb.Transaction) error) {
	t.Helper()
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return nil, fn(tr)
	})
	if err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}

func TestCrossLanguageConformance_GoAndRust(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "fdb_conformance.bin")
	t.Setenv("FDB_STORE_PATH", storePath)

	ctx := context.Background()
	db := fdb.MustOpenDefault()
	dir := newSharedSubspaceDir("conformance_v1")

	recordStore := fdblayer.NewRecordStore()
	userRepo := store.NewUserRepository(recordStore)
	productRepo := store.NewProductRepository(recordStore)
	postRepo := store.NewPostRepository(recordStore)
	taskRepo := store.NewTaskMessageRepository(recordStore)
	counterRepo := atomic.NewCounterRepository(recordStore)

	// =========================================================================
	// Phase 1: Go writes metadata, entities, secondary indexes, atomics & queue
	// =========================================================================
	withTx(t, db, func(tr fdb.Transaction) error {
		return recordStore.SyncMetadata(ctx, tr, dir, []string{
			"User", "Product", "Post", "TaskMessage", "Counter",
		})
	})

	withTx(t, db, func(tr fdb.Transaction) error {
		if err := userRepo.Create(ctx, tr, dir, &store.User{
			Id: "u1", Name: "Alice", Email: "alice@example.com",
		}); err != nil {
			return err
		}
		if err := userRepo.Create(ctx, tr, dir, &store.User{
			Id: "u2", Name: "Bob", Email: "bob@example.com",
		}); err != nil {
			return err
		}
		if err := productRepo.Create(ctx, tr, dir, &store.Product{
			Id: "prod1", Name: "Mechanical Keyboard", Category: "hardware", Price: 150,
		}); err != nil {
			return err
		}
		if err := postRepo.Create(ctx, tr, dir, &store.Post{
			Id: "post1", Tags: []string{"fdb", "golang", "rust"},
		}); err != nil {
			return err
		}
		return counterRepo.Create(ctx, tr, dir, &atomic.Counter{
			Id: "cnt1", Value: 100, MaxValue: 500, MinValue: 20,
		})
	})

	withTx(t, db, func(tr fdb.Transaction) error {
		if err := counterRepo.AddCounterValue(ctx, tr, dir, "cnt1", 25); err != nil {
			return err
		}
		if err := counterRepo.MaxCounterMaxValue(ctx, tr, dir, "cnt1", 750); err != nil {
			return err
		}
		return counterRepo.MinCounterMinValue(ctx, tr, dir, "cnt1", 10)
	})

	withTx(t, db, func(tr fdb.Transaction) error {
		return taskRepo.Enqueue(ctx, tr, dir, &store.TaskMessage{
			QueueName: "shared_queue", ShardId: 7, Payload: []byte("from-go-1"),
		})
	})
	withTx(t, db, func(tr fdb.Transaction) error {
		return taskRepo.Enqueue(ctx, tr, dir, &store.TaskMessage{
			QueueName: "shared_queue", ShardId: 2, Payload: []byte("from-go-2"),
		})
	})

	// =========================================================================
	// Phase 2: Invoke Rust conformance worker against the same FDB store
	// =========================================================================
	rustBin := os.Getenv("CONFORMANCE_RUST_BIN")
	var cmd *exec.Cmd
	if rustBin != "" {
		cmd = exec.CommandContext(ctx, rustBin)
	} else {
		cmd = exec.CommandContext(ctx, "cargo", "run", "--offline", "--quiet")
	}
	cmd.Env = append(os.Environ(), "FDB_STORE_PATH="+storePath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Rust conformance worker failed: %v\nOutput:\n%s", err, string(out))
	}

	// =========================================================================
	// Phase 3: Go reads back all Rust mutations and verifies binary compatibility
	// =========================================================================

	// 1. Verify User u1 updated by Rust & email secondary index updated
	var u1 *store.User
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		u1, err = userRepo.Get(ctx, tr, dir, "u1")
		return err
	})
	if u1.Name != "Alice (updated by Rust)" || u1.Email != "alice.rust@example.com" {
		t.Fatalf("unexpected u1 after Rust update: %+v", u1)
	}

	var oldEmailUsers, newEmailUsers []*store.User
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		oldEmailUsers, err = userRepo.GetUserByEmail(ctx, tr, dir, "alice@example.com")
		if err != nil {
			return err
		}
		newEmailUsers, err = userRepo.GetUserByEmail(ctx, tr, dir, "alice.rust@example.com")
		return err
	})
	if len(oldEmailUsers) != 0 {
		t.Fatalf("expected old email index cleared by Rust, got %d results", len(oldEmailUsers))
	}
	if len(newEmailUsers) != 1 || newEmailUsers[0].Id != "u1" {
		t.Fatalf("expected new email index written by Rust, got %+v", newEmailUsers)
	}

	// 2. Verify User u2 deleted by Rust
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return userRepo.Get(ctx, tr, dir, "u2")
	})
	if err == nil {
		t.Fatal("expected u2 to be deleted by Rust")
	}

	// 3. Verify Post fan-out index delta updated by Rust
	var golangPosts, conformancePosts []*store.Post
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		golangPosts, err = postRepo.GetPostByTags(ctx, tr, dir, "golang")
		if err != nil {
			return err
		}
		conformancePosts, err = postRepo.GetPostByTags(ctx, tr, dir, "conformance")
		return err
	})
	if len(golangPosts) != 0 {
		t.Fatalf("expected 'golang' tag index removed by Rust, got %d", len(golangPosts))
	}
	if len(conformancePosts) != 1 || conformancePosts[0].Id != "post1" {
		t.Fatalf("expected 'conformance' tag index added by Rust, got %+v", conformancePosts)
	}

	// 4. Verify Product prod2 created by Rust
	var bookProducts []*store.Product
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		bookProducts, err = productRepo.GetProductByCategory(ctx, tr, dir, "books")
		return err
	})
	if len(bookProducts) != 1 || bookProducts[0].Id != "prod2" || bookProducts[0].Price != 45 {
		t.Fatalf("unexpected book product created by Rust: %+v", bookProducts)
	}

	// 5. Verify Counter cnt1 atomic mutations applied by Rust
	var cnt1 *atomic.Counter
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		cnt1, err = counterRepo.Get(ctx, tr, dir, "cnt1")
		return err
	})
	if cnt1.Value != 200 || cnt1.MaxValue != 1000 || cnt1.MinValue != 5 {
		t.Fatalf("unexpected counter state after Rust atomic ops: %+v", cnt1)
	}

	// 6. Verify Queue FIFO order across Go and Rust enqueues/dequeues
	var nextTask1, nextTask2, emptyTask *store.TaskMessage
	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		nextTask1, err = taskRepo.Dequeue(ctx, tr, dir, "shared_queue")
		return err
	})
	if nextTask1 == nil || string(nextTask1.Payload) != "from-go-2" || nextTask1.ShardId != 2 || len(nextTask1.Versionstamp) != 12 {
		t.Fatalf("expected second Go task 'from-go-2', got %+v", nextTask1)
	}

	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		nextTask2, err = taskRepo.Dequeue(ctx, tr, dir, "shared_queue")
		return err
	})
	if nextTask2 == nil || string(nextTask2.Payload) != "from-rust-3" || nextTask2.ShardId != 9 || len(nextTask2.Versionstamp) != 12 {
		t.Fatalf("expected Rust-enqueued task 'from-rust-3', got %+v", nextTask2)
	}

	withTx(t, db, func(tr fdb.Transaction) error {
		var err error
		emptyTask, err = taskRepo.Dequeue(ctx, tr, dir, "shared_queue")
		return err
	})
	if emptyTask != nil {
		t.Fatalf("expected empty queue after dequeuing all items, got %+v", emptyTask)
	}
}
