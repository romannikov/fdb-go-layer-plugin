package complex_index_test

import (
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	"github.com/romannikov/fdb-go-layer-plugin/tests"
	"github.com/romannikov/fdb-go-layer-plugin/tests/store"
	fdblayer "github.com/romannikov/fdb-go-layer-plugin/fdb-layer"
)

func init() {
	fdb.MustAPIVersion(710)
}

func TestFanOutIndex(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()

	postRepo := store.NewPostRepository(recordStore)

	post := &store.Post{
		Id:   "post1",
		Tags: []string{"tag1", "tag2", "tag3"},
	}

	err := postRepo.Create(ctx, tr, dir, post)
	if err != nil {
		t.Fatal(err)
	}

	// Verify index entries in MockKV using the public metadata lookup
	typeID := recordStore.Metadata()["Post"]

	for _, tag := range post.Tags {
		indexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(4095142816), tag, post.Id})
		if !kv.HasKey(indexKey) {
			t.Errorf("Missing index entry for tag %s", tag)
		}
	}
}

func TestFanOutIndex_SetDeltaUpdates(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()

	postRepo := store.NewPostRepository(recordStore)

	post := &store.Post{
		Id:   "post1",
		Tags: []string{"tag1", "tag2", "tag3"},
	}

	if err := postRepo.Create(ctx, tr, dir, post); err != nil {
		t.Fatal(err)
	}

	tr.ResetLog()
	// Update Tags: remove "tag1", keep "tag2" and "tag3", add "tag4"
	updatedPost := &store.Post{
		Id:   "post1",
		Tags: []string{"tag2", "tag3", "tag4"},
	}
	if err := postRepo.Set(ctx, tr, dir, updatedPost); err != nil {
		t.Fatal(err)
	}

	typeID := recordStore.Metadata()["Post"]

	// Only "tag1" should have been cleared (1 Clear call instead of 3)
	if len(tr.ClearCalls) != 1 {
		t.Fatalf("expected exactly 1 Clear call for removed tag1, got %d", len(tr.ClearCalls))
	}
	// Only primary record + "tag4" index should have been set (2 Set calls instead of 4)
	if len(tr.SetCalls) != 2 {
		t.Fatalf("expected exactly 2 Set calls (primary key + new tag4 index), got %d", len(tr.SetCalls))
	}

	removedKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(4095142816), "tag1", post.Id})
	if kv.HasKey(removedKey) {
		t.Errorf("Removed tag1 index entry still present")
	}

	for _, tag := range updatedPost.Tags {
		indexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(4095142816), tag, post.Id})
		if !kv.HasKey(indexKey) {
			t.Errorf("Missing index entry for tag %s", tag)
		}
	}
}

func TestVersionstampedPrimaryKey(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()

	taskRepo := store.NewTaskMessageRepository(recordStore)

	task := &store.TaskMessage{
		QueueName: "email_queue",
		ShardId:   1,
		Payload:   []byte("send email"),
	}

	err := taskRepo.Enqueue(ctx, tr, dir, task)
	if err != nil {
		t.Fatal(err)
	}

	typeID := recordStore.Metadata()["TaskMessage"]

	// The mock transaction's SetVersionstampedKey replaces the incomplete versionstamp
	// placeholder with dummyVS transaction bytes.
	dummyVS := tuple.Versionstamp{
		TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		UserVersion:        0,
	}
	expectedKey := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, "email_queue", uint64(1), dummyVS})

	if !kv.HasKey(expectedKey) {
		t.Fatalf("Expected key not found in mock store: %v", expectedKey)
	}

	// Second enqueue in the same transaction with the same QueueName and ShardId
	// must receive UserVersion=1 so it does not overwrite the first item.
	task2 := &store.TaskMessage{
		QueueName: "email_queue",
		ShardId:   1,
		Payload:   []byte("send second email"),
	}
	if err := taskRepo.Enqueue(ctx, tr, dir, task2); err != nil {
		t.Fatal(err)
	}
	dummyVS2 := tuple.Versionstamp{
		TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		UserVersion:        1,
	}
	expectedKey2 := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, "email_queue", uint64(1), dummyVS2})
	if !kv.HasKey(expectedKey2) {
		t.Fatalf("Expected second versionstamped key with UserVersion=1 not found in mock store: %v", expectedKey2)
	}
}

