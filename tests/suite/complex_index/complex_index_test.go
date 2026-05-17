package complex_index_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	"github.com/romannikov/fdb-go-layer-plugin/tests"
	"github.com/romannikov/fdb-go-layer-plugin/tests/store"
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
		indexKey := dir.Pack(tuple.Tuple{typeID, "index", "Tags", tag, post.Id})
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

	err := taskRepo.Create(ctx, tr, dir, task)
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
	expectedKey := dir.Pack(tuple.Tuple{typeID, "email_queue", int64(1), dummyVS})

	if !kv.HasKey(expectedKey) {
		t.Fatalf("Expected key not found in mock store: %v", expectedKey)
	}

	// Retrieve the task by its primary key
	pk := store.TaskMessagePrimaryKey{
		QueueName:    "email_queue",
		ShardId:      1,
		Versionstamp: dummyVS,
	}
	fetched, err := taskRepo.Get(ctx, tr, dir, pk)
	if err != nil {
		t.Fatal(err)
	}

	if fetched.QueueName != "email_queue" || fetched.ShardId != 1 || !bytes.Equal(fetched.Payload, []byte("send email")) {
		t.Errorf("Unexpected fetched task: %+v", fetched)
	}
}
