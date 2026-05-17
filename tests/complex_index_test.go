package store

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func TestFanOutIndex(t *testing.T) {
	store, tr, dir, kv := syncAndSetup()

	postRepo := NewPostRepository(store)

	post := &Post{
		Id:   "post1",
		Tags: []string{"tag1", "tag2", "tag3"},
	}

	err := postRepo.Create(tr, dir, post)
	if err != nil {
		t.Fatal(err)
	}

	// Verify index entries in MockKV
	typeID, _ := store.getPostTypeID()

	for _, tag := range post.Tags {
		indexKey := dir.Pack(tuple.Tuple{typeID, "index", "Tags", tag, post.Id})
		if !kv.HasKey(indexKey) {
			t.Errorf("Missing index entry for tag %s", tag)
		}
	}
}
