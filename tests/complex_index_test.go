package store

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func TestFanOutIndex(t *testing.T) {
	store, tr, dir, kv := syncAndSetup()

	post := &Post{
		Id:   "post1",
		Tags: []string{"tag1", "tag2", "tag3"},
	}

	err := store.CreatePost(tr, dir, post)
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

func TestVersionstampIndex(t *testing.T) {
	store, tr, dir, kv := syncAndSetup()

	doc := &Document{
		Id:      "doc1",
		Content: "hello",
	}

	err := store.CreateDocument(tr, dir, doc)
	if err != nil {
		t.Fatal(err)
	}

	// Verify index entry in MockKV
	typeID, _ := store.getDocumentTypeID()
	
	// Our mock uses "0123456789" as dummy versionstamp
	dummyVS := []byte("0123456789")
	indexKey := dir.Pack(tuple.Tuple{typeID, "index", "versionstamp", dummyVS, doc.Id})
	
	if !kv.HasKey(indexKey) {
		t.Errorf("Missing index entry for versionstamp")
	}
}
