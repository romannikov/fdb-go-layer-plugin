package basic_test

import (
	"context"
	"testing"

	"github.com/romannikov/fdb-go-layer-plugin/tests"
	"github.com/romannikov/fdb-go-layer-plugin/tests/store"
)

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel context immediately

	recordStore, tr, dir, _ := tests.SyncAndSetup()

	// Test SyncMetadata
	err := recordStore.SyncMetadata(ctx, tr, dir)
	if err == nil {
		t.Fatal("expected error on SyncMetadata with cancelled context")
	}

	// Test Create
	user := &store.User{Id: "u_cancel", Name: "Cancel", Email: "cancel@test.com"}
	err = recordStore.CreateUser(ctx, tr, dir, user)
	if err == nil {
		t.Fatal("expected error on CreateUser with cancelled context")
	}

	// Test Get
	_, err = recordStore.GetUser(ctx, tr, dir, "u_cancel")
	if err == nil {
		t.Fatal("expected error on GetUser with cancelled context")
	}

	// Test Set
	err = recordStore.SetUser(ctx, tr, dir, user)
	if err == nil {
		t.Fatal("expected error on SetUser with cancelled context")
	}

	// Test Delete
	err = recordStore.DeleteUser(ctx, tr, dir, "u_cancel")
	if err == nil {
		t.Fatal("expected error on DeleteUser with cancelled context")
	}

	// Test List
	_, err = recordStore.ListUser(ctx, tr, dir, store.UserPaginationOptions{Limit: 10})
	if err == nil {
		t.Fatal("expected error on ListUser with cancelled context")
	}
}
