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
	userRepo := store.NewUserRepository(recordStore)

	// Test SyncMetadata
	err := recordStore.SyncMetadata(ctx, tr, dir, []string{"User", "Product", "Post"})
	if err == nil {
		t.Fatal("expected error on SyncMetadata with cancelled context")
	}

	// Test Create
	user := &store.User{Id: "u_cancel", Name: "Cancel", Email: "cancel@test.com"}
	err = userRepo.Create(ctx, tr, dir, user)
	if err == nil {
		t.Fatal("expected error on CreateUser with cancelled context")
	}

	// Test Get
	_, err = userRepo.Get(ctx, tr, dir, "u_cancel")
	if err == nil {
		t.Fatal("expected error on GetUser with cancelled context")
	}

	// Test Set
	err = userRepo.Set(ctx, tr, dir, user)
	if err == nil {
		t.Fatal("expected error on SetUser with cancelled context")
	}

	// Test Delete
	err = userRepo.Delete(ctx, tr, dir, "u_cancel")
	if err == nil {
		t.Fatal("expected error on DeleteUser with cancelled context")
	}

	// Test List
	_, err = userRepo.ListUser(ctx, tr, dir, store.UserPaginationOptions{Limit: 10})
	if err == nil {
		t.Fatal("expected error on ListUser with cancelled context")
	}
}
