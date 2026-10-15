package basic_test

import (
	"context"
	"testing"

	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	tests "github.com/romannikov/fdb-layer/tests/go"
	"github.com/romannikov/fdb-layer/tests/go/store"
)

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel context immediately

	tr, dir, _ := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)
	userRepo := store.NewUserRepository()

	// Test Create
	user := &store.User{Id: "u_cancel", Name: "Cancel", Email: "cancel@test.com"}
	err := userRepo.Create(ctx, tx, user)
	if err == nil {
		t.Fatal("expected error on CreateUser with cancelled context")
	}

	// Test Get
	_, err = userRepo.Get(ctx, tx, "u_cancel")
	if err == nil {
		t.Fatal("expected error on GetUser with cancelled context")
	}

	// Test Set
	err = userRepo.Set(ctx, tx, user)
	if err == nil {
		t.Fatal("expected error on SetUser with cancelled context")
	}

	// Test Delete
	err = userRepo.Delete(ctx, tx, "u_cancel")
	if err == nil {
		t.Fatal("expected error on DeleteUser with cancelled context")
	}

	// Test List
	_, err = userRepo.List(ctx, tx, fdblayer.PaginationOptions{Limit: 10})
	if err == nil {
		t.Fatal("expected error on ListUser with cancelled context")
	}
}
