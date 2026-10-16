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
	userRepo := store.NewUserRepository(dir)

	// Test Insert
	user := &store.User{Id: "u_cancel", Name: "Cancel", Email: "cancel@test.com"}
	err := userRepo.Insert(ctx, tr, user)
	if err == nil {
		t.Fatal("expected error on Insert with cancelled context")
	}

	// Test ReadRow
	_, err = userRepo.ReadRow(ctx, tr, fdblayer.Key{"u_cancel"})
	if err == nil {
		t.Fatal("expected error on ReadRow with cancelled context")
	}

	// Test ReadRows
	_, err = userRepo.ReadRows(ctx, tr, []fdblayer.Key{{"u_cancel"}})
	if err == nil {
		t.Fatal("expected error on ReadRows with cancelled context")
	}

	// Test Upsert
	err = userRepo.Upsert(ctx, tr, user)
	if err == nil {
		t.Fatal("expected error on Upsert with cancelled context")
	}

	// Test Delete
	err = userRepo.Delete(ctx, tr, fdblayer.Key{"u_cancel"})
	if err == nil {
		t.Fatal("expected error on Delete with cancelled context")
	}

	// Test Scan
	_, err = userRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithLimit(10))
	if err == nil {
		t.Fatal("expected error on Scan with cancelled context")
	}

	// Test ScanIndex
	_, err = userRepo.ScanIndex(ctx, tr, store.UserByEmail, fdblayer.AllKeys(), fdblayer.WithLimit(10))
	if err == nil {
		t.Fatal("expected error on ScanIndex with cancelled context")
	}
}
