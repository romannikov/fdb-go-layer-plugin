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

	// Test Insert
	user := &store.User{Id: "u_cancel", Name: "Cancel", Email: "cancel@test.com"}
	err := userRepo.Insert(ctx, tx, user)
	if err == nil {
		t.Fatal("expected error on Insert with cancelled context")
	}

	// Test ReadRow
	_, err = userRepo.ReadRow(ctx, tx, fdblayer.Key{"u_cancel"})
	if err == nil {
		t.Fatal("expected error on ReadRow with cancelled context")
	}

	// Test Upsert
	err = userRepo.Upsert(ctx, tx, user)
	if err == nil {
		t.Fatal("expected error on Upsert with cancelled context")
	}

	// Test Delete
	err = userRepo.Delete(ctx, tx, fdblayer.Key{"u_cancel"})
	if err == nil {
		t.Fatal("expected error on Delete with cancelled context")
	}

	// Test Scan
	_, err = userRepo.Scan(ctx, tx, fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 10})
	if err == nil {
		t.Fatal("expected error on Scan with cancelled context")
	}

	// Test ScanIndex
	_, err = userRepo.ScanIndex(ctx, tx, store.UserByEmail, fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 10})
	if err == nil {
		t.Fatal("expected error on ScanIndex with cancelled context")
	}
}
