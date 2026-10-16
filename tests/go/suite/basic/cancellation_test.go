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
	tx := fdblayer.NewTx(ctx, tr, dir)
	userRepo := store.NewUserRepository()

	// Test Insert
	user := &store.User{Id: "u_cancel", Name: "Cancel", Email: "cancel@test.com"}
	err := userRepo.Insert(tx, user)
	if err == nil {
		t.Fatal("expected error on Insert with cancelled context")
	}

	// Test ReadRow
	_, err = userRepo.ReadRow(tx, fdblayer.Key{"u_cancel"})
	if err == nil {
		t.Fatal("expected error on ReadRow with cancelled context")
	}

	// Test Upsert
	err = userRepo.Upsert(tx, user)
	if err == nil {
		t.Fatal("expected error on Upsert with cancelled context")
	}

	// Test Delete
	err = userRepo.Delete(tx, fdblayer.Key{"u_cancel"})
	if err == nil {
		t.Fatal("expected error on Delete with cancelled context")
	}

	// Test Scan
	_, err = userRepo.Scan(tx, fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 10})
	if err == nil {
		t.Fatal("expected error on Scan with cancelled context")
	}

	// Test ScanIndex
	_, err = userRepo.ScanIndex(tx, store.UserByEmail, fdblayer.AllKeys(), fdblayer.ScanOptions{Limit: 10})
	if err == nil {
		t.Fatal("expected error on ScanIndex with cancelled context")
	}

	// Test Store.Transact with cancelled context
	mockStore := fdblayer.NewMockStore(tr, dir)
	if err := mockStore.Transact(ctx, func(tx *fdblayer.Tx) error {
		return userRepo.Insert(tx, user)
	}); err == nil {
		t.Fatal("expected error on Store.Transact with cancelled context")
	}

	// Test Store.Transact and TransactVal with valid context
	bgCtx := context.Background()
	if err := mockStore.Transact(bgCtx, func(tx *fdblayer.Tx) error {
		return userRepo.Insert(tx, &store.User{Id: "u_store", Name: "StoreUser", Email: "store@test.com"})
	}); err != nil {
		t.Fatalf("unexpected error on Store.Transact: %v", err)
	}
	got, err := fdblayer.TransactVal(bgCtx, mockStore, func(tx *fdblayer.Tx) (*store.User, error) {
		return userRepo.ReadRow(tx, fdblayer.Key{"u_store"})
	})
	if err != nil || got.GetName() != "StoreUser" {
		t.Fatalf("unexpected result from TransactVal: got=%v, err=%v", got, err)
	}
}
