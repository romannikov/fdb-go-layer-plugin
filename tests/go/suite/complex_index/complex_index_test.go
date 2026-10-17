package complex_index_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"google.golang.org/protobuf/proto"

	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	tests "github.com/romannikov/fdb-layer/tests/go"
	"github.com/romannikov/fdb-layer/tests/go/store"
)

func init() {
	fdb.MustAPIVersion(710)
}

func TestFanOutIndex(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	postRepo := store.NewPostRepository(dir)

	post := &store.Post{
		Id:   "post1",
		Tags: []string{"tag1", "tag2", "tag3"},
	}

	err := postRepo.Insert(ctx, tr, post)
	if err != nil {
		t.Fatal(err)
	}

	// Verify index entries in MockKV using the compile-time TypeID constant
	for _, tag := range post.Tags {
		indexKey := dir.Pack(tuple.Tuple{store.PostTypeID, fdblayer.IndexNamespace, int64(1), tag, post.Id})
		if !kv.HasKey(indexKey) {
			t.Errorf("Missing index entry for tag %s", tag)
		}
	}
}

func TestFanOutIndex_UpsertDeltaUpdates(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	postRepo := store.NewPostRepository(dir)

	post := &store.Post{
		Id:   "post1",
		Tags: []string{"tag1", "tag2", "tag3"},
	}

	if err := postRepo.Insert(ctx, tr, post); err != nil {
		t.Fatal(err)
	}

	tr.ResetLog()
	// Update Tags: remove "tag1", keep "tag2" and "tag3", add "tag4"
	updatedPost := &store.Post{
		Id:   "post1",
		Tags: []string{"tag2", "tag3", "tag4"},
	}
	if err := postRepo.Upsert(ctx, tr, updatedPost); err != nil {
		t.Fatal(err)
	}

	// Only "tag1" should have been cleared (1 Clear call instead of 3)
	if len(tr.ClearCalls) != 1 {
		t.Fatalf("expected exactly 1 Clear call for removed tag1, got %d", len(tr.ClearCalls))
	}
	// Only primary record + "tag4" index should have been set (2 Set calls instead of 4)
	if len(tr.SetCalls) != 2 {
		t.Fatalf("expected exactly 2 Set calls (primary key + new tag4 index), got %d", len(tr.SetCalls))
	}

	removedKey := dir.Pack(tuple.Tuple{store.PostTypeID, fdblayer.IndexNamespace, int64(1), "tag1", post.Id})
	if kv.HasKey(removedKey) {
		t.Errorf("Removed tag1 index entry still present")
	}

	for _, tag := range updatedPost.Tags {
		indexKey := dir.Pack(tuple.Tuple{store.PostTypeID, fdblayer.IndexNamespace, int64(1), tag, post.Id})
		if !kv.HasKey(indexKey) {
			t.Errorf("Missing index entry for tag %s", tag)
		}
	}
}

func TestVersionstampedPrimaryKey(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	taskRepo := store.NewTaskMessageRepository(dir)

	task := &store.TaskMessage{
		QueueName: "email_queue",
		ShardId:   1,
		Payload:   []byte("send email"),
	}

	err := taskRepo.Enqueue(ctx, tr, task)
	if err != nil {
		t.Fatal(err)
	}

	// The mock transaction's SetVersionstampedKey replaces the incomplete versionstamp
	// placeholder with dummyVS transaction bytes.
	dummyVS := tuple.Versionstamp{
		TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		UserVersion:        0,
	}
	expectedKey := dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "email_queue", dummyVS, uint64(1)})

	if !kv.HasKey(expectedKey) {
		t.Fatalf("Expected key not found in mock store: %v", expectedKey)
	}

	// Second enqueue in the same transaction with the same QueueName and ShardId
	// passes WithUserVersion(1) so it does not overwrite the first item.
	task2 := &store.TaskMessage{
		QueueName: "email_queue",
		ShardId:   1,
		Payload:   []byte("send second email"),
	}
	if err := taskRepo.Enqueue(ctx, tr, task2, fdblayer.WithUserVersion(1)); err != nil {
		t.Fatal(err)
	}
	dummyVS2 := tuple.Versionstamp{
		TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		UserVersion:        1,
	}
	expectedKey2 := dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "email_queue", dummyVS2, uint64(1)})
	if !kv.HasKey(expectedKey2) {
		t.Fatalf("Expected second versionstamped key with UserVersion=1 not found in mock store: %v", expectedKey2)
	}

	// Invalid partition key arity/type must return ErrInvalidKey
	if _, err := taskRepo.Dequeue(ctx, tr, fdblayer.Key{"email_queue", "extra"}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on Dequeue arity mismatch, got %v", err)
	}
	if _, err := taskRepo.DequeueBatch(ctx, tr, fdblayer.Key{123}, 2); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey on DequeueBatch type mismatch, got %v", err)
	}

	// Verify Dequeue works in MockTransaction via fdblayer.GetRange
	d1, err := taskRepo.Dequeue(ctx, tr, fdblayer.Key{"email_queue"})
	if err != nil {
		t.Fatalf("Dequeue 1 failed: %v", err)
	}
	if d1 == nil || string(d1.Payload) != "send email" || len(d1.Versionstamp) != 12 {
		t.Fatalf("unexpected Dequeue 1 result: %+v", d1)
	}
	d2, err := taskRepo.Dequeue(ctx, tr, fdblayer.Key{"email_queue"})
	if err != nil {
		t.Fatalf("Dequeue 2 failed: %v", err)
	}
	if d2 == nil || string(d2.Payload) != "send second email" || len(d2.Versionstamp) != 12 {
		t.Fatalf("unexpected Dequeue 2 result: %+v", d2)
	}
	dEmpty, err := taskRepo.Dequeue(ctx, tr, fdblayer.Key{"email_queue"})
	if err != nil || dEmpty != nil {
		t.Fatalf("expected empty queue after 2 dequeues, got %+v, err=%v", dEmpty, err)
	}
}

func TestOrder_CompoundPK_CompoundIndex_BytesIndexes_NestedTypes(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	orderRepo := store.NewOrderRepository(dir)

	order1 := &store.Order{
		TenantId:         "acme",
		OrderSeq:         101,
		Status:           store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt:        1700000000,
		ReceiptHash:      []byte{0xAA, 0xBB, 0x01},
		AttachmentHashes: [][]byte{{0x10, 0x20}, {0x30, 0x40}},
		Priority:         store.Order_PRIORITY_HIGH,
		Address: &store.Order_ShippingAddress{
			City:    "Zurich",
			Country: "Switzerland",
		},
		History: []*store.Order_ShippingAddress{
			{City: "Basel", Country: "Switzerland"},
		},
	}

	if err := orderRepo.Insert(ctx, tr, order1); err != nil {
		t.Fatalf("Insert order1 failed: %v", err)
	}

	// Verify explicit index ID 1001 in MockKV for compound index (status, created_at)
	compoundIdxKey := dir.Pack(tuple.Tuple{
		store.OrderTypeID,
		fdblayer.IndexNamespace,
		int64(1001),
		int64(store.OrderStatus_ORDER_STATUS_PENDING),
		int64(1700000000),
		"acme",
		int64(101),
	})
	if !kv.HasKey(compoundIdxKey) {
		t.Fatalf("expected explicit index ID 1001 key in MockKV")
	}

	// 1. ReadRow by compound primary key
	pk := fdblayer.Key{"acme", int32(101)}
	got, err := orderRepo.ReadRow(ctx, tr, pk)
	if err != nil {
		t.Fatalf("ReadRow order1 failed: %v", err)
	}
	if got.Priority != store.Order_PRIORITY_HIGH || got.Address.GetCity() != "Zurich" || got.Address.GetCountry() != "Switzerland" || len(got.History) != 1 {
		t.Fatalf("unexpected nested fields on Order: %+v", got)
	}

	// 2. Query by compound secondary index (status, created_at)
	byStatus, err := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusCreatedAt, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING, int64(1700000000)))
	if err != nil || len(byStatus.Items) != 1 || byStatus.Items[0].OrderSeq != 101 {
		t.Fatalf("ScanIndex(OrderByStatusCreatedAt) failed: got %+v, err=%v", byStatus, err)
	}

	// 3. Query by scalar bytes secondary index (receipt_hash)
	byReceipt, err := orderRepo.ScanIndex(ctx, tr, store.OrderByReceiptHash, fdblayer.Prefix([]byte{0xAA, 0xBB, 0x01}))
	if err != nil || len(byReceipt.Items) != 1 || byReceipt.Items[0].OrderSeq != 101 {
		t.Fatalf("ScanIndex(OrderByReceiptHash) failed: got %+v, err=%v", byReceipt, err)
	}

	// 4. Query by fan-out repeated bytes secondary index (attachment_hashes)
	byAttach, err := orderRepo.ScanIndex(ctx, tr, store.OrderByAttachmentHashes, fdblayer.Prefix([]byte{0x10, 0x20}))
	if err != nil || len(byAttach.Items) != 1 || byAttach.Items[0].OrderSeq != 101 {
		t.Fatalf("ScanIndex(OrderByAttachmentHashes) failed: got %+v, err=%v", byAttach, err)
	}

	// 5. Upsert: update Status, ReceiptHash, and AttachmentHashes (remove {0x10,0x20}, keep {0x30,0x40}, add {0x50,0x60})
	updatedOrder := &store.Order{
		TenantId:         "acme",
		OrderSeq:         101,
		Status:           store.OrderStatus_ORDER_STATUS_SHIPPED,
		CreatedAt:        1700000000,
		ReceiptHash:      []byte{0xCC, 0xDD, 0x02},
		AttachmentHashes: [][]byte{{0x30, 0x40}, {0x50, 0x60}},
		Priority:         store.Order_PRIORITY_LOW,
	}
	if err := orderRepo.Upsert(ctx, tr, updatedOrder); err != nil {
		t.Fatalf("Upsert updatedOrder failed: %v", err)
	}

	// Old compound index, old receipt hash, and removed attachment hash must return 0 results
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusCreatedAt, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING, int64(1700000000))); len(res.Items) != 0 {
		t.Fatalf("stale compound index after Upsert: %+v", res)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByReceiptHash, fdblayer.Prefix([]byte{0xAA, 0xBB, 0x01})); len(res.Items) != 0 {
		t.Fatalf("stale receipt_hash index after Upsert: %+v", res)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByAttachmentHashes, fdblayer.Prefix([]byte{0x10, 0x20})); len(res.Items) != 0 {
		t.Fatalf("stale attachment_hashes index after Upsert: %+v", res)
	}

	// New index entries must resolve
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusCreatedAt, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_SHIPPED, int64(1700000000))); len(res.Items) != 1 {
		t.Fatalf("missing updated compound index after Upsert: %+v", res)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByReceiptHash, fdblayer.Prefix([]byte{0xCC, 0xDD, 0x02})); len(res.Items) != 1 {
		t.Fatalf("missing updated receipt_hash index after Upsert: %+v", res)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByAttachmentHashes, fdblayer.Prefix([]byte{0x50, 0x60})); len(res.Items) != 1 {
		t.Fatalf("missing new attachment_hashes index after Upsert: %+v", res)
	}

	// 6. ReadRows and Delete with compound primary key
	batchRes, err := orderRepo.ReadRows(ctx, tr, []fdblayer.Key{pk})
	if err != nil || len(batchRes) != 1 {
		t.Fatalf("ReadRows failed: got %+v, err=%v", batchRes, err)
	}

	if err := orderRepo.Delete(ctx, tr, pk); err != nil {
		t.Fatalf("Delete order1 failed: %v", err)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusCreatedAt, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_SHIPPED, int64(1700000000))); len(res.Items) != 0 {
		t.Fatalf("compound index not cleared after Delete: %+v", res)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByAttachmentHashes, fdblayer.Prefix([]byte{0x30, 0x40})); len(res.Items) != 0 {
		t.Fatalf("fan-out bytes index not cleared after Delete: %+v", res)
	}
}

func TestAuditLog_NonQueueVersionstampPK(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()

	auditRepo := store.NewAuditLogRepository(dir)

	// 1. Insert two AuditLog entities in the same transaction with initial Views,
	// standard index (Actor), and fan-out index (Tags).
	if err := auditRepo.Insert(ctx, tr, &store.AuditLog{
		Actor: "alice",
		Tags:  []string{"auth", "login"},
		Views: 10,
	}); err != nil {
		t.Fatalf("Insert AuditLog 1 failed: %v", err)
	}
	if err := auditRepo.Insert(ctx, tr, &store.AuditLog{
		Actor: "bob",
		Tags:  []string{"auth", "logout"},
		Views: 5,
	}, fdblayer.WithUserVersion(1)); err != nil {
		t.Fatalf("Insert AuditLog 2 failed: %v", err)
	}

	// 2. Scan must unpack the 12-byte versionstamp from the primary key into LogId
	// and populate the Views field.
	listed, err := auditRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithLimit(10))
	if err != nil || len(listed.Items) != 2 {
		t.Fatalf("Scan AuditLog expected 2 items, got %+v, err=%v", listed, err)
	}
	firstLogID := listed.Items[0].LogId
	secondLogID := listed.Items[1].LogId
	if len(firstLogID) != 12 || len(secondLogID) != 12 || bytes.Equal(firstLogID, secondLogID) {
		t.Fatalf("expected two distinct 12-byte LogIds, got %x and %x", firstLogID, secondLogID)
	}
	if listed.Items[0].Views != 10 || listed.Items[1].Views != 5 {
		t.Fatalf("expected Views 10 and 5 from Scan AuditLog, got %d and %d", listed.Items[0].Views, listed.Items[1].Views)
	}

	// 3. ReadRow by log_id using the unpacked 12-byte versionstamp
	got, err := auditRepo.ReadRow(ctx, tr, fdblayer.Key{firstLogID})
	if err != nil {
		t.Fatalf("ReadRow AuditLog by versionstamp failed: %v", err)
	}
	if got.Actor != "alice" || !bytes.Equal(got.LogId, firstLogID) || got.Views != 10 || len(got.Tags) != 2 {
		t.Fatalf("unexpected AuditLog from ReadRow: %+v", got)
	}

	// 4. Query by standard secondary index (AuditLogByActor)
	byActor, err := auditRepo.ScanIndex(ctx, tr, store.AuditLogByActor, fdblayer.Prefix("alice"))
	if err != nil || len(byActor.Items) != 1 {
		t.Fatalf("ScanIndex(AuditLogByActor, 'alice') expected 1 item, got %+v, err=%v", byActor, err)
	}
	if !bytes.Equal(byActor.Items[0].LogId, firstLogID) || byActor.Items[0].Views != 10 {
		t.Fatalf("ScanIndex(AuditLogByActor, 'alice') did not populate LogId/Views: %+v", byActor.Items[0])
	}

	// 5. Query by fan-out secondary index (AuditLogByTags)
	byTagAuth, err := auditRepo.ScanIndex(ctx, tr, store.AuditLogByTags, fdblayer.Prefix("auth"))
	if err != nil || len(byTagAuth.Items) != 2 {
		t.Fatalf("ScanIndex(AuditLogByTags, 'auth') expected 2 items, got %+v, err=%v", byTagAuth, err)
	}
	if !bytes.Equal(byTagAuth.Items[0].LogId, firstLogID) || !bytes.Equal(byTagAuth.Items[1].LogId, secondLogID) {
		t.Fatalf("ScanIndex(AuditLogByTags, 'auth') did not populate LogIds: %+v, %+v", byTagAuth.Items[0], byTagAuth.Items[1])
	}

	// 6. ReadRows with 12-byte versionstamp primary keys
	batchRes, err := auditRepo.ReadRows(ctx, tr, []fdblayer.Key{{firstLogID}, {secondLogID}})
	if err != nil || len(batchRes) != 2 {
		t.Fatalf("ReadRows AuditLog expected 2 items, got %+v, err=%v", batchRes, err)
	}
	for _, item := range batchRes {
		if len(item.LogId) != 12 {
			t.Fatalf("ReadRows did not populate 12-byte LogId: %+v", item)
		}
		if item.Actor == "alice" && item.Views != 10 {
			t.Fatalf("ReadRows expected Views=10 for alice, got %d", item.Views)
		}
	}

	// 7. Upsert on existing entity updates in-place (preserving LogId) and updates indexes
	if err := auditRepo.Upsert(ctx, tr, &store.AuditLog{
		LogId: firstLogID,
		Actor: "alice_admin",
		Tags:  []string{"auth", "elevated"},
		Views: 17,
	}); err != nil {
		t.Fatalf("Upsert existing AuditLog failed: %v", err)
	}
	gotAfterSet, err := auditRepo.ReadRow(ctx, tr, fdblayer.Key{firstLogID})
	if err != nil || gotAfterSet.Actor != "alice_admin" || gotAfterSet.Views != 17 {
		t.Fatalf("expected updated Actor='alice_admin' and Views=17 after Upsert, got %+v, err=%v", gotAfterSet, err)
	}
	if oldIdx, _ := auditRepo.ScanIndex(ctx, tr, store.AuditLogByActor, fdblayer.Prefix("alice")); len(oldIdx.Items) != 0 {
		t.Fatalf("expected old Actor index 'alice' cleared after Upsert, got %+v", oldIdx)
	}
	if newIdx, _ := auditRepo.ScanIndex(ctx, tr, store.AuditLogByActor, fdblayer.Prefix("alice_admin")); len(newIdx.Items) != 1 || !bytes.Equal(newIdx.Items[0].LogId, firstLogID) {
		t.Fatalf("expected new Actor index 'alice_admin' after Upsert, got %+v", newIdx)
	}
	if oldTag, _ := auditRepo.ScanIndex(ctx, tr, store.AuditLogByTags, fdblayer.Prefix("login")); len(oldTag.Items) != 0 {
		t.Fatalf("expected removed tag 'login' cleared after Upsert, got %+v", oldTag)
	}
	if newTag, _ := auditRepo.ScanIndex(ctx, tr, store.AuditLogByTags, fdblayer.Prefix("elevated")); len(newTag.Items) != 1 || !bytes.Equal(newTag.Items[0].LogId, firstLogID) {
		t.Fatalf("expected added tag 'elevated' indexed after Upsert, got %+v", newTag)
	}

	// 8. Nil entity and malformed non-12-byte LogId validation checks
	if err := auditRepo.Insert(ctx, tr, nil); err == nil {
		t.Fatal("expected Insert(nil) to return error")
	}
	if err := auditRepo.Upsert(ctx, tr, nil); err == nil {
		t.Fatal("expected Upsert(nil) to return error")
	}
	if _, err := auditRepo.ReadRow(ctx, tr, fdblayer.Key{[]byte{1, 2, 3}}); err == nil {
		t.Fatal("expected ReadRow with 3-byte LogId to return error")
	}
	if err := auditRepo.Delete(ctx, tr, fdblayer.Key{[]byte{1, 2, 3}}); err == nil {
		t.Fatal("expected Delete with 3-byte LogId to return error")
	}
	if _, err := auditRepo.ReadRows(ctx, tr, []fdblayer.Key{{firstLogID}, {[]byte{1, 2, 3}}}); err == nil {
		t.Fatal("expected ReadRows with 3-byte LogId to return error")
	}
	if err := auditRepo.Upsert(ctx, tr, &store.AuditLog{
		LogId: []byte{1, 2, 3},
		Actor: "malformed",
	}); err == nil {
		t.Fatalf("expected Upsert with 3-byte LogId to return error")
	}
	incompleteVS := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01}
	if err := auditRepo.Upsert(ctx, tr, &store.AuditLog{
		LogId: incompleteVS,
		Actor: "incomplete",
	}); !errors.Is(err, fdblayer.ErrInvalidKey) {
		t.Fatalf("expected Upsert with 12-byte incomplete versionstamp to return ErrInvalidKey, got %v", err)
	}
	if err := auditRepo.Upsert(ctx, tr, &store.AuditLog{
		Actor: "charlie",
		Tags:  []string{"audit"},
		Views: 3,
	}, fdblayer.WithUserVersion(2)); err != nil {
		t.Fatalf("Upsert new AuditLog (WithUserVersion(2)) failed: %v", err)
	}
	byCharlie, err := auditRepo.ScanIndex(ctx, tr, store.AuditLogByActor, fdblayer.Prefix("charlie"))
	if err != nil || len(byCharlie.Items) != 1 || len(byCharlie.Items[0].LogId) != 12 || byCharlie.Items[0].Views != 3 {
		t.Fatalf("expected Upsert with WithUserVersion(2) to create indexed AuditLog, got %+v, err=%v", byCharlie, err)
	}

	// 9. Scan pagination with Limit=1 and raw []byte Cursor
	page1, err := auditRepo.Scan(ctx, tr, fdblayer.AllKeys(), fdblayer.WithLimit(1))
	if err != nil || len(page1.Items) != 1 || !page1.HasMore {
		t.Fatalf("expected page1 with 1 item and HasMore=true, got %+v, err=%v", page1, err)
	}
	page2, err := auditRepo.Scan(ctx, tr, fdblayer.AllKeys(),
		fdblayer.WithCursor(fdblayer.Key{secondLogID}),
		fdblayer.WithLimit(1),
	)
	if err != nil || len(page2.Items) != 1 || !bytes.Equal(page2.Items[0].LogId, secondLogID) || !page2.HasMore {
		t.Fatalf("expected page2 starting at secondLogID ([]byte) with HasMore=true, got %+v, err=%v", page2, err)
	}

	// 10. Delete by log_id clears primary data, standard index, and fan-out index
	if err := auditRepo.Delete(ctx, tr, fdblayer.Key{firstLogID}); err != nil {
		t.Fatalf("Delete AuditLog by versionstamp failed: %v", err)
	}
	if _, err := auditRepo.ReadRow(ctx, tr, fdblayer.Key{firstLogID}); err == nil {
		t.Fatalf("expected ReadRow after Delete to fail")
	}
	if idxAfterDel, _ := auditRepo.ScanIndex(ctx, tr, store.AuditLogByActor, fdblayer.Prefix("alice_admin")); len(idxAfterDel.Items) != 0 {
		t.Fatalf("expected Actor index cleared after Delete, got %+v", idxAfterDel)
	}
	if tagAfterDel, _ := auditRepo.ScanIndex(ctx, tr, store.AuditLogByTags, fdblayer.Prefix("elevated")); len(tagAfterDel.Items) != 0 {
		t.Fatalf("expected fan-out tag 'elevated' cleared after Delete, got %+v", tagAfterDel)
	}
	if sharedTagAfterDel, _ := auditRepo.ScanIndex(ctx, tr, store.AuditLogByTags, fdblayer.Prefix("auth")); len(sharedTagAfterDel.Items) != 1 || sharedTagAfterDel.Items[0].Actor != "bob" {
		t.Fatalf("expected only bob on 'auth' tag after Delete, got %+v", sharedTagAfterDel)
	}
	primaryKey := dir.Pack(tuple.Tuple{store.AuditLogTypeID, fdblayer.DataNamespace, fdblayer.BytesToVersionstamp(firstLogID)})
	if kv.HasKey(primaryKey) {
		t.Fatalf("expected primary data key cleared after Delete")
	}
}

func TestFanOutIndex_InsertDeduplicatesRepeatedTags(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	postRepo := store.NewPostRepository(dir)

	tr.ResetLog()
	post := &store.Post{
		Id:   "post_dup",
		Tags: []string{"go", "go", "fdb", "go"},
	}
	if err := postRepo.Insert(ctx, tr, post); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	// 1 primary record write + 2 unique tag index writes ("go", "fdb") = 3 SetCalls total
	if len(tr.SetCalls) != 3 {
		t.Fatalf("expected 3 Set calls (1 primary + 2 unique tags), got %d", len(tr.SetCalls))
	}
}

func TestQueue_EnqueueZerosVersionstampPayload_AndDequeueBatch(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	var taskRepo fdblayer.QueueRepository[*store.TaskMessage] = store.NewTaskMessageRepository(dir)

	callerVS := []byte{0x11, 0x22}
	msg1 := &store.TaskMessage{
		QueueName:    "q_batch",
		ShardId:      1,
		Versionstamp: callerVS,
		Payload:      []byte("item-1"),
	}
	if err := taskRepo.Enqueue(ctx, tr, msg1); err != nil {
		t.Fatalf("Enqueue 1 failed: %v", err)
	}
	// Caller's struct must not be mutated
	if !bytes.Equal(msg1.Versionstamp, callerVS) {
		t.Fatalf("Enqueue mutated caller's Versionstamp slice: %x", msg1.Versionstamp)
	}

	// Raw stored protobuf value in FDB must have Versionstamp zeroed
	dummyVS := tuple.Versionstamp{
		TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		UserVersion:        0,
	}
	rawKey := dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "q_batch", dummyVS, uint64(1)})
	rawVal := kv.Get(rawKey)
	rawMsg := &store.TaskMessage{}
	if err := proto.Unmarshal(rawVal, rawMsg); err != nil {
		t.Fatalf("unmarshal raw TaskMessage failed: %v", err)
	}
	if len(rawMsg.Versionstamp) != 0 {
		t.Fatalf("expected Enqueue to zero Versionstamp in stored protobuf payload, got %x", rawMsg.Versionstamp)
	}

	// Enqueue 2 more items with explicit WithUserVersion and test DequeueBatch
	_ = taskRepo.Enqueue(ctx, tr, &store.TaskMessage{QueueName: "q_batch", ShardId: 2, Payload: []byte("item-2")}, fdblayer.WithUserVersion(1))
	_ = taskRepo.Enqueue(ctx, tr, &store.TaskMessage{QueueName: "q_batch", ShardId: 3, Payload: []byte("item-3")}, fdblayer.WithUserVersion(2))

	batch, err := taskRepo.DequeueBatch(ctx, tr, fdblayer.Key{"q_batch"}, 2)
	if err != nil || len(batch) != 2 {
		t.Fatalf("DequeueBatch(2) expected 2 items, got %+v, err=%v", batch, err)
	}
	if string(batch[0].Payload) != "item-1" || string(batch[1].Payload) != "item-2" || len(batch[0].Versionstamp) != 12 {
		t.Fatalf("unexpected DequeueBatch(2) items: %+v", batch)
	}

	rem, err := taskRepo.DequeueBatch(ctx, tr, fdblayer.Key{"q_batch"}, 5)
	if err != nil || len(rem) != 1 || string(rem[0].Payload) != "item-3" {
		t.Fatalf("DequeueBatch remaining expected 1 item ('item-3'), got %+v, err=%v", rem, err)
	}
}

func TestOrder_PrefixScan_RangeScan_ReverseScan_AndReadRowsNormalization(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	orderRepo := store.NewOrderRepository(dir)

	for _, seq := range []int32{1, 2, 3} {
		_ = orderRepo.Insert(ctx, tr, &store.Order{
			TenantId: "acme",
			OrderSeq: seq,
			Status:   store.OrderStatus_ORDER_STATUS_PENDING,
		})
	}
	_ = orderRepo.Insert(ctx, tr, &store.Order{
		TenantId: "other",
		OrderSeq: 1,
		Status:   store.OrderStatus_ORDER_STATUS_PENDING,
	})

	// 1. Scan with Prefix("acme") and Reverse=true
	acmeRev, err := orderRepo.Scan(ctx, tr, fdblayer.Prefix("acme"),
		fdblayer.WithLimit(2),
		fdblayer.WithReverse(),
	)
	if err != nil || len(acmeRev.Items) != 2 || !acmeRev.HasMore {
		t.Fatalf("unexpected Scan(Prefix:acme, Reverse:true) page 1: %+v, err=%v", acmeRev, err)
	}
	if acmeRev.Items[0].OrderSeq != 3 || acmeRev.Items[1].OrderSeq != 2 {
		t.Fatalf("expected OrderSeq [3, 2], got [%d, %d]", acmeRev.Items[0].OrderSeq, acmeRev.Items[1].OrderSeq)
	}

	acmeRev2, err := orderRepo.Scan(ctx, tr, fdblayer.Prefix("acme"),
		fdblayer.WithCursor(acmeRev.NextCursor),
		fdblayer.WithLimit(2),
		fdblayer.WithReverse(),
	)
	if err != nil || len(acmeRev2.Items) != 1 || acmeRev2.HasMore || acmeRev2.Items[0].OrderSeq != 1 {
		t.Fatalf("unexpected Scan(Prefix:acme, Reverse:true) page 2: %+v, err=%v", acmeRev2, err)
	}

	// 1b. Scan with Range([{"acme", 1}, {"acme", 3})) returns OrderSeq 1 and 2
	acmeRange, err := orderRepo.Scan(ctx, tr, fdblayer.Range(fdblayer.Key{"acme", int32(1)}, fdblayer.Key{"acme", int32(3)}))
	if err != nil || len(acmeRange.Items) != 2 || acmeRange.Items[0].OrderSeq != 1 || acmeRange.Items[1].OrderSeq != 2 {
		t.Fatalf("unexpected Scan(Range:[1,3)) result: %+v, err=%v", acmeRange, err)
	}

	// 2. ReadRows with compound primary key (int32 normalized automatically)
	batchByPK, err := orderRepo.ReadRows(ctx, tr, []fdblayer.Key{
		{"acme", int32(1)},
		{"acme", int32(3)},
	})
	if err != nil || len(batchByPK) != 2 {
		t.Fatalf("ReadRows failed: %+v, err=%v", batchByPK, err)
	}

	// 3. Out-of-bounds Cursor with Prefix("acme") must be clamped to prefix range
	// without leaking records from "other" or causing an inverted range.
	clampedForwardBefore, err := orderRepo.Scan(ctx, tr, fdblayer.Prefix("acme"),
		fdblayer.WithCursor(fdblayer.Key{"aaa", int64(1)}),
		fdblayer.WithLimit(10),
	)
	if err != nil || len(clampedForwardBefore.Items) != 3 {
		t.Fatalf("expected Cursor<'acme' to be clamped to 'acme' start (3 items), got %+v, err=%v", clampedForwardBefore, err)
	}
	clampedForwardAfter, err := orderRepo.Scan(ctx, tr, fdblayer.Prefix("acme"),
		fdblayer.WithCursor(fdblayer.Key{"zzz", int64(1)}),
		fdblayer.WithLimit(10),
	)
	if err != nil || len(clampedForwardAfter.Items) != 0 {
		t.Fatalf("expected Cursor>'acme' to return 0 items without inverted range, got %+v, err=%v", clampedForwardAfter, err)
	}
	clampedReverseBefore, err := orderRepo.Scan(ctx, tr, fdblayer.Prefix("acme"),
		fdblayer.WithCursor(fdblayer.Key{"aaa", int64(1)}),
		fdblayer.WithLimit(10),
		fdblayer.WithReverse(),
	)
	if err != nil || len(clampedReverseBefore.Items) != 0 {
		t.Fatalf("expected Reverse=true with Cursor<'acme' to return 0 items, got %+v, err=%v", clampedReverseBefore, err)
	}
	clampedReverseAfter, err := orderRepo.Scan(ctx, tr, fdblayer.Prefix("acme"),
		fdblayer.WithCursor(fdblayer.Key{"zzz", int64(1)}),
		fdblayer.WithLimit(10),
		fdblayer.WithReverse(),
	)
	if err != nil || len(clampedReverseAfter.Items) != 3 {
		t.Fatalf("expected Reverse=true with Cursor>'acme' to clamp to 'acme' end (3 items), got %+v, err=%v", clampedReverseAfter, err)
	}
}

func TestOrder_CompositeIndexPrefixAndRangeQueries(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	orderRepo := store.NewOrderRepository(dir)

	// Insert 3 PENDING orders with distinct CreatedAt timestamps and 1 SHIPPED order
	for i, ts := range []int64{1000, 2000, 3000} {
		if err := orderRepo.Insert(ctx, tr, &store.Order{
			TenantId:  "acme",
			OrderSeq:  int32(i + 1),
			Status:    store.OrderStatus_ORDER_STATUS_PENDING,
			CreatedAt: ts,
		}); err != nil {
			t.Fatalf("Insert pending order %d failed: %v", i+1, err)
		}
	}
	if err := orderRepo.Insert(ctx, tr, &store.Order{
		TenantId:  "acme",
		OrderSeq:  99,
		Status:    store.OrderStatus_ORDER_STATUS_SHIPPED,
		CreatedAt: 1500,
	}); err != nil {
		t.Fatalf("Insert shipped order failed: %v", err)
	}

	// 1. ScanIndex with partial prefix fdblayer.Prefix(PENDING) returns all 3 PENDING orders ordered by CreatedAt
	pendingRes, err := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusCreatedAt, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING))
	if err != nil || len(pendingRes.Items) != 3 {
		t.Fatalf("ScanIndex(OrderByStatusCreatedAt, Prefix(PENDING)) expected 3 orders, got %+v, err=%v", pendingRes, err)
	}
	pending := pendingRes.Items
	if pending[0].CreatedAt != 1000 || pending[1].CreatedAt != 2000 || pending[2].CreatedAt != 3000 {
		t.Fatalf("unexpected CreatedAt order: [%d, %d, %d]", pending[0].CreatedAt, pending[1].CreatedAt, pending[2].CreatedAt)
	}

	// 1b. ScanIndex with half-open Range([ {PENDING, 1000}, {PENDING, 3000} )) returns CreatedAt 1000 and 2000
	rangeRes, err := orderRepo.ScanIndex(ctx, tr,
		store.OrderByStatusCreatedAt,
		fdblayer.Range(
			fdblayer.Key{store.OrderStatus_ORDER_STATUS_PENDING, int64(1000)},
			fdblayer.Key{store.OrderStatus_ORDER_STATUS_PENDING, int64(3000)},
		),
	)
	if err != nil || len(rangeRes.Items) != 2 || rangeRes.Items[0].CreatedAt != 1000 || rangeRes.Items[1].CreatedAt != 2000 {
		t.Fatalf("ScanIndex(OrderByStatusCreatedAt, Range([1000, 3000))) expected 2 orders, got %+v, err=%v", rangeRes, err)
	}

	// 2. ScanIndex forward pagination (Limit=2) across different CreatedAt values
	fwd1, err := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusCreatedAt, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING),
		fdblayer.WithLimit(2),
	)
	if err != nil || len(fwd1.Items) != 2 || !fwd1.HasMore {
		t.Fatalf("ScanIndex forward page 1 failed: %+v, err=%v", fwd1, err)
	}
	if fwd1.Items[0].OrderSeq != 1 || fwd1.Items[1].OrderSeq != 2 {
		t.Fatalf("expected OrderSeq [1, 2], got [%d, %d]", fwd1.Items[0].OrderSeq, fwd1.Items[1].OrderSeq)
	}

	fwd2, err := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusCreatedAt, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING),
		fdblayer.WithCursor(fwd1.NextCursor),
		fdblayer.WithLimit(2),
	)
	if err != nil || len(fwd2.Items) != 1 || fwd2.HasMore || fwd2.Items[0].OrderSeq != 3 {
		t.Fatalf("ScanIndex forward page 2 failed: %+v, err=%v", fwd2, err)
	}

	// 3. ScanIndex reverse pagination (Limit=2, Reverse=true)
	rev1, err := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusCreatedAt, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING),
		fdblayer.WithLimit(2),
		fdblayer.WithReverse(),
	)
	if err != nil || len(rev1.Items) != 2 || !rev1.HasMore {
		t.Fatalf("ScanIndex reverse page 1 failed: %+v, err=%v", rev1, err)
	}
	if rev1.Items[0].OrderSeq != 3 || rev1.Items[1].OrderSeq != 2 {
		t.Fatalf("expected reverse OrderSeq [3, 2], got [%d, %d]", rev1.Items[0].OrderSeq, rev1.Items[1].OrderSeq)
	}

	rev2, err := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusCreatedAt, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING),
		fdblayer.WithCursor(rev1.NextCursor),
		fdblayer.WithLimit(2),
		fdblayer.WithReverse(),
	)
	if err != nil || len(rev2.Items) != 1 || rev2.HasMore || rev2.Items[0].OrderSeq != 1 {
		t.Fatalf("ScanIndex reverse page 2 failed: %+v, err=%v", rev2, err)
	}
}

func TestFanOutIndex_UpsertAndDeleteDeduplicateRepeatedTags(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	postRepo := store.NewPostRepository(dir)

	if err := postRepo.Insert(ctx, tr, &store.Post{
		Id:   "p_dup",
		Tags: []string{"a", "a", "b"},
	}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	tr.ResetLog()
	if err := postRepo.Upsert(ctx, tr, &store.Post{
		Id:   "p_dup",
		Tags: []string{"b", "c", "c", "c"},
	}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}
	if len(tr.ClearCalls) != 1 || len(tr.SetCalls) != 2 {
		t.Fatalf("expected 1 ClearCall and 2 SetCalls on Upsert with duplicate tags, got %d ClearCalls and %d SetCalls", len(tr.ClearCalls), len(tr.SetCalls))
	}

	tr.ResetLog()
	if err := postRepo.Delete(ctx, tr, fdblayer.Key{"p_dup"}); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if len(tr.ClearCalls) != 3 {
		t.Fatalf("expected 3 ClearCalls (1 primary + 2 unique tags) on Delete with duplicate tags, got %d", len(tr.ClearCalls))
	}
}

func TestQueue_CorruptedPayloadPreservesEntry(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	taskRepo := store.NewTaskMessageRepository(dir)

	dummyVS := tuple.Versionstamp{
		TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		UserVersion:        0,
	}
	corruptKey := dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "corrupt_q", dummyVS, uint64(1)})
	tr.Set(corruptKey, []byte{0x00, 0xff, 0xff})

	if _, err := taskRepo.Dequeue(ctx, tr, fdblayer.Key{"corrupt_q"}); err == nil {
		t.Fatal("expected unmarshal error from Dequeue on corrupted payload")
	}
	if !kv.HasKey(corruptKey) {
		t.Fatal("expected corrupted queue item to remain in store after Dequeue error")
	}

	if _, err := taskRepo.DequeueBatch(ctx, tr, fdblayer.Key{"corrupt_q"}, 5); err == nil {
		t.Fatal("expected unmarshal error from DequeueBatch on corrupted payload")
	}
	if !kv.HasKey(corruptKey) {
		t.Fatal("expected corrupted queue item to remain in store after DequeueBatch error")
	}
}

func TestSecondaryAndFanOutIndexKeysNeverMissed_InsertUpsertDelete(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	orderRepo := store.NewOrderRepository(dir)

	countOrderIndexKeys := func() int {
		prefix, err := fdb.PrefixRange(dir.Pack(tuple.Tuple{store.OrderTypeID, fdblayer.IndexNamespace}))
		if err != nil {
			t.Fatalf("PrefixRange failed: %v", err)
		}
		count := 0
		for _, kv := range tr.GetRangeSlice(prefix, fdb.RangeOptions{}) {
			if len(kv.Value) == 0 {
				count++
			}
		}
		return count
	}

	hashA := []byte{0x0A}
	hashB := []byte{0x0B}
	hashC := []byte{0x0C}
	hashD := []byte{0x0D}
	pk := fdblayer.Key{"t1", int32(7)}

	// 1. Insert with 2 attachment hashes:
	//    - 1 key for (status, created_at) [id=1001]
	//    - 1 key for (receipt_hash) [id=1002]
	//    - 2 keys for fan-out (attachment_hashes) [id=1003]
	//    - 2 keys for composite fan-out (status, attachment_hashes) [id=1004]
	//    Total = 6 index keys.
	if err := orderRepo.Insert(ctx, tr, &store.Order{
		TenantId:         "t1",
		OrderSeq:         7,
		Status:           store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt:        100,
		ReceiptHash:      []byte{0x01},
		AttachmentHashes: [][]byte{hashA, hashB, hashA}, // duplicate hashA must be deduplicated
	}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 6 {
		t.Fatalf("expected 6 index keys after Insert, got %d", got)
	}
	if res, err := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusAttachmentHashes, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING, hashA)); err != nil || len(res.Items) != 1 {
		t.Fatalf("expected 1 order for (PENDING, hashA), got %+v, err=%v", res, err)
	}
	if res, err := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusAttachmentHashes, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING, hashB)); err != nil || len(res.Items) != 1 {
		t.Fatalf("expected 1 order for (PENDING, hashB), got %+v, err=%v", res, err)
	}

	// 2. Upsert with UNCHANGED Status (PENDING) and updated AttachmentHashes ({hashB, hashC}):
	//    Delta path must remove hashA, keep hashB, and add hashC on both fan-out indexes.
	if err := orderRepo.Upsert(ctx, tr, &store.Order{
		TenantId:         "t1",
		OrderSeq:         7,
		Status:           store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt:        100,
		ReceiptHash:      []byte{0x01},
		AttachmentHashes: [][]byte{hashB, hashC},
	}); err != nil {
		t.Fatalf("Upsert (same status) failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 6 {
		t.Fatalf("expected 6 index keys after Upsert (same status), got %d", got)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusAttachmentHashes, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING, hashA)); len(res.Items) != 0 {
		t.Fatalf("expected (PENDING, hashA) cleared after Upsert, got %+v", res)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusAttachmentHashes, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING, hashC)); len(res.Items) != 1 {
		t.Fatalf("expected (PENDING, hashC) added after Upsert, got %+v", res)
	}

	// 3. Upsert with CHANGED Status (PENDING -> SHIPPED) and overlapping AttachmentHashes ({hashC, hashD}):
	//    Non-repeated field changed in composite fan-out index (status, attachment_hashes),
	//    so ALL old (PENDING, hashB) and (PENDING, hashC) keys must be cleared, and (SHIPPED, hashC) + (SHIPPED, hashD) written!
	if err := orderRepo.Upsert(ctx, tr, &store.Order{
		TenantId:         "t1",
		OrderSeq:         7,
		Status:           store.OrderStatus_ORDER_STATUS_SHIPPED,
		CreatedAt:        200,
		ReceiptHash:      []byte{0x02},
		AttachmentHashes: [][]byte{hashC, hashD},
	}); err != nil {
		t.Fatalf("Upsert (changed status) failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 6 {
		t.Fatalf("expected 6 index keys after Upsert (changed status), got %d", got)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusAttachmentHashes, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_PENDING, hashC)); len(res.Items) != 0 {
		t.Fatalf("stale composite fan-out key (PENDING, hashC) was not cleared when status changed: %+v", res)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusAttachmentHashes, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_SHIPPED, hashC)); len(res.Items) != 1 {
		t.Fatalf("missing composite fan-out key (SHIPPED, hashC) after status change: %+v", res)
	}
	if res, _ := orderRepo.ScanIndex(ctx, tr, store.OrderByStatusAttachmentHashes, fdblayer.Prefix(store.OrderStatus_ORDER_STATUS_SHIPPED, hashD)); len(res.Items) != 1 {
		t.Fatalf("missing composite fan-out key (SHIPPED, hashD) after status change: %+v", res)
	}

	// 4. Upsert with empty AttachmentHashes (nil): all fan-out keys must be cleared, leaving only the 2 standard index keys.
	if err := orderRepo.Upsert(ctx, tr, &store.Order{
		TenantId:         "t1",
		OrderSeq:         7,
		Status:           store.OrderStatus_ORDER_STATUS_SHIPPED,
		CreatedAt:        200,
		ReceiptHash:      []byte{0x02},
		AttachmentHashes: nil,
	}); err != nil {
		t.Fatalf("Upsert (empty fan-out) failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 2 {
		t.Fatalf("expected 2 standard index keys after clearing fan-out slice, got %d", got)
	}

	// 5. Delete must clear all remaining index keys, leaving 0 keys in IndexNamespace.
	if err := orderRepo.Delete(ctx, tr, pk); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 0 {
		t.Fatalf("expected 0 index keys after Delete, got %d", got)
	}

	// 6. Upsert on a non-existent record must write all standard and fan-out index keys,
	//    and Delete must still clear all index keys even if primary key fields inside the stored
	//    protobuf value were omitted.
	pk2 := fdblayer.Key{"t2", int32(9)}
	if err := orderRepo.Upsert(ctx, tr, &store.Order{
		TenantId:         "t2",
		OrderSeq:         9,
		Status:           store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt:        300,
		ReceiptHash:      []byte{0x09},
		AttachmentHashes: [][]byte{hashA, hashB},
	}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 6 {
		t.Fatalf("expected 6 index keys after Upsert, got %d", got)
	}

	// Simulate a stored protobuf value whose PK fields were omitted from the serialized body.
	rawDataKey := dir.Pack(tuple.Tuple{store.OrderTypeID, fdblayer.DataNamespace, "t2", int64(9)})
	strippedBytes, err := proto.Marshal(&store.Order{
		Status:           store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt:        300,
		ReceiptHash:      []byte{0x09},
		AttachmentHashes: [][]byte{hashA, hashB},
	})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	tr.Set(rawDataKey, strippedBytes)

	if err := orderRepo.Delete(ctx, tr, pk2); err != nil {
		t.Fatalf("Delete after PK-stripped payload failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 0 {
		t.Fatalf("expected 0 index keys after Delete of PK-stripped payload, got %d", got)
	}
	if got, err := orderRepo.ReadAggregate(ctx, tr, store.OrderCountByTenantIdStatus, fdblayer.Key{"t2", store.OrderStatus_ORDER_STATUS_PENDING}); err != nil || got != 0 {
		t.Fatalf("expected OrderCountByTenantIdStatus=0 after Delete of PK-stripped payload, got %d, err=%v", got, err)
	}
	if got, err := orderRepo.ReadAggregate(ctx, tr, store.OrderSumCreatedAtByTenantId, fdblayer.Key{"t2"}); err != nil || got != 0 {
		t.Fatalf("expected OrderSumCreatedAtByTenantId=0 after Delete of PK-stripped payload, got %d, err=%v", got, err)
	}
}

func TestFanOutAggregateAndCompositeAggregate_PostAndOrder(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	postRepo := store.NewPostRepository(dir)
	orderRepo := store.NewOrderRepository(dir)

	// 1. Fan-out aggregate COUNT on Post.Tags (with duplicate tags in input slice)
	if err := postRepo.Insert(ctx, tr, &store.Post{Id: "p1", Tags: []string{"go", "fdb", "go"}}); err != nil {
		t.Fatalf("Insert(p1) failed: %v", err)
	}
	if err := postRepo.Insert(ctx, tr, &store.Post{Id: "p2", Tags: []string{"go", "storage"}}); err != nil {
		t.Fatalf("Insert(p2) failed: %v", err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostCountByTags, fdblayer.Key{"go"}); err != nil || got != 2 {
		t.Fatalf("expected PostCountByTags('go')=2, got %d, err=%v", got, err)
	}
	if got, err := postRepo.ReadAggregate(ctx, tr, store.PostCountByTags, fdblayer.Key{"fdb"}); err != nil || got != 1 {
		t.Fatalf("expected PostCountByTags('fdb')=1, got %d, err=%v", got, err)
	}

	// Upsert p1: remove "fdb", keep "go", add "cloud"
	if err := postRepo.Upsert(ctx, tr, &store.Post{Id: "p1", Tags: []string{"go", "cloud", "cloud"}}); err != nil {
		t.Fatalf("Upsert(p1) failed: %v", err)
	}
	if got, _ := postRepo.ReadAggregate(ctx, tr, store.PostCountByTags, fdblayer.Key{"fdb"}); got != 0 {
		t.Fatalf("expected PostCountByTags('fdb')=0 after Upsert, got %d", got)
	}
	if got, _ := postRepo.ReadAggregate(ctx, tr, store.PostCountByTags, fdblayer.Key{"go"}); got != 2 {
		t.Fatalf("expected PostCountByTags('go')=2 after Upsert, got %d", got)
	}
	if got, _ := postRepo.ReadAggregate(ctx, tr, store.PostCountByTags, fdblayer.Key{"cloud"}); got != 1 {
		t.Fatalf("expected PostCountByTags('cloud')=1 after Upsert, got %d", got)
	}

	// Delete p2
	if err := postRepo.Delete(ctx, tr, fdblayer.Key{"p2"}); err != nil {
		t.Fatalf("Delete(p2) failed: %v", err)
	}
	if got, _ := postRepo.ReadAggregate(ctx, tr, store.PostCountByTags, fdblayer.Key{"go"}); got != 1 {
		t.Fatalf("expected PostCountByTags('go')=1 after Delete(p2), got %d", got)
	}

	// 2. Composite group key (tenant_id, status enum) on OrderCountByTenantIdStatus and OrderSumCreatedAtByTenantId
	if err := orderRepo.Insert(ctx, tr, &store.Order{
		TenantId:  "acme",
		OrderSeq:  1,
		Status:    store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt: 1000,
	}); err != nil {
		t.Fatalf("Insert(order 1) failed: %v", err)
	}
	if err := orderRepo.Insert(ctx, tr, &store.Order{
		TenantId:  "acme",
		OrderSeq:  2,
		Status:    store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt: 2500,
	}); err != nil {
		t.Fatalf("Insert(order 2) failed: %v", err)
	}
	if got, err := orderRepo.ReadAggregate(ctx, tr, store.OrderCountByTenantIdStatus, fdblayer.Key{"acme", store.OrderStatus_ORDER_STATUS_PENDING}); err != nil || got != 2 {
		t.Fatalf("expected OrderCountByTenantIdStatus(acme, PENDING)=2, got %d, err=%v", got, err)
	}
	if got, err := orderRepo.ReadAggregate(ctx, tr, store.OrderSumCreatedAtByTenantId, fdblayer.Key{"acme"}); err != nil || got != 3500 {
		t.Fatalf("expected OrderSumCreatedAtByTenantId(acme)=3500, got %d, err=%v", got, err)
	}
}
