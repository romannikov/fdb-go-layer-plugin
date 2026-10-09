package complex_index_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
	tests "github.com/romannikov/fdb-layer/tests/go"
	"github.com/romannikov/fdb-layer/tests/go/store"
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
		indexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(4095142816), tag, post.Id})
		if !kv.HasKey(indexKey) {
			t.Errorf("Missing index entry for tag %s", tag)
		}
	}
}

func TestFanOutIndex_SetDeltaUpdates(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()

	postRepo := store.NewPostRepository(recordStore)

	post := &store.Post{
		Id:   "post1",
		Tags: []string{"tag1", "tag2", "tag3"},
	}

	if err := postRepo.Create(ctx, tr, dir, post); err != nil {
		t.Fatal(err)
	}

	tr.ResetLog()
	// Update Tags: remove "tag1", keep "tag2" and "tag3", add "tag4"
	updatedPost := &store.Post{
		Id:   "post1",
		Tags: []string{"tag2", "tag3", "tag4"},
	}
	if err := postRepo.Set(ctx, tr, dir, updatedPost); err != nil {
		t.Fatal(err)
	}

	typeID := recordStore.Metadata()["Post"]

	// Only "tag1" should have been cleared (1 Clear call instead of 3)
	if len(tr.ClearCalls) != 1 {
		t.Fatalf("expected exactly 1 Clear call for removed tag1, got %d", len(tr.ClearCalls))
	}
	// Only primary record + "tag4" index should have been set (2 Set calls instead of 4)
	if len(tr.SetCalls) != 2 {
		t.Fatalf("expected exactly 2 Set calls (primary key + new tag4 index), got %d", len(tr.SetCalls))
	}

	removedKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(4095142816), "tag1", post.Id})
	if kv.HasKey(removedKey) {
		t.Errorf("Removed tag1 index entry still present")
	}

	for _, tag := range updatedPost.Tags {
		indexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, int64(4095142816), tag, post.Id})
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

	err := taskRepo.Enqueue(ctx, tr, dir, task)
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
	expectedKey := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, "email_queue", dummyVS, uint64(1)})

	if !kv.HasKey(expectedKey) {
		t.Fatalf("Expected key not found in mock store: %v", expectedKey)
	}

	// Second enqueue in the same transaction with the same QueueName and ShardId
	// must receive UserVersion=1 so it does not overwrite the first item.
	task2 := &store.TaskMessage{
		QueueName: "email_queue",
		ShardId:   1,
		Payload:   []byte("send second email"),
	}
	if err := taskRepo.Enqueue(ctx, tr, dir, task2); err != nil {
		t.Fatal(err)
	}
	dummyVS2 := tuple.Versionstamp{
		TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		UserVersion:        1,
	}
	expectedKey2 := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, "email_queue", dummyVS2, uint64(1)})
	if !kv.HasKey(expectedKey2) {
		t.Fatalf("Expected second versionstamped key with UserVersion=1 not found in mock store: %v", expectedKey2)
	}

	// Verify Dequeue works in MockTransaction via fdblayer.GetRange
	d1, err := taskRepo.Dequeue(ctx, tr, dir, "email_queue")
	if err != nil {
		t.Fatalf("Dequeue 1 failed: %v", err)
	}
	if d1 == nil || string(d1.Payload) != "send email" || len(d1.Versionstamp) != 12 {
		t.Fatalf("unexpected Dequeue 1 result: %+v", d1)
	}
	d2, err := taskRepo.Dequeue(ctx, tr, dir, "email_queue")
	if err != nil {
		t.Fatalf("Dequeue 2 failed: %v", err)
	}
	if d2 == nil || string(d2.Payload) != "send second email" || len(d2.Versionstamp) != 12 {
		t.Fatalf("unexpected Dequeue 2 result: %+v", d2)
	}
	dEmpty, err := taskRepo.Dequeue(ctx, tr, dir, "email_queue")
	if err != nil || dEmpty != nil {
		t.Fatalf("expected empty queue after 2 dequeues, got %+v, err=%v", dEmpty, err)
	}
}

func TestOrder_CompoundPK_CompoundIndex_BytesIndexes_NestedTypes(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"Order"}); err != nil {
		t.Fatalf("failed to sync Order metadata: %v", err)
	}

	orderRepo := store.NewOrderRepository(recordStore)

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

	if err := orderRepo.Create(ctx, tr, dir, order1); err != nil {
		t.Fatalf("Create order1 failed: %v", err)
	}

	// Verify explicit index ID 1001 in MockKV for compound index (status, created_at)
	typeID := recordStore.Metadata()["Order"]
	compoundIdxKey := dir.Pack(tuple.Tuple{
		typeID,
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

	// 1. Get by compound primary key
	pk := store.OrderPrimaryKey{TenantId: "acme", OrderSeq: 101}
	got, err := orderRepo.Get(ctx, tr, dir, pk)
	if err != nil {
		t.Fatalf("Get order1 failed: %v", err)
	}
	if got.Priority != store.Order_PRIORITY_HIGH || got.Address.GetCity() != "Zurich" || got.Address.GetCountry() != "Switzerland" || len(got.History) != 1 {
		t.Fatalf("unexpected nested fields on Order: %+v", got)
	}

	// 2. Query by compound secondary index (status, created_at)
	byStatus, err := orderRepo.GetOrderByStatusAndCreatedAt(ctx, tr, dir, store.OrderStatus_ORDER_STATUS_PENDING, 1700000000)
	if err != nil || len(byStatus) != 1 || byStatus[0].OrderSeq != 101 {
		t.Fatalf("GetOrderByStatusAndCreatedAt failed: got %+v, err=%v", byStatus, err)
	}

	// 3. Query by scalar bytes secondary index (receipt_hash)
	byReceipt, err := orderRepo.GetOrderByReceiptHash(ctx, tr, dir, []byte{0xAA, 0xBB, 0x01})
	if err != nil || len(byReceipt) != 1 || byReceipt[0].OrderSeq != 101 {
		t.Fatalf("GetOrderByReceiptHash failed: got %+v, err=%v", byReceipt, err)
	}

	// 4. Query by fan-out repeated bytes secondary index (attachment_hashes)
	byAttach, err := orderRepo.GetOrderByAttachmentHashes(ctx, tr, dir, []byte{0x10, 0x20})
	if err != nil || len(byAttach) != 1 || byAttach[0].OrderSeq != 101 {
		t.Fatalf("GetOrderByAttachmentHashes failed: got %+v, err=%v", byAttach, err)
	}

	// 5. Set: update Status, ReceiptHash, and AttachmentHashes (remove {0x10,0x20}, keep {0x30,0x40}, add {0x50,0x60})
	updatedOrder := &store.Order{
		TenantId:         "acme",
		OrderSeq:         101,
		Status:           store.OrderStatus_ORDER_STATUS_SHIPPED,
		CreatedAt:        1700000000,
		ReceiptHash:      []byte{0xCC, 0xDD, 0x02},
		AttachmentHashes: [][]byte{{0x30, 0x40}, {0x50, 0x60}},
		Priority:         store.Order_PRIORITY_LOW,
	}
	if err := orderRepo.Set(ctx, tr, dir, updatedOrder); err != nil {
		t.Fatalf("Set updatedOrder failed: %v", err)
	}

	// Old compound index, old receipt hash, and removed attachment hash must return 0 results
	if res, _ := orderRepo.GetOrderByStatusAndCreatedAt(ctx, tr, dir, store.OrderStatus_ORDER_STATUS_PENDING, 1700000000); len(res) != 0 {
		t.Fatalf("stale compound index after Set: %+v", res)
	}
	if res, _ := orderRepo.GetOrderByReceiptHash(ctx, tr, dir, []byte{0xAA, 0xBB, 0x01}); len(res) != 0 {
		t.Fatalf("stale receipt_hash index after Set: %+v", res)
	}
	if res, _ := orderRepo.GetOrderByAttachmentHashes(ctx, tr, dir, []byte{0x10, 0x20}); len(res) != 0 {
		t.Fatalf("stale attachment_hashes index after Set: %+v", res)
	}

	// New index entries must resolve
	if res, _ := orderRepo.GetOrderByStatusAndCreatedAt(ctx, tr, dir, store.OrderStatus_ORDER_STATUS_SHIPPED, 1700000000); len(res) != 1 {
		t.Fatalf("missing updated compound index after Set: %+v", res)
	}
	if res, _ := orderRepo.GetOrderByReceiptHash(ctx, tr, dir, []byte{0xCC, 0xDD, 0x02}); len(res) != 1 {
		t.Fatalf("missing updated receipt_hash index after Set: %+v", res)
	}
	if res, _ := orderRepo.GetOrderByAttachmentHashes(ctx, tr, dir, []byte{0x50, 0x60}); len(res) != 1 {
		t.Fatalf("missing new attachment_hashes index after Set: %+v", res)
	}

	// 6. BatchGetOrder and Delete with compound primary key
	batchRes, err := orderRepo.BatchGetOrder(ctx, tr, dir, []tuple.Tuple{{"acme", int64(101)}})
	if err != nil || len(batchRes) != 1 {
		t.Fatalf("BatchGetOrder failed: got %+v, err=%v", batchRes, err)
	}

	if err := orderRepo.Delete(ctx, tr, dir, pk); err != nil {
		t.Fatalf("Delete order1 failed: %v", err)
	}
	if res, _ := orderRepo.GetOrderByStatusAndCreatedAt(ctx, tr, dir, store.OrderStatus_ORDER_STATUS_SHIPPED, 1700000000); len(res) != 0 {
		t.Fatalf("compound index not cleared after Delete: %+v", res)
	}
	if res, _ := orderRepo.GetOrderByAttachmentHashes(ctx, tr, dir, []byte{0x30, 0x40}); len(res) != 0 {
		t.Fatalf("fan-out bytes index not cleared after Delete: %+v", res)
	}
}

func TestAuditLog_NonQueueVersionstampPK(t *testing.T) {
	ctx := context.Background()
	recordStore, tr, dir, kv := tests.SyncAndSetup()
	if err := recordStore.SyncMetadata(ctx, tr, dir, []string{"AuditLog"}); err != nil {
		t.Fatalf("failed to sync AuditLog metadata: %v", err)
	}

	auditRepo := store.NewAuditLogRepository(recordStore)

	// 1. Create two AuditLog entities in the same transaction with initial atomic Views,
	// standard index (Actor), and fan-out index (Tags).
	if err := auditRepo.Create(ctx, tr, dir, &store.AuditLog{
		Actor: "alice",
		Tags:  []string{"auth", "login"},
		Views: 10,
	}); err != nil {
		t.Fatalf("Create AuditLog 1 failed: %v", err)
	}
	if err := auditRepo.Create(ctx, tr, dir, &store.AuditLog{
		Actor: "bob",
		Tags:  []string{"auth", "logout"},
		Views: 5,
	}); err != nil {
		t.Fatalf("Create AuditLog 2 failed: %v", err)
	}

	// 2. ListAuditLog must unpack the 12-byte versionstamp from the primary key into LogId
	// and populate the atomic Views field.
	listed, err := auditRepo.ListAuditLog(ctx, tr, dir, store.AuditLogPaginationOptions{Limit: 10})
	if err != nil || len(listed.Items) != 2 {
		t.Fatalf("ListAuditLog expected 2 items, got %+v, err=%v", listed, err)
	}
	firstLogID := listed.Items[0].LogId
	secondLogID := listed.Items[1].LogId
	if len(firstLogID) != 12 || len(secondLogID) != 12 || bytes.Equal(firstLogID, secondLogID) {
		t.Fatalf("expected two distinct 12-byte LogIds, got %x and %x", firstLogID, secondLogID)
	}
	if listed.Items[0].Views != 10 || listed.Items[1].Views != 5 {
		t.Fatalf("expected Views 10 and 5 from ListAuditLog, got %d and %d", listed.Items[0].Views, listed.Items[1].Views)
	}

	// 3. Get by log_id using the unpacked 12-byte versionstamp
	got, err := auditRepo.Get(ctx, tr, dir, firstLogID)
	if err != nil {
		t.Fatalf("Get AuditLog by versionstamp failed: %v", err)
	}
	if got.Actor != "alice" || !bytes.Equal(got.LogId, firstLogID) || got.Views != 10 || len(got.Tags) != 2 {
		t.Fatalf("unexpected AuditLog from Get: %+v", got)
	}

	// 4. Query by standard secondary index (GetAuditLogByActor)
	byActor, err := auditRepo.GetAuditLogByActor(ctx, tr, dir, "alice")
	if err != nil || len(byActor) != 1 {
		t.Fatalf("GetAuditLogByActor('alice') expected 1 item, got %+v, err=%v", byActor, err)
	}
	if !bytes.Equal(byActor[0].LogId, firstLogID) || byActor[0].Views != 10 {
		t.Fatalf("GetAuditLogByActor('alice') did not populate LogId/Views: %+v", byActor[0])
	}

	// 5. Query by fan-out secondary index (GetAuditLogByTags)
	byTagAuth, err := auditRepo.GetAuditLogByTags(ctx, tr, dir, "auth")
	if err != nil || len(byTagAuth) != 2 {
		t.Fatalf("GetAuditLogByTags('auth') expected 2 items, got %+v, err=%v", byTagAuth, err)
	}
	if !bytes.Equal(byTagAuth[0].LogId, firstLogID) || !bytes.Equal(byTagAuth[1].LogId, secondLogID) {
		t.Fatalf("GetAuditLogByTags('auth') did not populate LogIds: %+v, %+v", byTagAuth[0], byTagAuth[1])
	}

	// 6. BatchGetAuditLog with both tuple.Versionstamp and raw []byte elements
	batchRes, err := auditRepo.BatchGetAuditLog(ctx, tr, dir, []tuple.Tuple{
		{fdblayer.BytesToVersionstamp(firstLogID)},
		{secondLogID},
	})
	if err != nil || len(batchRes) != 2 {
		t.Fatalf("BatchGetAuditLog expected 2 items, got %+v, err=%v", batchRes, err)
	}
	for _, item := range batchRes {
		if len(item.LogId) != 12 {
			t.Fatalf("BatchGetAuditLog did not populate 12-byte LogId: %+v", item)
		}
		if item.Actor == "alice" && item.Views != 10 {
			t.Fatalf("BatchGetAuditLog expected Views=10 for alice, got %d", item.Views)
		}
	}

	// 7. Atomic mutator AddAuditLogViews
	if err := auditRepo.AddAuditLogViews(ctx, tr, dir, firstLogID, 7); err != nil {
		t.Fatalf("AddAuditLogViews failed: %v", err)
	}
	gotAfterAdd, err := auditRepo.Get(ctx, tr, dir, firstLogID)
	if err != nil || gotAfterAdd.Views != 17 {
		t.Fatalf("expected Views=17 after AddAuditLogViews, got %+v, err=%v", gotAfterAdd, err)
	}

	// 8. Set on existing entity updates in-place (preserving LogId and atomic Views) and updates indexes
	if err := auditRepo.Set(ctx, tr, dir, &store.AuditLog{
		LogId: firstLogID,
		Actor: "alice_admin",
		Tags:  []string{"auth", "elevated"},
	}); err != nil {
		t.Fatalf("Set existing AuditLog failed: %v", err)
	}
	gotAfterSet, err := auditRepo.Get(ctx, tr, dir, firstLogID)
	if err != nil || gotAfterSet.Actor != "alice_admin" || gotAfterSet.Views != 17 {
		t.Fatalf("expected updated Actor='alice_admin' and preserved Views=17 after Set, got %+v, err=%v", gotAfterSet, err)
	}
	if oldIdx, _ := auditRepo.GetAuditLogByActor(ctx, tr, dir, "alice"); len(oldIdx) != 0 {
		t.Fatalf("expected old Actor index 'alice' cleared after Set, got %+v", oldIdx)
	}
	if newIdx, _ := auditRepo.GetAuditLogByActor(ctx, tr, dir, "alice_admin"); len(newIdx) != 1 || !bytes.Equal(newIdx[0].LogId, firstLogID) {
		t.Fatalf("expected new Actor index 'alice_admin' after Set, got %+v", newIdx)
	}
	if oldTag, _ := auditRepo.GetAuditLogByTags(ctx, tr, dir, "login"); len(oldTag) != 0 {
		t.Fatalf("expected removed tag 'login' cleared after Set, got %+v", oldTag)
	}
	if newTag, _ := auditRepo.GetAuditLogByTags(ctx, tr, dir, "elevated"); len(newTag) != 1 || !bytes.Equal(newTag[0].LogId, firstLogID) {
		t.Fatalf("expected added tag 'elevated' indexed after Set, got %+v", newTag)
	}

	// 9. Set with empty LogId delegates to Create; malformed non-12-byte LogId returns error
	if err := auditRepo.Set(ctx, tr, dir, &store.AuditLog{
		LogId: []byte{1, 2, 3},
		Actor: "malformed",
	}); err == nil {
		t.Fatalf("expected Set with 3-byte LogId to return error")
	}
	if err := auditRepo.Set(ctx, tr, dir, &store.AuditLog{
		Actor: "charlie",
		Tags:  []string{"audit"},
		Views: 3,
	}); err != nil {
		t.Fatalf("Set new AuditLog (empty LogId) failed: %v", err)
	}
	byCharlie, err := auditRepo.GetAuditLogByActor(ctx, tr, dir, "charlie")
	if err != nil || len(byCharlie) != 1 || len(byCharlie[0].LogId) != 12 || byCharlie[0].Views != 3 {
		t.Fatalf("expected Set with empty LogId to create indexed AuditLog, got %+v, err=%v", byCharlie, err)
	}

	// 9b. ListAuditLog pagination with Limit=1 and raw []byte Begin cursor
	page1, err := auditRepo.ListAuditLog(ctx, tr, dir, store.AuditLogPaginationOptions{Limit: 1})
	if err != nil || len(page1.Items) != 1 || !page1.HasMore {
		t.Fatalf("expected page1 with 1 item and HasMore=true, got %+v, err=%v", page1, err)
	}
	page2, err := auditRepo.ListAuditLog(ctx, tr, dir, store.AuditLogPaginationOptions{
		Begin: tuple.Tuple{secondLogID},
		Limit: 1,
	})
	if err != nil || len(page2.Items) != 1 || !bytes.Equal(page2.Items[0].LogId, secondLogID) || !page2.HasMore {
		t.Fatalf("expected page2 starting at secondLogID ([]byte) with HasMore=true, got %+v, err=%v", page2, err)
	}

	// 10. Delete by log_id clears primary data, standard index, fan-out index, and atomic field
	if err := auditRepo.Delete(ctx, tr, dir, firstLogID); err != nil {
		t.Fatalf("Delete AuditLog by versionstamp failed: %v", err)
	}
	if _, err := auditRepo.Get(ctx, tr, dir, firstLogID); err == nil {
		t.Fatalf("expected Get after Delete to fail")
	}
	if idxAfterDel, _ := auditRepo.GetAuditLogByActor(ctx, tr, dir, "alice_admin"); len(idxAfterDel) != 0 {
		t.Fatalf("expected Actor index cleared after Delete, got %+v", idxAfterDel)
	}
	if tagAfterDel, _ := auditRepo.GetAuditLogByTags(ctx, tr, dir, "elevated"); len(tagAfterDel) != 0 {
		t.Fatalf("expected fan-out tag 'elevated' cleared after Delete, got %+v", tagAfterDel)
	}
	if sharedTagAfterDel, _ := auditRepo.GetAuditLogByTags(ctx, tr, dir, "auth"); len(sharedTagAfterDel) != 1 || sharedTagAfterDel[0].Actor != "bob" {
		t.Fatalf("expected only bob on 'auth' tag after Delete, got %+v", sharedTagAfterDel)
	}
	typeID := recordStore.Metadata()["AuditLog"]
	fieldKey := dir.Pack(tuple.Tuple{typeID, fdblayer.FieldNamespace, fdblayer.BytesToVersionstamp(firstLogID), int64(4)})
	if kv.HasKey(fieldKey) {
		t.Fatalf("expected atomic field key cleared after Delete")
	}
}




