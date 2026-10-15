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
	tx := fdblayer.NewTx(tr, dir)

	postRepo := store.NewPostRepository()

	post := &store.Post{
		Id:   "post1",
		Tags: []string{"tag1", "tag2", "tag3"},
	}

	err := postRepo.Create(ctx, tx, post)
	if err != nil {
		t.Fatal(err)
	}

	// Verify index entries in MockKV using the compile-time TypeID constant
	for _, tag := range post.Tags {
		indexKey := dir.Pack(tuple.Tuple{store.PostTypeID, fdblayer.IndexNamespace, int64(4095142816), tag, post.Id})
		if !kv.HasKey(indexKey) {
			t.Errorf("Missing index entry for tag %s", tag)
		}
	}
}

func TestFanOutIndex_SetDeltaUpdates(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)

	postRepo := store.NewPostRepository()

	post := &store.Post{
		Id:   "post1",
		Tags: []string{"tag1", "tag2", "tag3"},
	}

	if err := postRepo.Create(ctx, tx, post); err != nil {
		t.Fatal(err)
	}

	tr.ResetLog()
	// Update Tags: remove "tag1", keep "tag2" and "tag3", add "tag4"
	updatedPost := &store.Post{
		Id:   "post1",
		Tags: []string{"tag2", "tag3", "tag4"},
	}
	if err := postRepo.Set(ctx, tx, updatedPost); err != nil {
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

	removedKey := dir.Pack(tuple.Tuple{store.PostTypeID, fdblayer.IndexNamespace, int64(4095142816), "tag1", post.Id})
	if kv.HasKey(removedKey) {
		t.Errorf("Removed tag1 index entry still present")
	}

	for _, tag := range updatedPost.Tags {
		indexKey := dir.Pack(tuple.Tuple{store.PostTypeID, fdblayer.IndexNamespace, int64(4095142816), tag, post.Id})
		if !kv.HasKey(indexKey) {
			t.Errorf("Missing index entry for tag %s", tag)
		}
	}
}

func TestVersionstampedPrimaryKey(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)

	taskRepo := store.NewTaskMessageRepository()

	task := &store.TaskMessage{
		QueueName: "email_queue",
		ShardId:   1,
		Payload:   []byte("send email"),
	}

	err := taskRepo.Enqueue(ctx, tx, task)
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
	// must receive UserVersion=1 so it does not overwrite the first item.
	task2 := &store.TaskMessage{
		QueueName: "email_queue",
		ShardId:   1,
		Payload:   []byte("send second email"),
	}
	if err := taskRepo.Enqueue(ctx, tx, task2); err != nil {
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

	// Verify Dequeue works in MockTransaction via fdblayer.GetRange
	d1, err := taskRepo.Dequeue(ctx, tx, "email_queue")
	if err != nil {
		t.Fatalf("Dequeue 1 failed: %v", err)
	}
	if d1 == nil || string(d1.Payload) != "send email" || len(d1.Versionstamp) != 12 {
		t.Fatalf("unexpected Dequeue 1 result: %+v", d1)
	}
	d2, err := taskRepo.Dequeue(ctx, tx, "email_queue")
	if err != nil {
		t.Fatalf("Dequeue 2 failed: %v", err)
	}
	if d2 == nil || string(d2.Payload) != "send second email" || len(d2.Versionstamp) != 12 {
		t.Fatalf("unexpected Dequeue 2 result: %+v", d2)
	}
	dEmpty, err := taskRepo.Dequeue(ctx, tx, "email_queue")
	if err != nil || dEmpty != nil {
		t.Fatalf("expected empty queue after 2 dequeues, got %+v, err=%v", dEmpty, err)
	}
}

func TestOrder_CompoundPK_CompoundIndex_BytesIndexes_NestedTypes(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)

	orderRepo := store.NewOrderRepository()

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

	if err := orderRepo.Create(ctx, tx, order1); err != nil {
		t.Fatalf("Create order1 failed: %v", err)
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

	// 1. Get by compound primary key
	pk := store.OrderPrimaryKey{TenantId: "acme", OrderSeq: 101}
	got, err := orderRepo.Get(ctx, tx, pk)
	if err != nil {
		t.Fatalf("Get order1 failed: %v", err)
	}
	if got.Priority != store.Order_PRIORITY_HIGH || got.Address.GetCity() != "Zurich" || got.Address.GetCountry() != "Switzerland" || len(got.History) != 1 {
		t.Fatalf("unexpected nested fields on Order: %+v", got)
	}

	// 2. Query by compound secondary index (status, created_at)
	byStatus, err := orderRepo.GetByStatusAndCreatedAt(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, 1700000000, fdblayer.PaginationOptions{})
	if err != nil || len(byStatus.Items) != 1 || byStatus.Items[0].OrderSeq != 101 {
		t.Fatalf("GetOrderByStatusAndCreatedAt failed: got %+v, err=%v", byStatus, err)
	}

	// 3. Query by scalar bytes secondary index (receipt_hash)
	byReceipt, err := orderRepo.GetByReceiptHash(ctx, tx, []byte{0xAA, 0xBB, 0x01}, fdblayer.PaginationOptions{})
	if err != nil || len(byReceipt.Items) != 1 || byReceipt.Items[0].OrderSeq != 101 {
		t.Fatalf("GetOrderByReceiptHash failed: got %+v, err=%v", byReceipt, err)
	}

	// 4. Query by fan-out repeated bytes secondary index (attachment_hashes)
	byAttach, err := orderRepo.GetByAttachmentHashes(ctx, tx, []byte{0x10, 0x20}, fdblayer.PaginationOptions{})
	if err != nil || len(byAttach.Items) != 1 || byAttach.Items[0].OrderSeq != 101 {
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
	if err := orderRepo.Set(ctx, tx, updatedOrder); err != nil {
		t.Fatalf("Set updatedOrder failed: %v", err)
	}

	// Old compound index, old receipt hash, and removed attachment hash must return 0 results
	if res, _ := orderRepo.GetByStatusAndCreatedAt(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, 1700000000, fdblayer.PaginationOptions{}); len(res.Items) != 0 {
		t.Fatalf("stale compound index after Set: %+v", res)
	}
	if res, _ := orderRepo.GetByReceiptHash(ctx, tx, []byte{0xAA, 0xBB, 0x01}, fdblayer.PaginationOptions{}); len(res.Items) != 0 {
		t.Fatalf("stale receipt_hash index after Set: %+v", res)
	}
	if res, _ := orderRepo.GetByAttachmentHashes(ctx, tx, []byte{0x10, 0x20}, fdblayer.PaginationOptions{}); len(res.Items) != 0 {
		t.Fatalf("stale attachment_hashes index after Set: %+v", res)
	}

	// New index entries must resolve
	if res, _ := orderRepo.GetByStatusAndCreatedAt(ctx, tx, store.OrderStatus_ORDER_STATUS_SHIPPED, 1700000000, fdblayer.PaginationOptions{}); len(res.Items) != 1 {
		t.Fatalf("missing updated compound index after Set: %+v", res)
	}
	if res, _ := orderRepo.GetByReceiptHash(ctx, tx, []byte{0xCC, 0xDD, 0x02}, fdblayer.PaginationOptions{}); len(res.Items) != 1 {
		t.Fatalf("missing updated receipt_hash index after Set: %+v", res)
	}
	if res, _ := orderRepo.GetByAttachmentHashes(ctx, tx, []byte{0x50, 0x60}, fdblayer.PaginationOptions{}); len(res.Items) != 1 {
		t.Fatalf("missing new attachment_hashes index after Set: %+v", res)
	}

	// 6. BatchGetOrder and Delete with compound primary key
	batchRes, err := orderRepo.BatchGet(ctx, tx, []store.OrderPrimaryKey{pk})
	if err != nil || len(batchRes) != 1 {
		t.Fatalf("BatchGetOrder failed: got %+v, err=%v", batchRes, err)
	}

	if err := orderRepo.Delete(ctx, tx, pk); err != nil {
		t.Fatalf("Delete order1 failed: %v", err)
	}
	if res, _ := orderRepo.GetByStatusAndCreatedAt(ctx, tx, store.OrderStatus_ORDER_STATUS_SHIPPED, 1700000000, fdblayer.PaginationOptions{}); len(res.Items) != 0 {
		t.Fatalf("compound index not cleared after Delete: %+v", res)
	}
	if res, _ := orderRepo.GetByAttachmentHashes(ctx, tx, []byte{0x30, 0x40}, fdblayer.PaginationOptions{}); len(res.Items) != 0 {
		t.Fatalf("fan-out bytes index not cleared after Delete: %+v", res)
	}
}

func TestAuditLog_NonQueueVersionstampPK(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)

	auditRepo := store.NewAuditLogRepository()

	// 1. Create two AuditLog entities in the same transaction with initial atomic Views,
	// standard index (Actor), and fan-out index (Tags).
	if err := auditRepo.Create(ctx, tx, &store.AuditLog{
		Actor: "alice",
		Tags:  []string{"auth", "login"},
		Views: 10,
	}); err != nil {
		t.Fatalf("Create AuditLog 1 failed: %v", err)
	}
	if err := auditRepo.Create(ctx, tx, &store.AuditLog{
		Actor: "bob",
		Tags:  []string{"auth", "logout"},
		Views: 5,
	}); err != nil {
		t.Fatalf("Create AuditLog 2 failed: %v", err)
	}

	// 2. ListAuditLog must unpack the 12-byte versionstamp from the primary key into LogId
	// and populate the atomic Views field.
	listed, err := auditRepo.List(ctx, tx, fdblayer.PaginationOptions{Limit: 10})
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
	got, err := auditRepo.Get(ctx, tx, firstLogID)
	if err != nil {
		t.Fatalf("Get AuditLog by versionstamp failed: %v", err)
	}
	if got.Actor != "alice" || !bytes.Equal(got.LogId, firstLogID) || got.Views != 10 || len(got.Tags) != 2 {
		t.Fatalf("unexpected AuditLog from Get: %+v", got)
	}

	// 4. Query by standard secondary index (GetAuditLogByActor)
	byActor, err := auditRepo.GetByActor(ctx, tx, "alice", fdblayer.PaginationOptions{})
	if err != nil || len(byActor.Items) != 1 {
		t.Fatalf("GetAuditLogByActor('alice') expected 1 item, got %+v, err=%v", byActor, err)
	}
	if !bytes.Equal(byActor.Items[0].LogId, firstLogID) || byActor.Items[0].Views != 10 {
		t.Fatalf("GetAuditLogByActor('alice') did not populate LogId/Views: %+v", byActor.Items[0])
	}

	// 5. Query by fan-out secondary index (GetAuditLogByTags)
	byTagAuth, err := auditRepo.GetByTags(ctx, tx, "auth", fdblayer.PaginationOptions{})
	if err != nil || len(byTagAuth.Items) != 2 {
		t.Fatalf("GetAuditLogByTags('auth') expected 2 items, got %+v, err=%v", byTagAuth, err)
	}
	if !bytes.Equal(byTagAuth.Items[0].LogId, firstLogID) || !bytes.Equal(byTagAuth.Items[1].LogId, secondLogID) {
		t.Fatalf("GetAuditLogByTags('auth') did not populate LogIds: %+v, %+v", byTagAuth.Items[0], byTagAuth.Items[1])
	}

	// 6. BatchGetAuditLog with 12-byte versionstamp primary keys
	batchRes, err := auditRepo.BatchGet(ctx, tx, [][]byte{firstLogID, secondLogID})
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
	if err := auditRepo.AddViews(ctx, tx, firstLogID, 7); err != nil {
		t.Fatalf("AddAuditLogViews failed: %v", err)
	}
	gotAfterAdd, err := auditRepo.Get(ctx, tx, firstLogID)
	if err != nil || gotAfterAdd.Views != 17 {
		t.Fatalf("expected Views=17 after AddAuditLogViews, got %+v, err=%v", gotAfterAdd, err)
	}

	// 8. Set on existing entity updates in-place (preserving LogId and atomic Views) and updates indexes
	if err := auditRepo.Set(ctx, tx, &store.AuditLog{
		LogId: firstLogID,
		Actor: "alice_admin",
		Tags:  []string{"auth", "elevated"},
	}); err != nil {
		t.Fatalf("Set existing AuditLog failed: %v", err)
	}
	gotAfterSet, err := auditRepo.Get(ctx, tx, firstLogID)
	if err != nil || gotAfterSet.Actor != "alice_admin" || gotAfterSet.Views != 17 {
		t.Fatalf("expected updated Actor='alice_admin' and preserved Views=17 after Set, got %+v, err=%v", gotAfterSet, err)
	}
	if oldIdx, _ := auditRepo.GetByActor(ctx, tx, "alice", fdblayer.PaginationOptions{}); len(oldIdx.Items) != 0 {
		t.Fatalf("expected old Actor index 'alice' cleared after Set, got %+v", oldIdx)
	}
	if newIdx, _ := auditRepo.GetByActor(ctx, tx, "alice_admin", fdblayer.PaginationOptions{}); len(newIdx.Items) != 1 || !bytes.Equal(newIdx.Items[0].LogId, firstLogID) {
		t.Fatalf("expected new Actor index 'alice_admin' after Set, got %+v", newIdx)
	}
	if oldTag, _ := auditRepo.GetByTags(ctx, tx, "login", fdblayer.PaginationOptions{}); len(oldTag.Items) != 0 {
		t.Fatalf("expected removed tag 'login' cleared after Set, got %+v", oldTag)
	}
	if newTag, _ := auditRepo.GetByTags(ctx, tx, "elevated", fdblayer.PaginationOptions{}); len(newTag.Items) != 1 || !bytes.Equal(newTag.Items[0].LogId, firstLogID) {
		t.Fatalf("expected added tag 'elevated' indexed after Set, got %+v", newTag)
	}

	// 9. Set with empty LogId delegates to Create; malformed non-12-byte LogId returns error
	if err := auditRepo.Set(ctx, tx, &store.AuditLog{
		LogId: []byte{1, 2, 3},
		Actor: "malformed",
	}); err == nil {
		t.Fatalf("expected Set with 3-byte LogId to return error")
	}
	if err := auditRepo.Set(ctx, tx, &store.AuditLog{
		Actor: "charlie",
		Tags:  []string{"audit"},
		Views: 3,
	}); err != nil {
		t.Fatalf("Set new AuditLog (empty LogId) failed: %v", err)
	}
	byCharlie, err := auditRepo.GetByActor(ctx, tx, "charlie", fdblayer.PaginationOptions{})
	if err != nil || len(byCharlie.Items) != 1 || len(byCharlie.Items[0].LogId) != 12 || byCharlie.Items[0].Views != 3 {
		t.Fatalf("expected Set with empty LogId to create indexed AuditLog, got %+v, err=%v", byCharlie, err)
	}

	// 9b. ListAuditLog pagination with Limit=1 and raw []byte Begin cursor
	page1, err := auditRepo.List(ctx, tx, fdblayer.PaginationOptions{Limit: 1})
	if err != nil || len(page1.Items) != 1 || !page1.HasMore {
		t.Fatalf("expected page1 with 1 item and HasMore=true, got %+v, err=%v", page1, err)
	}
	page2, err := auditRepo.List(ctx, tx, fdblayer.PaginationOptions{
		Begin: tuple.Tuple{secondLogID},
		Limit: 1,
	})
	if err != nil || len(page2.Items) != 1 || !bytes.Equal(page2.Items[0].LogId, secondLogID) || !page2.HasMore {
		t.Fatalf("expected page2 starting at secondLogID ([]byte) with HasMore=true, got %+v, err=%v", page2, err)
	}

	// 10. Delete by log_id clears primary data, standard index, fan-out index, and atomic field
	if err := auditRepo.Delete(ctx, tx, firstLogID); err != nil {
		t.Fatalf("Delete AuditLog by versionstamp failed: %v", err)
	}
	if _, err := auditRepo.Get(ctx, tx, firstLogID); err == nil {
		t.Fatalf("expected Get after Delete to fail")
	}
	if idxAfterDel, _ := auditRepo.GetByActor(ctx, tx, "alice_admin", fdblayer.PaginationOptions{}); len(idxAfterDel.Items) != 0 {
		t.Fatalf("expected Actor index cleared after Delete, got %+v", idxAfterDel)
	}
	if tagAfterDel, _ := auditRepo.GetByTags(ctx, tx, "elevated", fdblayer.PaginationOptions{}); len(tagAfterDel.Items) != 0 {
		t.Fatalf("expected fan-out tag 'elevated' cleared after Delete, got %+v", tagAfterDel)
	}
	if sharedTagAfterDel, _ := auditRepo.GetByTags(ctx, tx, "auth", fdblayer.PaginationOptions{}); len(sharedTagAfterDel.Items) != 1 || sharedTagAfterDel.Items[0].Actor != "bob" {
		t.Fatalf("expected only bob on 'auth' tag after Delete, got %+v", sharedTagAfterDel)
	}
	fieldKey := dir.Pack(tuple.Tuple{store.AuditLogTypeID, fdblayer.FieldNamespace, fdblayer.BytesToVersionstamp(firstLogID), int64(4)})
	if kv.HasKey(fieldKey) {
		t.Fatalf("expected atomic field key cleared after Delete")
	}
}

func TestFanOutIndex_CreateDeduplicatesRepeatedTags(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)
	postRepo := store.NewPostRepository()

	tr.ResetLog()
	post := &store.Post{
		Id:   "post_dup",
		Tags: []string{"go", "go", "fdb", "go"},
	}
	if err := postRepo.Create(ctx, tx, post); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	// 1 primary record write + 2 unique tag index writes ("go", "fdb") = 3 SetCalls total (BUG-7)
	if len(tr.SetCalls) != 3 {
		t.Fatalf("expected 3 Set calls (1 primary + 2 unique tags), got %d", len(tr.SetCalls))
	}
}

func TestQueue_EnqueueZerosVersionstampPayload_AndDequeueBatch(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)
	taskRepo := store.NewTaskMessageRepository()

	staleVS := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	msg1 := &store.TaskMessage{
		QueueName:    "q_batch",
		ShardId:      1,
		Versionstamp: staleVS,
		Payload:      []byte("item-1"),
	}
	if err := taskRepo.Enqueue(ctx, tx, msg1); err != nil {
		t.Fatalf("Enqueue 1 failed: %v", err)
	}
	// Caller's struct must not be mutated
	if !bytes.Equal(msg1.Versionstamp, staleVS) {
		t.Fatalf("Enqueue mutated caller's Versionstamp slice: %x", msg1.Versionstamp)
	}

	// Raw stored protobuf value in FDB must have Versionstamp zeroed (BUG-4)
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

	// Enqueue 2 more items and test DequeueBatch (PERF-5)
	_ = taskRepo.Enqueue(ctx, tx, &store.TaskMessage{QueueName: "q_batch", ShardId: 2, Payload: []byte("item-2")})
	_ = taskRepo.Enqueue(ctx, tx, &store.TaskMessage{QueueName: "q_batch", ShardId: 3, Payload: []byte("item-3")})

	batch, err := taskRepo.DequeueBatch(ctx, tx, "q_batch", 2)
	if err != nil || len(batch) != 2 {
		t.Fatalf("DequeueBatch(2) expected 2 items, got %+v, err=%v", batch, err)
	}
	if string(batch[0].Payload) != "item-1" || string(batch[1].Payload) != "item-2" || len(batch[0].Versionstamp) != 12 {
		t.Fatalf("unexpected DequeueBatch(2) items: %+v", batch)
	}

	rem, err := taskRepo.DequeueBatch(ctx, tx, "q_batch", 5)
	if err != nil || len(rem) != 1 || string(rem[0].Payload) != "item-3" {
		t.Fatalf("DequeueBatch remaining expected 1 item ('item-3'), got %+v, err=%v", rem, err)
	}
}

func TestOrder_PrefixList_ReverseList_AndBatchGetNormalization(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)
	orderRepo := store.NewOrderRepository()

	for _, seq := range []int32{1, 2, 3} {
		_ = orderRepo.Create(ctx, tx, &store.Order{
			TenantId: "acme",
			OrderSeq: seq,
			Status:   store.OrderStatus_ORDER_STATUS_PENDING,
		})
	}
	_ = orderRepo.Create(ctx, tx, &store.Order{
		TenantId: "other",
		OrderSeq: 1,
		Status:   store.OrderStatus_ORDER_STATUS_PENDING,
	})

	// 1. ListOrder with Prefix={"acme"} and Reverse=true (PERF-1, PERF-3)
	acmeRev, err := orderRepo.List(ctx, tx, fdblayer.PaginationOptions{
		Prefix:  tuple.Tuple{"acme"},
		Limit:   2,
		Reverse: true,
	})
	if err != nil || len(acmeRev.Items) != 2 || !acmeRev.HasMore {
		t.Fatalf("unexpected ListOrder(Prefix:acme, Reverse:true) page 1: %+v, err=%v", acmeRev, err)
	}
	if acmeRev.Items[0].OrderSeq != 3 || acmeRev.Items[1].OrderSeq != 2 {
		t.Fatalf("expected OrderSeq [3, 2], got [%d, %d]", acmeRev.Items[0].OrderSeq, acmeRev.Items[1].OrderSeq)
	}

	acmeRev2, err := orderRepo.List(ctx, tx, fdblayer.PaginationOptions{
		Prefix:  tuple.Tuple{"acme"},
		Begin:   acmeRev.NextKey,
		Limit:   2,
		Reverse: true,
	})
	if err != nil || len(acmeRev2.Items) != 1 || acmeRev2.HasMore || acmeRev2.Items[0].OrderSeq != 1 {
		t.Fatalf("unexpected ListOrder(Prefix:acme, Reverse:true) page 2: %+v, err=%v", acmeRev2, err)
	}

	// 2. BatchGetOrder with strongly-typed compound primary key (BUG-5)
	batchByPK, err := orderRepo.BatchGet(ctx, tx, []store.OrderPrimaryKey{
		{TenantId: "acme", OrderSeq: 1},
		{TenantId: "acme", OrderSeq: 3},
	})
	if err != nil || len(batchByPK) != 2 {
		t.Fatalf("BatchGetOrder failed: %+v, err=%v", batchByPK, err)
	}

	// 3. Out-of-bounds Begin cursor with Prefix={"acme"} must be clamped to prefix range
	// without leaking records from "other" or causing an inverted range.
	clampedForwardBefore, err := orderRepo.List(ctx, tx, fdblayer.PaginationOptions{
		Prefix: tuple.Tuple{"acme"},
		Begin:  tuple.Tuple{"aaa", int64(1)},
		Limit:  10,
	})
	if err != nil || len(clampedForwardBefore.Items) != 3 {
		t.Fatalf("expected Begin<'acme' to be clamped to 'acme' start (3 items), got %+v, err=%v", clampedForwardBefore, err)
	}
	clampedForwardAfter, err := orderRepo.List(ctx, tx, fdblayer.PaginationOptions{
		Prefix: tuple.Tuple{"acme"},
		Begin:  tuple.Tuple{"zzz", int64(1)},
		Limit:  10,
	})
	if err != nil || len(clampedForwardAfter.Items) != 0 {
		t.Fatalf("expected Begin>'acme' to return 0 items without inverted range, got %+v, err=%v", clampedForwardAfter, err)
	}
	clampedReverseBefore, err := orderRepo.List(ctx, tx, fdblayer.PaginationOptions{
		Prefix:  tuple.Tuple{"acme"},
		Begin:   tuple.Tuple{"aaa", int64(1)},
		Limit:   10,
		Reverse: true,
	})
	if err != nil || len(clampedReverseBefore.Items) != 0 {
		t.Fatalf("expected Reverse=true with Begin<'acme' to return 0 items, got %+v, err=%v", clampedReverseBefore, err)
	}
	clampedReverseAfter, err := orderRepo.List(ctx, tx, fdblayer.PaginationOptions{
		Prefix:  tuple.Tuple{"acme"},
		Begin:   tuple.Tuple{"zzz", int64(1)},
		Limit:   10,
		Reverse: true,
	})
	if err != nil || len(clampedReverseAfter.Items) != 3 {
		t.Fatalf("expected Reverse=true with Begin>'acme' to clamp to 'acme' end (3 items), got %+v, err=%v", clampedReverseAfter, err)
	}
}

func TestOrder_CompositeIndexPrefixQueries(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)
	orderRepo := store.NewOrderRepository()

	// Create 3 PENDING orders with distinct CreatedAt timestamps and 1 SHIPPED order
	for i, ts := range []int64{1000, 2000, 3000} {
		if err := orderRepo.Create(ctx, tx, &store.Order{
			TenantId:  "acme",
			OrderSeq:  int32(i + 1),
			Status:    store.OrderStatus_ORDER_STATUS_PENDING,
			CreatedAt: ts,
		}); err != nil {
			t.Fatalf("Create pending order %d failed: %v", i+1, err)
		}
	}
	if err := orderRepo.Create(ctx, tx, &store.Order{
		TenantId:  "acme",
		OrderSeq:  99,
		Status:    store.OrderStatus_ORDER_STATUS_SHIPPED,
		CreatedAt: 1500,
	}); err != nil {
		t.Fatalf("Create shipped order failed: %v", err)
	}

	// 1. GetOrderByStatus (partial prefix of (status, created_at)) returns all 3 PENDING orders ordered by CreatedAt
	pendingRes, err := orderRepo.GetByStatus(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, fdblayer.PaginationOptions{})
	if err != nil || len(pendingRes.Items) != 3 {
		t.Fatalf("GetOrderByStatus(PENDING) expected 3 orders, got %+v, err=%v", pendingRes, err)
	}
	pending := pendingRes.Items
	if pending[0].CreatedAt != 1000 || pending[1].CreatedAt != 2000 || pending[2].CreatedAt != 3000 {
		t.Fatalf("unexpected CreatedAt order from GetOrderByStatus: [%d, %d, %d]", pending[0].CreatedAt, pending[1].CreatedAt, pending[2].CreatedAt)
	}

	// 2. GetOrderByStatus forward pagination (Limit=2) across different CreatedAt values
	fwd1, err := orderRepo.GetByStatus(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, fdblayer.PaginationOptions{
		Limit: 2,
	})
	if err != nil || len(fwd1.Items) != 2 || !fwd1.HasMore {
		t.Fatalf("GetOrderByStatus forward page 1 failed: %+v, err=%v", fwd1, err)
	}
	if fwd1.Items[0].OrderSeq != 1 || fwd1.Items[1].OrderSeq != 2 {
		t.Fatalf("expected OrderSeq [1, 2], got [%d, %d]", fwd1.Items[0].OrderSeq, fwd1.Items[1].OrderSeq)
	}

	fwd2, err := orderRepo.GetByStatus(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, fdblayer.PaginationOptions{
		Begin: fwd1.NextKey,
		Limit: 2,
	})
	if err != nil || len(fwd2.Items) != 1 || fwd2.HasMore || fwd2.Items[0].OrderSeq != 3 {
		t.Fatalf("GetOrderByStatus forward page 2 failed: %+v, err=%v", fwd2, err)
	}

	// 3. GetOrderByStatus reverse pagination (Limit=2, Reverse=true)
	rev1, err := orderRepo.GetByStatus(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, fdblayer.PaginationOptions{
		Limit:   2,
		Reverse: true,
	})
	if err != nil || len(rev1.Items) != 2 || !rev1.HasMore {
		t.Fatalf("GetOrderByStatus reverse page 1 failed: %+v, err=%v", rev1, err)
	}
	if rev1.Items[0].OrderSeq != 3 || rev1.Items[1].OrderSeq != 2 {
		t.Fatalf("expected reverse OrderSeq [3, 2], got [%d, %d]", rev1.Items[0].OrderSeq, rev1.Items[1].OrderSeq)
	}

	rev2, err := orderRepo.GetByStatus(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, fdblayer.PaginationOptions{
		Begin:   rev1.NextKey,
		Limit:   2,
		Reverse: true,
	})
	if err != nil || len(rev2.Items) != 1 || rev2.HasMore || rev2.Items[0].OrderSeq != 1 {
		t.Fatalf("GetOrderByStatus reverse page 2 failed: %+v, err=%v", rev2, err)
	}
}

func TestFanOutIndex_SetAndDeleteDeduplicateRepeatedTags(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)
	postRepo := store.NewPostRepository()

	if err := postRepo.Create(ctx, tx, &store.Post{
		Id:   "p_dup",
		Tags: []string{"a", "a", "b"},
	}); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	tr.ResetLog()
	if err := postRepo.Set(ctx, tx, &store.Post{
		Id:   "p_dup",
		Tags: []string{"b", "c", "c", "c"},
	}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if len(tr.ClearCalls) != 1 || len(tr.SetCalls) != 2 {
		t.Fatalf("expected 1 ClearCall and 2 SetCalls on Set with duplicate tags, got %d ClearCalls and %d SetCalls", len(tr.ClearCalls), len(tr.SetCalls))
	}

	tr.ResetLog()
	if err := postRepo.Delete(ctx, tx, "p_dup"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if len(tr.ClearCalls) != 3 {
		t.Fatalf("expected 3 ClearCalls (1 primary + 2 unique tags) on Delete with duplicate tags, got %d", len(tr.ClearCalls))
	}
}

func TestQueue_CorruptedPayloadPreservesEntry(t *testing.T) {
	ctx := context.Background()
	tr, dir, kv := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)
	taskRepo := store.NewTaskMessageRepository()

	dummyVS := tuple.Versionstamp{
		TransactionVersion: [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		UserVersion:        0,
	}
	corruptKey := dir.Pack(tuple.Tuple{store.TaskMessageTypeID, fdblayer.DataNamespace, "corrupt_q", dummyVS, uint64(1)})
	tr.Set(corruptKey, []byte{0x00, 0xff, 0xff})

	if _, err := taskRepo.Dequeue(ctx, tx, "corrupt_q"); err == nil {
		t.Fatal("expected unmarshal error from Dequeue on corrupted payload")
	}
	if !kv.HasKey(corruptKey) {
		t.Fatal("expected corrupted queue item to remain in store after Dequeue error")
	}

	if _, err := taskRepo.DequeueBatch(ctx, tx, "corrupt_q", 5); err == nil {
		t.Fatal("expected unmarshal error from DequeueBatch on corrupted payload")
	}
	if !kv.HasKey(corruptKey) {
		t.Fatal("expected corrupted queue item to remain in store after DequeueBatch error")
	}
}

func TestSecondaryAndFanOutIndexKeysNeverMissed_CreateSetDelete(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	tx := fdblayer.NewTx(tr, dir)
	orderRepo := store.NewOrderRepository()

	countOrderIndexKeys := func() int {
		prefix, err := fdb.PrefixRange(dir.Pack(tuple.Tuple{store.OrderTypeID, fdblayer.IndexNamespace}))
		if err != nil {
			t.Fatalf("PrefixRange failed: %v", err)
		}
		return len(tr.GetRangeSlice(prefix, fdb.RangeOptions{}))
	}

	hashA := []byte{0x0A}
	hashB := []byte{0x0B}
	hashC := []byte{0x0C}
	hashD := []byte{0x0D}
	pk := store.OrderPrimaryKey{TenantId: "t1", OrderSeq: 7}

	// 1. Create with 2 attachment hashes:
	//    - 1 key for (status, created_at) [id=1001]
	//    - 1 key for (receipt_hash)
	//    - 2 keys for fan-out (attachment_hashes)
	//    - 2 keys for composite fan-out (status, attachment_hashes)
	//    Total = 6 index keys.
	if err := orderRepo.Create(ctx, tx, &store.Order{
		TenantId:         pk.TenantId,
		OrderSeq:         pk.OrderSeq,
		Status:           store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt:        100,
		ReceiptHash:      []byte{0x01},
		AttachmentHashes: [][]byte{hashA, hashB, hashA}, // duplicate hashA must be deduplicated
	}); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 6 {
		t.Fatalf("expected 6 index keys after Create, got %d", got)
	}
	if res, err := orderRepo.GetByStatusAndAttachmentHashes(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, hashA, fdblayer.PaginationOptions{}); err != nil || len(res.Items) != 1 {
		t.Fatalf("expected 1 order for (PENDING, hashA), got %+v, err=%v", res, err)
	}
	if res, err := orderRepo.GetByStatusAndAttachmentHashes(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, hashB, fdblayer.PaginationOptions{}); err != nil || len(res.Items) != 1 {
		t.Fatalf("expected 1 order for (PENDING, hashB), got %+v, err=%v", res, err)
	}

	// 2. Set with UNCHANGED Status (PENDING) and updated AttachmentHashes ({hashB, hashC}):
	//    Delta path must remove hashA, keep hashB, and add hashC on both fan-out indexes.
	if err := orderRepo.Set(ctx, tx, &store.Order{
		TenantId:         pk.TenantId,
		OrderSeq:         pk.OrderSeq,
		Status:           store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt:        100,
		ReceiptHash:      []byte{0x01},
		AttachmentHashes: [][]byte{hashB, hashC},
	}); err != nil {
		t.Fatalf("Set (same status) failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 6 {
		t.Fatalf("expected 6 index keys after Set (same status), got %d", got)
	}
	if res, _ := orderRepo.GetByStatusAndAttachmentHashes(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, hashA, fdblayer.PaginationOptions{}); len(res.Items) != 0 {
		t.Fatalf("expected (PENDING, hashA) cleared after Set, got %+v", res)
	}
	if res, _ := orderRepo.GetByStatusAndAttachmentHashes(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, hashC, fdblayer.PaginationOptions{}); len(res.Items) != 1 {
		t.Fatalf("expected (PENDING, hashC) added after Set, got %+v", res)
	}

	// 3. Set with CHANGED Status (PENDING -> SHIPPED) and overlapping AttachmentHashes ({hashC, hashD}):
	//    Non-repeated field changed in composite fan-out index (status, attachment_hashes),
	//    so ALL old (PENDING, hashB) and (PENDING, hashC) keys must be cleared, and (SHIPPED, hashC) + (SHIPPED, hashD) written!
	if err := orderRepo.Set(ctx, tx, &store.Order{
		TenantId:         pk.TenantId,
		OrderSeq:         pk.OrderSeq,
		Status:           store.OrderStatus_ORDER_STATUS_SHIPPED,
		CreatedAt:        200,
		ReceiptHash:      []byte{0x02},
		AttachmentHashes: [][]byte{hashC, hashD},
	}); err != nil {
		t.Fatalf("Set (changed status) failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 6 {
		t.Fatalf("expected 6 index keys after Set (changed status), got %d", got)
	}
	if res, _ := orderRepo.GetByStatusAndAttachmentHashes(ctx, tx, store.OrderStatus_ORDER_STATUS_PENDING, hashC, fdblayer.PaginationOptions{}); len(res.Items) != 0 {
		t.Fatalf("stale composite fan-out key (PENDING, hashC) was not cleared when status changed: %+v", res)
	}
	if res, _ := orderRepo.GetByStatusAndAttachmentHashes(ctx, tx, store.OrderStatus_ORDER_STATUS_SHIPPED, hashC, fdblayer.PaginationOptions{}); len(res.Items) != 1 {
		t.Fatalf("missing composite fan-out key (SHIPPED, hashC) after status change: %+v", res)
	}
	if res, _ := orderRepo.GetByStatusAndAttachmentHashes(ctx, tx, store.OrderStatus_ORDER_STATUS_SHIPPED, hashD, fdblayer.PaginationOptions{}); len(res.Items) != 1 {
		t.Fatalf("missing composite fan-out key (SHIPPED, hashD) after status change: %+v", res)
	}

	// 4. Set with empty AttachmentHashes (nil): all fan-out keys must be cleared, leaving only the 2 standard index keys.
	if err := orderRepo.Set(ctx, tx, &store.Order{
		TenantId:         pk.TenantId,
		OrderSeq:         pk.OrderSeq,
		Status:           store.OrderStatus_ORDER_STATUS_SHIPPED,
		CreatedAt:        200,
		ReceiptHash:      []byte{0x02},
		AttachmentHashes: nil,
	}); err != nil {
		t.Fatalf("Set (empty fan-out) failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 2 {
		t.Fatalf("expected 2 standard index keys after clearing fan-out slice, got %d", got)
	}

	// 5. Delete must clear all remaining index keys, leaving 0 keys in IndexNamespace.
	if err := orderRepo.Delete(ctx, tx, pk); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 0 {
		t.Fatalf("expected 0 index keys after Delete, got %d", got)
	}

	// 6. Upsert via Set on a non-existent record must write all standard and fan-out index keys,
	//    and Delete must still clear all index keys even if primary key fields inside the stored
	//    protobuf value were omitted.
	pk2 := store.OrderPrimaryKey{TenantId: "t2", OrderSeq: 9}
	if err := orderRepo.Set(ctx, tx, &store.Order{
		TenantId:         pk2.TenantId,
		OrderSeq:         pk2.OrderSeq,
		Status:           store.OrderStatus_ORDER_STATUS_PENDING,
		CreatedAt:        300,
		ReceiptHash:      []byte{0x09},
		AttachmentHashes: [][]byte{hashA, hashB},
	}); err != nil {
		t.Fatalf("Upsert via Set failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 6 {
		t.Fatalf("expected 6 index keys after Upsert via Set, got %d", got)
	}

	// Simulate a stored protobuf value whose PK fields were omitted from the serialized body.
	rawDataKey := dir.Pack(tuple.Tuple{store.OrderTypeID, fdblayer.DataNamespace, pk2.TenantId, int64(pk2.OrderSeq)})
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

	if err := orderRepo.Delete(ctx, tx, pk2); err != nil {
		t.Fatalf("Delete after PK-stripped payload failed: %v", err)
	}
	if got := countOrderIndexKeys(); got != 0 {
		t.Fatalf("expected 0 index keys after Delete of PK-stripped payload, got %d", got)
	}
}

func TestRecordLayer_StoreAndPipelinedMutations(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	st := fdblayer.NewStoreFromTransaction(tr, dir)

	settingRepo := store.NewSimpleSettingRepository()
	custRepo := store.NewCustomerRepository()

	// 1. Blind Replace and Insert via Store.Apply
	if err := st.Apply(ctx,
		settingRepo.Replace(&store.SimpleSetting{Key: "theme", Value: "dark"}),
		settingRepo.Insert(&store.SimpleSetting{Key: "lang", Value: "en"}),
		custRepo.Insert(&store.Customer{CustomerId: "c1", Email: "alice@example.com", Name: "Alice", ReferralCode: ""}),
		custRepo.Insert(&store.Customer{CustomerId: "c2", Email: "bob@example.com", Name: "Bob", ReferralCode: "REF-BOB"}),
	); err != nil {
		t.Fatalf("Store.Apply initial batch failed: %v", err)
	}

	// 2. ReadTransact + Snapshot reads
	if err := st.ReadTransact(ctx, func(ctx context.Context, rtx fdblayer.ReadTx) error {
		s, err := settingRepo.Get(ctx, rtx, "theme")
		if err != nil || s.Value != "dark" {
			t.Fatalf("unexpected theme setting: %+v, err=%v", s, err)
		}
		// Snapshot read via rtx.Snapshot()
		c, err := custRepo.GetUniqueByEmail(ctx, rtx.Snapshot(), "alice@example.com")
		if err != nil || c.CustomerId != "c1" || c.Name != "Alice" {
			t.Fatalf("GetUniqueByEmail(alice) failed: %+v, err=%v", c, err)
		}
		// Sparse index: Alice has empty ReferralCode so she must not appear under ""
		sparseEmpty, err := custRepo.GetByReferralCode(ctx, rtx, "", fdblayer.PaginationOptions{})
		if err != nil || len(sparseEmpty.Items) != 0 {
			t.Fatalf("expected 0 items in sparse index for empty ReferralCode, got %+v, err=%v", sparseEmpty, err)
		}
		sparseBob, err := custRepo.GetByReferralCode(ctx, rtx, "REF-BOB", fdblayer.PaginationOptions{})
		if err != nil || len(sparseBob.Items) != 1 || sparseBob.Items[0].CustomerId != "c2" {
			t.Fatalf("expected c2 in sparse ReferralCode index, got %+v, err=%v", sparseBob, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("ReadTransact failed: %v", err)
	}

	// 3. Intra-batch duplicate primary key detection during Buffer/Flush
	err := st.Transact(ctx, func(ctx context.Context, tx *fdblayer.Tx) error {
		tx.Buffer(
			custRepo.Insert(&store.Customer{CustomerId: "c3", Email: "c3_a@example.com"}),
			custRepo.Insert(&store.Customer{CustomerId: "c3", Email: "c3_b@example.com"}),
		)
		return nil
	})
	if !errors.Is(err, fdblayer.ErrAlreadyExists) {
		t.Fatalf("expected intra-batch duplicate PK ErrAlreadyExists, got %v", err)
	}

	// 4. Intra-batch duplicate unique index detection during Buffer/Flush
	err = st.Transact(ctx, func(ctx context.Context, tx *fdblayer.Tx) error {
		tx.Buffer(
			custRepo.Insert(&store.Customer{CustomerId: "c3", Email: "shared@example.com"}),
			custRepo.Insert(&store.Customer{CustomerId: "c4", Email: "shared@example.com"}),
		)
		return nil
	})
	if !errors.Is(err, fdblayer.ErrUniqueConstraintViolation) || !errors.Is(err, fdblayer.ErrAlreadyExists) {
		t.Fatalf("expected intra-batch duplicate unique index ErrUniqueConstraintViolation, got %v", err)
	}

	// 5. Database-level unique index violation on Insert/Flush
	err = st.Apply(ctx,
		custRepo.Insert(&store.Customer{CustomerId: "c5", Email: "alice@example.com"}),
	)
	if !errors.Is(err, fdblayer.ErrUniqueConstraintViolation) {
		t.Fatalf("expected DB-level ErrUniqueConstraintViolation for alice@example.com, got %v", err)
	}

	// 6. Pipelined Upsert updating unique Email and sparse ReferralCode + DeleteMutation
	if err := st.Apply(ctx,
		custRepo.Upsert(&store.Customer{CustomerId: "c1", Email: "alice_new@example.com", Name: "Alice V2", ReferralCode: "REF-ALICE"}),
		custRepo.Upsert(&store.Customer{CustomerId: "c2", Email: "bob@example.com", Name: "Bob V2", ReferralCode: ""}),
		settingRepo.DeleteMutation("lang"),
	); err != nil {
		t.Fatalf("Store.Apply upsert/delete batch failed: %v", err)
	}

	if err := st.ReadTransact(ctx, func(ctx context.Context, rtx fdblayer.ReadTx) error {
		if _, err := custRepo.GetUniqueByEmail(ctx, rtx, "alice@example.com"); !errors.Is(err, fdblayer.ErrNotFound) {
			t.Fatalf("expected old email alice@example.com to return ErrNotFound, got %v", err)
		}
		c1, err := custRepo.GetUniqueByEmail(ctx, rtx, "alice_new@example.com")
		if err != nil || c1.Name != "Alice V2" {
			t.Fatalf("expected updated c1 by new email, got %+v, err=%v", c1, err)
		}
		byAliceRef, err := custRepo.GetByReferralCode(ctx, rtx, "REF-ALICE", fdblayer.PaginationOptions{})
		if err != nil || len(byAliceRef.Items) != 1 || byAliceRef.Items[0].CustomerId != "c1" {
			t.Fatalf("expected c1 in sparse index after setting ReferralCode, got %+v, err=%v", byAliceRef, err)
		}
		byBobRef, err := custRepo.GetByReferralCode(ctx, rtx, "REF-BOB", fdblayer.PaginationOptions{})
		if err != nil || len(byBobRef.Items) != 0 {
			t.Fatalf("expected c2 removed from sparse index after clearing ReferralCode, got %+v, err=%v", byBobRef, err)
		}
		if _, err := settingRepo.Get(ctx, rtx, "lang"); !errors.Is(err, fdblayer.ErrNotFound) {
			t.Fatalf("expected deleted setting 'lang' to return ErrNotFound, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("ReadTransact verification failed: %v", err)
	}
}

func TestRecordLayer_InterleaveCoveringAndAggregates(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	st := fdblayer.NewStoreFromTransaction(tr, dir)

	custRepo := store.NewCustomerRepository()
	invRepo := store.NewCustomerInvoiceRepository()

	// 1. Insert two customers and four interleaved invoices in a single pipelined batch
	if err := st.Apply(ctx,
		custRepo.Insert(&store.Customer{CustomerId: "cust-1", Email: "c1@example.com", Name: "Customer One"}),
		custRepo.Insert(&store.Customer{CustomerId: "cust-2", Email: "c2@example.com", Name: "Customer Two"}),
		invRepo.Insert(&store.CustomerInvoice{
			CustomerId:  "cust-1",
			InvoiceId:   10,
			Status:      store.OrderStatus_ORDER_STATUS_PENDING,
			AmountCents: 1500,
			IssuedAt:    1000,
			Note:        "inv-10-note",
		}),
		invRepo.Insert(&store.CustomerInvoice{
			CustomerId:  "cust-1",
			InvoiceId:   20,
			Status:      store.OrderStatus_ORDER_STATUS_PENDING,
			AmountCents: 2500,
			IssuedAt:    2000,
			Note:        "inv-20-note",
		}),
		invRepo.Insert(&store.CustomerInvoice{
			CustomerId:  "cust-1",
			InvoiceId:   30,
			Status:      store.OrderStatus_ORDER_STATUS_SHIPPED,
			AmountCents: 5000,
			IssuedAt:    3000,
			Note:        "inv-30-note",
		}),
		invRepo.Insert(&store.CustomerInvoice{
			CustomerId:  "cust-2",
			InvoiceId:   100,
			Status:      store.OrderStatus_ORDER_STATUS_PENDING,
			AmountCents: 9900,
			IssuedAt:    1500,
			Note:        "inv-100-note",
		}),
	); err != nil {
		t.Fatalf("Apply initial customers and invoices failed: %v", err)
	}

	// 2. Verify GetWithChildren and parent/child List isolation
	if err := st.ReadTransact(ctx, func(ctx context.Context, rtx fdblayer.ReadTx) error {
		fam, err := custRepo.GetWithChildren(ctx, rtx, "cust-1")
		if err != nil {
			return err
		}
		if fam.Customer == nil || fam.Customer.Name != "Customer One" {
			t.Fatalf("unexpected parent Customer in GetWithChildren: %+v", fam.Customer)
		}
		if len(fam.CustomerInvoices) != 3 ||
			fam.CustomerInvoices[0].InvoiceId != 10 ||
			fam.CustomerInvoices[1].InvoiceId != 20 ||
			fam.CustomerInvoices[2].InvoiceId != 30 {
			t.Fatalf("expected 3 ordered invoices for cust-1, got %+v", fam.CustomerInvoices)
		}

		// Parent List must only return Customer records (2), not interleaved CustomerInvoice records
		allCusts, err := custRepo.List(ctx, rtx, fdblayer.PaginationOptions{})
		if err != nil || len(allCusts.Items) != 2 {
			t.Fatalf("expected 2 customers from List, got %+v, err=%v", allCusts, err)
		}

		// Child ListByCustomerId must only return cust-1's 3 invoices
		cust1Invs, err := invRepo.ListByCustomerId(ctx, rtx, "cust-1", fdblayer.PaginationOptions{})
		if err != nil || len(cust1Invs.Items) != 3 {
			t.Fatalf("expected 3 invoices from ListByCustomerId(cust-1), got %+v, err=%v", cust1Invs, err)
		}

		// Covering index: GetCoveredByStatus returns projected CustomerInvoice with AmountCents populated and Note empty
		covered, err := invRepo.GetCoveredByStatus(ctx, rtx, store.OrderStatus_ORDER_STATUS_PENDING, fdblayer.PaginationOptions{})
		if err != nil || len(covered.Items) != 3 {
			t.Fatalf("expected 3 covered PENDING invoices, got %+v, err=%v", covered, err)
		}
		if covered.Items[0].InvoiceId != 10 || covered.Items[0].AmountCents != 1500 || covered.Items[0].Note != "" {
			t.Fatalf("unexpected covered projection item[0]: %+v", covered.Items[0])
		}

		// Aggregate indexes on cust-1
		cntPending, err := invRepo.GetCountByCustomerIdAndStatus(ctx, rtx, "cust-1", store.OrderStatus_ORDER_STATUS_PENDING)
		if err != nil || cntPending != 2 {
			t.Fatalf("expected count=2 for (cust-1, PENDING), got %d, err=%v", cntPending, err)
		}
		sumPending, err := invRepo.GetSumAmountCentsByCustomerIdAndStatus(ctx, rtx, "cust-1", store.OrderStatus_ORDER_STATUS_PENDING)
		if err != nil || sumPending != 4000 {
			t.Fatalf("expected sum=4000 for (cust-1, PENDING), got %d, err=%v", sumPending, err)
		}
		minAmt, ok, err := invRepo.GetMinAmountCentsByCustomerId(ctx, rtx, "cust-1")
		if err != nil || !ok || minAmt != 1500 {
			t.Fatalf("expected min=1500 (ok=true) for cust-1, got %d (ok=%v), err=%v", minAmt, ok, err)
		}
		maxAmt, ok, err := invRepo.GetMaxAmountCentsByCustomerId(ctx, rtx, "cust-1")
		if err != nil || !ok || maxAmt != 5000 {
			t.Fatalf("expected max=5000 (ok=true) for cust-1, got %d (ok=%v), err=%v", maxAmt, ok, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("ReadTransact failed: %v", err)
	}

	// 3. Update invoice 10 (change status PENDING -> SHIPPED, amount 1500 -> 6000) and invoice 20 (amount 2500 -> 900)
	//    and verify aggregate delta/extrema maintenance
	if err := st.Apply(ctx,
		invRepo.Upsert(&store.CustomerInvoice{
			CustomerId:  "cust-1",
			InvoiceId:   10,
			Status:      store.OrderStatus_ORDER_STATUS_SHIPPED,
			AmountCents: 6000,
			IssuedAt:    1000,
			Note:        "inv-10-updated",
		}),
		invRepo.Upsert(&store.CustomerInvoice{
			CustomerId:  "cust-1",
			InvoiceId:   20,
			Status:      store.OrderStatus_ORDER_STATUS_PENDING,
			AmountCents: 900,
			IssuedAt:    2000,
			Note:        "inv-20-updated",
		}),
	); err != nil {
		t.Fatalf("Upsert inv-10/inv-20 failed: %v", err)
	}

	if err := st.ReadTransact(ctx, func(ctx context.Context, rtx fdblayer.ReadTx) error {
		cntPending, _ := invRepo.GetCountByCustomerIdAndStatus(ctx, rtx, "cust-1", store.OrderStatus_ORDER_STATUS_PENDING)
		sumPending, _ := invRepo.GetSumAmountCentsByCustomerIdAndStatus(ctx, rtx, "cust-1", store.OrderStatus_ORDER_STATUS_PENDING)
		cntShipped, _ := invRepo.GetCountByCustomerIdAndStatus(ctx, rtx, "cust-1", store.OrderStatus_ORDER_STATUS_SHIPPED)
		sumShipped, _ := invRepo.GetSumAmountCentsByCustomerIdAndStatus(ctx, rtx, "cust-1", store.OrderStatus_ORDER_STATUS_SHIPPED)
		if cntPending != 1 || sumPending != 900 || cntShipped != 2 || sumShipped != 11000 {
			t.Fatalf("unexpected aggregates after Upsert: cntP=%d sumP=%d cntS=%d sumS=%d", cntPending, sumPending, cntShipped, sumShipped)
		}
		minAmt, ok, _ := invRepo.GetMinAmountCentsByCustomerId(ctx, rtx, "cust-1")
		maxAmt, okMax, _ := invRepo.GetMaxAmountCentsByCustomerId(ctx, rtx, "cust-1")
		if !ok || minAmt != 900 || !okMax || maxAmt != 6000 {
			t.Fatalf("expected min=900 max=6000 after Upsert, got min=%d max=%d", minAmt, maxAmt)
		}
		return nil
	}); err != nil {
		t.Fatalf("ReadTransact after Upsert failed: %v", err)
	}

	// 4. Cascade delete parent cust-1 and verify all child records, covering indexes, and aggregates for cust-1 are cleaned up
	if err := st.Apply(ctx, custRepo.DeleteMutation("cust-1")); err != nil {
		t.Fatalf("DeleteMutation(cust-1) failed: %v", err)
	}

	if err := st.ReadTransact(ctx, func(ctx context.Context, rtx fdblayer.ReadTx) error {
		if _, err := custRepo.GetWithChildren(ctx, rtx, "cust-1"); !errors.Is(err, fdblayer.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for deleted cust-1, got %v", err)
		}
		cust1Invs, err := invRepo.ListByCustomerId(ctx, rtx, "cust-1", fdblayer.PaginationOptions{})
		if err != nil || len(cust1Invs.Items) != 0 {
			t.Fatalf("expected 0 invoices for cust-1 after cascade delete, got %+v", cust1Invs)
		}
		cntShipped, _ := invRepo.GetCountByCustomerIdAndStatus(ctx, rtx, "cust-1", store.OrderStatus_ORDER_STATUS_SHIPPED)
		sumShipped, _ := invRepo.GetSumAmountCentsByCustomerIdAndStatus(ctx, rtx, "cust-1", store.OrderStatus_ORDER_STATUS_SHIPPED)
		_, okMin, _ := invRepo.GetMinAmountCentsByCustomerId(ctx, rtx, "cust-1")
		_, okMax, _ := invRepo.GetMaxAmountCentsByCustomerId(ctx, rtx, "cust-1")
		if cntShipped != 0 || sumShipped != 0 || okMin || okMax {
			t.Fatalf("expected cust-1 aggregates cleared after cascade delete, got cnt=%d sum=%d okMin=%v okMax=%v", cntShipped, sumShipped, okMin, okMax)
		}
		// cust-2 must remain untouched
		cust2Invs, err := invRepo.ListByCustomerId(ctx, rtx, "cust-2", fdblayer.PaginationOptions{})
		if err != nil || len(cust2Invs.Items) != 1 || cust2Invs.Items[0].InvoiceId != 100 {
			t.Fatalf("expected cust-2 invoice 100 untouched, got %+v, err=%v", cust2Invs, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("ReadTransact after cascade delete failed: %v", err)
	}
}

func TestRecordLayer_KeyRangeAndPageToken(t *testing.T) {
	ctx := context.Background()
	tr, dir, _ := tests.Setup()
	st := fdblayer.NewStoreFromTransaction(tr, dir)

	invRepo := store.NewCustomerInvoiceRepository()
	for _, id := range []int64{10, 20, 30, 40, 50} {
		if err := st.Apply(ctx, invRepo.Insert(&store.CustomerInvoice{
			CustomerId:  "c1",
			InvoiceId:   id,
			Status:      store.OrderStatus_ORDER_STATUS_PENDING,
			AmountCents: id * 100,
			IssuedAt:    id * 10,
		})); err != nil {
			t.Fatalf("Insert invoice %d failed: %v", id, err)
		}
	}

	if err := st.ReadTransact(ctx, func(ctx context.Context, rtx fdblayer.ReadTx) error {
		// 1. KeyRange bounds on primary key suffix (ScanByCustomerIdAndInvoiceId)
		between, err := invRepo.ScanByCustomerIdAndInvoiceId(ctx, rtx, "c1", fdblayer.Between[int64](20, 40), fdblayer.PaginationOptions{})
		if err != nil || len(between.Items) != 3 || between.Items[0].InvoiceId != 20 || between.Items[2].InvoiceId != 40 {
			t.Fatalf("Between(20, 40) expected [20, 30, 40], got %+v, err=%v", between, err)
		}

		halfOpen, err := invRepo.ScanByCustomerIdAndInvoiceId(ctx, rtx, "c1", fdblayer.HalfOpen[int64](20, 40), fdblayer.PaginationOptions{})
		if err != nil || len(halfOpen.Items) != 2 || halfOpen.Items[0].InvoiceId != 20 || halfOpen.Items[1].InvoiceId != 30 {
			t.Fatalf("HalfOpen(20, 40) expected [20, 30], got %+v, err=%v", halfOpen, err)
		}

		openClosed, err := invRepo.ScanByCustomerIdAndInvoiceId(ctx, rtx, "c1", fdblayer.OpenClosedRange[int64](20, 40), fdblayer.PaginationOptions{})
		if err != nil || len(openClosed.Items) != 2 || openClosed.Items[0].InvoiceId != 30 || openClosed.Items[1].InvoiceId != 40 {
			t.Fatalf("OpenClosedRange(20, 40) expected [30, 40], got %+v, err=%v", openClosed, err)
		}

		openInterval, err := invRepo.ScanByCustomerIdAndInvoiceId(ctx, rtx, "c1", fdblayer.OpenInterval[int64](20, 40), fdblayer.PaginationOptions{})
		if err != nil || len(openInterval.Items) != 1 || openInterval.Items[0].InvoiceId != 30 {
			t.Fatalf("OpenInterval(20, 40) expected [30], got %+v, err=%v", openInterval, err)
		}

		gt, err := invRepo.ScanByCustomerIdAndInvoiceId(ctx, rtx, "c1", fdblayer.GreaterThan[int64](30), fdblayer.PaginationOptions{})
		if err != nil || len(gt.Items) != 2 || gt.Items[0].InvoiceId != 40 || gt.Items[1].InvoiceId != 50 {
			t.Fatalf("GreaterThan(30) expected [40, 50], got %+v, err=%v", gt, err)
		}

		le, err := invRepo.ScanByCustomerIdAndInvoiceId(ctx, rtx, "c1", fdblayer.LessOrEqual[int64](30), fdblayer.PaginationOptions{})
		if err != nil || len(le.Items) != 3 || le.Items[0].InvoiceId != 10 || le.Items[2].InvoiceId != 30 {
			t.Fatalf("LessOrEqual(30) expected [10, 20, 30], got %+v, err=%v", le, err)
		}

		// 2. Covered range scan with PageToken pagination (forward and reverse)
		p1, err := invRepo.ScanCoveredByStatusAndIssuedAt(ctx, rtx, store.OrderStatus_ORDER_STATUS_PENDING, fdblayer.Between[int64](100, 400), fdblayer.PaginationOptions{
			Limit: 2,
		})
		if err != nil || len(p1.Items) != 2 || !p1.HasMore || len(p1.NextPageToken) == 0 || p1.Items[0].InvoiceId != 10 || p1.Items[1].InvoiceId != 20 {
			t.Fatalf("unexpected covered range scan page 1: %+v, err=%v", p1, err)
		}

		p2, err := invRepo.ScanCoveredByStatusAndIssuedAt(ctx, rtx, store.OrderStatus_ORDER_STATUS_PENDING, fdblayer.Between[int64](100, 400), fdblayer.PaginationOptions{
			PageToken: p1.NextPageToken,
			Limit:     2,
		})
		if err != nil || len(p2.Items) != 2 || p2.HasMore || len(p2.NextPageToken) != 0 || p2.Items[0].InvoiceId != 30 || p2.Items[1].InvoiceId != 40 {
			t.Fatalf("unexpected covered range scan page 2: %+v, err=%v", p2, err)
		}

		// Reverse covered range scan with PageToken
		rev1, err := invRepo.ScanCoveredByStatusAndIssuedAt(ctx, rtx, store.OrderStatus_ORDER_STATUS_PENDING, fdblayer.Between[int64](200, 500), fdblayer.PaginationOptions{
			Limit:   2,
			Reverse: true,
		})
		if err != nil || len(rev1.Items) != 2 || !rev1.HasMore || rev1.Items[0].InvoiceId != 50 || rev1.Items[1].InvoiceId != 40 {
			t.Fatalf("unexpected reverse covered range scan page 1: %+v, err=%v", rev1, err)
		}
		rev2, err := invRepo.ScanCoveredByStatusAndIssuedAt(ctx, rtx, store.OrderStatus_ORDER_STATUS_PENDING, fdblayer.Between[int64](200, 500), fdblayer.PaginationOptions{
			PageToken: rev1.NextPageToken,
			Limit:     2,
			Reverse:   true,
		})
		if err != nil || len(rev2.Items) != 2 || rev2.HasMore || rev2.Items[0].InvoiceId != 30 || rev2.Items[1].InvoiceId != 20 {
			t.Fatalf("unexpected reverse covered range scan page 2: %+v, err=%v", rev2, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("ReadTransact failed: %v", err)
	}
}



