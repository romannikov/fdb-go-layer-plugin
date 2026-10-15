package ir

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	annotationspb "github.com/romannikov/fdb-layer/proto/fdb-layer"
)

func TestProcessMessage_SkipsUnannotatedHelperMessage(t *testing.T) {
	helperMsg := &protogen.Message{
		GoIdent: protogen.GoIdent{GoName: "Address"},
	}
	opts := &descriptorpb.MessageOptions{}
	got, err := ProcessMessage(helperMsg, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected ProcessMessage to return nil for unannotated helper message, got %+v", got)
	}
}

func TestProcessMessage_ReservedOptionsFailCompilation(t *testing.T) {
	msg := &protogen.Message{
		GoIdent: protogen.GoIdent{GoName: "User"},
	}

	optsSplit := &descriptorpb.MessageOptions{}
	proto.SetExtension(optsSplit, annotationspb.E_SplitLargeRecords, true)
	if _, err := ProcessMessage(msg, optsSplit); err == nil || !strings.Contains(err.Error(), "not implemented yet") {
		t.Fatalf("expected split_large_records to fail with 'not implemented yet', got %v", err)
	}

	optsTTL := &descriptorpb.MessageOptions{}
	proto.SetExtension(optsTTL, annotationspb.E_TtlField, "expires_at")
	if _, err := ProcessMessage(msg, optsTTL); err == nil || !strings.Contains(err.Error(), "not implemented yet") {
		t.Fatalf("expected ttl_field to fail with 'not implemented yet', got %v", err)
	}
}

func TestMapProtoKind_AllKinds(t *testing.T) {
	tests := []struct {
		protoKind protoreflect.Kind
		expected  FieldKind
		isInt     bool
	}{
		{protoreflect.Int32Kind, KindInt32, true},
		{protoreflect.Sint32Kind, KindSint32, true},
		{protoreflect.Sfixed32Kind, KindSfixed32, true},
		{protoreflect.Int64Kind, KindInt64, true},
		{protoreflect.Sint64Kind, KindSint64, true},
		{protoreflect.Sfixed64Kind, KindSfixed64, true},
		{protoreflect.Uint32Kind, KindUint32, true},
		{protoreflect.Fixed32Kind, KindFixed32, true},
		{protoreflect.Uint64Kind, KindUint64, true},
		{protoreflect.Fixed64Kind, KindFixed64, true},
		{protoreflect.FloatKind, KindFloat, false},
		{protoreflect.DoubleKind, KindDouble, false},
		{protoreflect.StringKind, KindString, false},
		{protoreflect.BoolKind, KindBool, false},
		{protoreflect.BytesKind, KindBytes, false},
		{protoreflect.EnumKind, KindEnum, false},
		{protoreflect.MessageKind, KindMessage, false},
	}

	for _, tt := range tests {
		got := MapProtoKind(tt.protoKind)
		if got != tt.expected {
			t.Errorf("MapProtoKind(%v) = %v, want %v", tt.protoKind, got, tt.expected)
		}
		if got.IsInteger() != tt.isInt {
			t.Errorf("Kind(%v).IsInteger() = %v, want %v", got, got.IsInteger(), tt.isInt)
		}
	}
}

func TestReorderQueuePrimaryKeyFields(t *testing.T) {
	input := []FieldSpec{
		{ProtoName: "queue_name", GoName: "QueueName", Kind: KindString},
		{ProtoName: "shard_id", GoName: "ShardId", Kind: KindUint32, IsUnsigned: true},
		{ProtoName: "versionstamp", GoName: "Versionstamp", Kind: KindBytes, IsVersionstamp: true},
	}
	reordered := ReorderQueuePrimaryKeyFields(input)
	if len(reordered) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(reordered))
	}
	if reordered[0].GoName != "QueueName" || reordered[1].GoName != "Versionstamp" || reordered[2].GoName != "ShardId" {
		t.Fatalf("unexpected field order: [%s, %s, %s]", reordered[0].GoName, reordered[1].GoName, reordered[2].GoName)
	}
}

func TestMandatoryTypeID_ZeroFailsAndCollisionDetected(t *testing.T) {
	fields := []FieldSpec{
		{ProtoName: "id", GoName: "Id", Kind: KindString},
	}
	fieldMap := map[string]FieldSpec{
		"id": fields[0],
	}

	// Zero type_id must fail compilation
	if _, err := BuildMessageSpec("User", fields, fieldMap, []string{"id"}, nil, false); err == nil || !strings.Contains(err.Error(), "non-zero (annotations.type_id)") {
		t.Fatalf("expected zero type_id to fail compilation, got %v", err)
	}

	userSpec, err := BuildMessageSpecWithTypeID("User", fields, fieldMap, []string{"id"}, nil, false, 1)
	if err != nil {
		t.Fatalf("BuildMessageSpecWithTypeID failed: %v", err)
	}
	if userSpec.TypeID != 1 {
		t.Fatalf("expected explicit TypeID 1, got %d", userSpec.TypeID)
	}

	accountSpec, err := BuildMessageSpecWithTypeID("Account", fields, fieldMap, []string{"id"}, nil, false, 42)
	if err != nil {
		t.Fatalf("BuildMessageSpecWithTypeID failed: %v", err)
	}
	if accountSpec.TypeID != 42 {
		t.Fatalf("expected explicit TypeID 42, got %d", accountSpec.TypeID)
	}

	// Distinct TypeIDs pass validation
	if err := ValidateUniqueTypeIDs([]MessageSpec{*userSpec, *accountSpec}); err != nil {
		t.Fatalf("expected distinct TypeIDs to pass validation, got %v", err)
	}

	// Colliding TypeIDs fail validation
	collidingSpec, err := BuildMessageSpecWithTypeID("Profile", fields, fieldMap, []string{"id"}, nil, false, 42)
	if err != nil {
		t.Fatalf("BuildMessageSpecWithTypeID failed: %v", err)
	}
	if err := ValidateUniqueTypeIDs([]MessageSpec{*accountSpec, *collidingSpec}); err == nil || !strings.Contains(err.Error(), "collision") {
		t.Fatalf("expected message type ID collision error, got %v", err)
	}
}

func TestBuildSecondaryIndex_MandatoryIDAndRichOptions(t *testing.T) {
	fieldMap := map[string]FieldSpec{
		"id":          {ProtoName: "id", GoName: "Id", Kind: KindString},
		"email":       {ProtoName: "email", GoName: "Email", Kind: KindString},
		"name":        {ProtoName: "name", GoName: "Name", Kind: KindString},
		"status":      {ProtoName: "status", GoName: "Status", Kind: KindString},
		"total_cents": {ProtoName: "total_cents", GoName: "TotalCents", Kind: KindInt64},
		"tags":        {ProtoName: "tags", GoName: "Tags", Kind: KindString, IsRepeated: true},
		"roles":       {ProtoName: "roles", GoName: "Roles", Kind: KindString, IsRepeated: true},
		"counter":     {ProtoName: "counter", GoName: "Counter", Kind: KindInt64, Mutation: MutationAdd},
	}
	pkSet := map[string]bool{"id": true}

	// Zero id must fail compilation
	if _, err := BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Fields: []string{"email"},
		Id:     0,
	}, fieldMap, pkSet, nil, "User"); err == nil || !strings.Contains(err.Error(), "non-zero id") {
		t.Fatalf("expected zero secondary index id to fail, got %v", err)
	}

	usedHashes := make(map[int64]string)
	si, err := BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Id:      42,
		Fields:  []string{"email"},
		Unique:  true,
		Sparse:  true,
		Storing: []string{"name"},
	}, fieldMap, pkSet, usedHashes, "User")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if si.IndexID != 42 || !si.IsUnique || !si.IsSparse || len(si.StoringFields) != 1 || si.StoringFields[0].ProtoName != "name" {
		t.Fatalf("unexpected secondary index spec: %+v", si)
	}

	// Colliding explicit ID should fail
	_, err = BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Id:     42,
		Fields: []string{"name"},
	}, fieldMap, pkSet, usedHashes, "User")
	if err == nil || !strings.Contains(err.Error(), "collision") {
		t.Fatalf("expected collision error, got %v", err)
	}

	// Multiple repeated fields in one index should fail
	_, err = BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Id:     2,
		Fields: []string{"tags", "roles"},
	}, fieldMap, pkSet, usedHashes, "User")
	if err == nil || !strings.Contains(err.Error(), "multiple repeated fields") {
		t.Fatalf("expected multiple repeated fields error, got %v", err)
	}

	// Unique on repeated field should fail
	_, err = BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Id:     3,
		Fields: []string{"tags"},
		Unique: true,
	}, fieldMap, pkSet, usedHashes, "User")
	if err == nil || !strings.Contains(err.Error(), "cannot index repeated field") {
		t.Fatalf("expected unique fan-out error, got %v", err)
	}

	// Storing field already in PK or index key should fail
	_, err = BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Id:      4,
		Fields:  []string{"email"},
		Storing: []string{"id"},
	}, fieldMap, pkSet, usedHashes, "User")
	if err == nil || !strings.Contains(err.Error(), "already part of the index key or primary key") {
		t.Fatalf("expected storing PK overlap error, got %v", err)
	}

	// Valid Aggregate COUNT and SUM indexes
	aggCount, err := BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Id:        5,
		Fields:    []string{"status"},
		Aggregate: annotationspb.IndexAggregateType_AGGREGATE_COUNT,
	}, fieldMap, pkSet, usedHashes, "Order")
	if err != nil || aggCount.Aggregate != AggregateCount {
		t.Fatalf("expected valid COUNT aggregate index, got %+v, err=%v", aggCount, err)
	}

	aggSum, err := BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Id:             6,
		Fields:         []string{"status"},
		Aggregate:      annotationspb.IndexAggregateType_AGGREGATE_SUM,
		AggregateField: "total_cents",
	}, fieldMap, pkSet, usedHashes, "Order")
	if err != nil || aggSum.Aggregate != AggregateSum || aggSum.AggregateField == nil || aggSum.AggregateField.ProtoName != "total_cents" {
		t.Fatalf("expected valid SUM aggregate index, got %+v, err=%v", aggSum, err)
	}

	// Invalid aggregate combinations
	if _, err := BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Id:             7,
		Fields:         []string{"status"},
		Aggregate:      annotationspb.IndexAggregateType_AGGREGATE_COUNT,
		AggregateField: "total_cents",
	}, fieldMap, pkSet, nil, "Order"); err == nil || !strings.Contains(err.Error(), "must not specify aggregate_field") {
		t.Fatalf("expected COUNT with aggregate_field error, got %v", err)
	}

	if _, err := BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Id:        8,
		Fields:    []string{"status"},
		Aggregate: annotationspb.IndexAggregateType_AGGREGATE_SUM,
	}, fieldMap, pkSet, nil, "Order"); err == nil || !strings.Contains(err.Error(), "must specify aggregate_field") {
		t.Fatalf("expected SUM without aggregate_field error, got %v", err)
	}

	if _, err := BuildSecondaryIndexWithPK(&annotationspb.SecondaryIndex{
		Id:             9,
		Fields:         []string{"status"},
		Aggregate:      annotationspb.IndexAggregateType_AGGREGATE_SUM,
		AggregateField: "name",
	}, fieldMap, pkSet, nil, "Order"); err == nil || !strings.Contains(err.Error(), "must be an integer type") {
		t.Fatalf("expected SUM on non-integer field error, got %v", err)
	}
}

func TestResolveInterleave(t *testing.T) {
	userID := FieldSpec{ProtoName: "user_id", GoName: "UserId", Kind: KindString}
	orderID := FieldSpec{ProtoName: "order_id", GoName: "OrderId", Kind: KindInt64}
	totalCents := FieldSpec{ProtoName: "total_cents", GoName: "TotalCents", Kind: KindInt64}

	userMsg, err := BuildMessageSpecFull(
		"User",
		[]FieldSpec{userID},
		map[string]FieldSpec{"user_id": userID},
		[]string{"user_id"},
		nil,
		false,
		1,
		nil,
	)
	if err != nil {
		t.Fatalf("failed to build User: %v", err)
	}

	orderMsg, err := BuildMessageSpecFull(
		"Order",
		[]FieldSpec{userID, orderID, totalCents},
		map[string]FieldSpec{"user_id": userID, "order_id": orderID, "total_cents": totalCents},
		[]string{"user_id", "order_id"},
		nil,
		false,
		2,
		&annotationspb.InterleaveSpec{
			Parent:          "User",
			OnDeleteCascade: true,
		},
	)
	if err != nil {
		t.Fatalf("failed to build Order: %v", err)
	}

	resolved, err := ResolveInterleave([]MessageSpec{*userMsg, *orderMsg})
	if err != nil {
		t.Fatalf("ResolveInterleave failed: %v", err)
	}
	if len(resolved[0].Children) != 1 || resolved[0].Children[0].Name != "Order" || !resolved[0].Children[0].OnDeleteCascade {
		t.Fatalf("expected User to have 1 child Order with OnDeleteCascade, got %+v", resolved[0].Children)
	}
	if resolved[1].Interleave == nil || resolved[1].Interleave.ParentTypeID != 1 || len(resolved[1].Interleave.ChildPKSuffixFields) != 1 {
		t.Fatalf("unexpected Order InterleaveSpec: %+v", resolved[1].Interleave)
	}

	// Missing parent must fail
	badParentOrder := *orderMsg
	badParentOrder.Interleave = &InterleaveSpec{ParentName: "NonExistent"}
	if _, err := ResolveInterleave([]MessageSpec{*userMsg, badParentOrder}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected missing parent error, got %v", err)
	}
}

func TestValidateFieldSpec_Errors(t *testing.T) {
	// Versionstamp on non-bytes field must fail
	err := ValidateFieldSpec(FieldSpec{
		ProtoName:      "vs",
		Kind:           KindString,
		IsVersionstamp: true,
	}, "TaskMessage")
	if err == nil || !strings.Contains(err.Error(), "is_versionstamp") {
		t.Fatalf("expected versionstamp type error, got %v", err)
	}

	// Atomic mutation on non-integer field must fail
	err = ValidateFieldSpec(FieldSpec{
		ProtoName: "score",
		Kind:      KindString,
		Mutation:  MutationAdd,
	}, "Counter")
	if err == nil || !strings.Contains(err.Error(), "atomic mutation") {
		t.Fatalf("expected atomic mutation type error, got %v", err)
	}
}

func TestBuildMessageSpec_ValidationErrors(t *testing.T) {
	fields := []FieldSpec{
		{ProtoName: "id", GoName: "Id", Kind: KindString},
		{ProtoName: "tags", GoName: "Tags", Kind: KindString, IsRepeated: true},
		{ProtoName: "count", GoName: "Count", Kind: KindInt64, Mutation: MutationAdd},
		{ProtoName: "vs1", GoName: "Vs1", Kind: KindBytes, IsVersionstamp: true},
		{ProtoName: "vs2", GoName: "Vs2", Kind: KindBytes, IsVersionstamp: true},
	}
	fieldMap := make(map[string]FieldSpec)
	for _, f := range fields {
		fieldMap[f.ProtoName] = f
	}

	// 1. Multiple is_versionstamp = true fields in one message
	_, err := BuildMessageSpecWithTypeID("InvalidMsg", fields, fieldMap, []string{"id", "vs1", "vs2"}, nil, false, 1)
	if err == nil || !strings.Contains(err.Error(), "multiple is_versionstamp=true fields") {
		t.Fatalf("expected multiple versionstamp error, got %v", err)
	}

	// Remove vs2 for subsequent tests
	singleVsFields := fields[:4]
	singleVsMap := map[string]FieldSpec{
		"id":    fields[0],
		"tags":  fields[1],
		"count": fields[2],
		"vs1":   fields[3],
	}

	// 2. Non-existent primary_key field
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", singleVsFields, singleVsMap, []string{"missing"}, nil, false, 1)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected missing primary key error, got %v", err)
	}

	// 3. Repeated primary_key field
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", singleVsFields, singleVsMap, []string{"tags", "vs1"}, nil, false, 1)
	if err == nil || !strings.Contains(err.Error(), "cannot be repeated") {
		t.Fatalf("expected repeated primary key error, got %v", err)
	}

	// 4. Atomic mutation field in primary_key
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", singleVsFields, singleVsMap, []string{"count", "vs1"}, nil, false, 1)
	if err == nil || !strings.Contains(err.Error(), "cannot have an atomic mutation") {
		t.Fatalf("expected atomic mutation primary key error, got %v", err)
	}

	// 5. Versionstamp field not in primary_key
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", singleVsFields, singleVsMap, []string{"id"}, nil, false, 1)
	if err == nil || !strings.Contains(err.Error(), "is not in primary_key") {
		t.Fatalf("expected versionstamp not in primary_key error, got %v", err)
	}

	// 6. Queue without versionstamp primary_key field
	noVsFields := fields[:3]
	noVsMap := map[string]FieldSpec{
		"id":    fields[0],
		"tags":  fields[1],
		"count": fields[2],
	}
	_, err = BuildMessageSpecWithTypeID("InvalidQueue", noVsFields, noVsMap, []string{"id"}, nil, true, 1)
	if err == nil || !strings.Contains(err.Error(), "must have an is_versionstamp=true primary key field") {
		t.Fatalf("expected queue missing versionstamp error, got %v", err)
	}

	// 7. Secondary index referencing non-existent field
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", noVsFields, noVsMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"unknown_idx"}},
	}, false, 1)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected secondary index field not found error, got %v", err)
	}

	// 8. Secondary index referencing atomic mutation field
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", noVsFields, noVsMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"count"}},
	}, false, 1)
	if err == nil || !strings.Contains(err.Error(), "cannot have an atomic mutation") {
		t.Fatalf("expected secondary index on atomic mutation field error, got %v", err)
	}

	// 9. Secondary index referencing a versionstamp field
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", singleVsFields, singleVsMap, []string{"id", "vs1"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"vs1"}},
	}, false, 1)
	if err == nil || !strings.Contains(err.Error(), "cannot be a versionstamp field") {
		t.Fatalf("expected secondary index on versionstamp field error, got %v", err)
	}

	// 10. Duplicate primary key field
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", noVsFields, noVsMap, []string{"id", "id"}, nil, false, 1)
	if err == nil || !strings.Contains(err.Error(), "duplicate primary key field") {
		t.Fatalf("expected duplicate primary key field error, got %v", err)
	}

	// 11. Queue with only 1 primary key field (missing partition key)
	_, err = BuildMessageSpecWithTypeID("InvalidQueue", singleVsFields, singleVsMap, []string{"vs1"}, nil, true, 1)
	if err == nil || !strings.Contains(err.Error(), "at least 2 primary key fields") {
		t.Fatalf("expected queue with 1 PK field error, got %v", err)
	}

	// 12. Empty secondary index fields or duplicate secondary index fields
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", noVsFields, noVsMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{}},
	}, false, 1)
	if err == nil || !strings.Contains(err.Error(), "must specify at least one field") {
		t.Fatalf("expected empty secondary index error, got %v", err)
	}
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", noVsFields, noVsMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"id", "id"}},
	}, false, 1)
	if err == nil || !strings.Contains(err.Error(), "duplicate field") {
		t.Fatalf("expected duplicate secondary index field error, got %v", err)
	}

	// 13. Unsupported key types (float, double, message) in primary_key or secondary_index
	for _, badKind := range []FieldKind{KindFloat, KindDouble, KindMessage, KindUnknown} {
		badField := FieldSpec{ProtoName: "bad", GoName: "Bad", Kind: badKind}
		badFields := []FieldSpec{fields[0], badField}
		badMap := map[string]FieldSpec{
			"id":  fields[0],
			"bad": badField,
		}
		if _, err := BuildMessageSpecWithTypeID("InvalidMsg", badFields, badMap, []string{"bad"}, nil, false, 1); err == nil || !strings.Contains(err.Error(), "unsupported kind") {
			t.Fatalf("expected unsupported primary key type error for kind %v, got %v", badKind, err)
		}
		if _, err := BuildMessageSpecWithTypeID("InvalidMsg", badFields, badMap, []string{"id"}, []*annotationspb.SecondaryIndex{
			{Id: 1, Fields: []string{"bad"}},
		}, false, 1); err == nil || !strings.Contains(err.Error(), "unsupported kind") {
			t.Fatalf("expected unsupported secondary index key type error for kind %v, got %v", badKind, err)
		}
	}
}

func TestEffectiveIndexPrefixesAndRangeScans(t *testing.T) {
	status := FieldSpec{ProtoName: "status", GoName: "Status", Kind: KindString}
	createdAt := FieldSpec{ProtoName: "created_at", GoName: "CreatedAt", Kind: KindInt64}
	region := FieldSpec{ProtoName: "region", GoName: "Region", Kind: KindString}
	totalCents := FieldSpec{ProtoName: "total_cents", GoName: "TotalCents", Kind: KindInt64}

	indexes := []SecondaryIndexSpec{
		{
			IndexID:       1,
			Fields:        []FieldSpec{status, createdAt, region},
			StoringFields: []FieldSpec{totalCents},
		},
		{
			IndexID: 2,
			// Exact match with the 1-field prefix of index 0; should prevent index 0 from emitting a duplicate "Status" prefix.
			Fields: []FieldSpec{status},
		},
		{
			IndexID: 3,
			// Shares the 2-field prefix "StatusCreatedAt" with index 0; only index 0 should emit "StatusCreatedAt".
			Fields: []FieldSpec{status, createdAt, status},
		},
	}

	indexes = PopulateIndexPrefixes(indexes)

	// Index 0 has 3 fields: Status, CreatedAt, Region.
	// Prefix "Status" collides with full index 1 ("Status"), so only [status, createdAt] should be emitted in Prefixes.
	if len(indexes[0].Prefixes) != 1 {
		t.Fatalf("expected 1 prefix on indexes[0], got %d: %+v", len(indexes[0].Prefixes), indexes[0].Prefixes)
	}
	if len(indexes[0].Prefixes[0].Fields) != 2 || indexes[0].Prefixes[0].Fields[0].GoName != "Status" || indexes[0].Prefixes[0].Fields[1].GoName != "CreatedAt" {
		t.Fatalf("unexpected prefix on indexes[0]: %+v", indexes[0].Prefixes[0])
	}

	// CoveredPrefixes on index 0 should have BOTH [status] and [status, createdAt] since index 1 is NOT a covering index!
	if len(indexes[0].CoveredPrefixes) != 2 {
		t.Fatalf("expected 2 covered prefixes on indexes[0], got %d: %+v", len(indexes[0].CoveredPrefixes), indexes[0].CoveredPrefixes)
	}
	if len(indexes[0].RangeScans) != 3 {
		t.Fatalf("expected 3 range scans on indexes[0], got %d", len(indexes[0].RangeScans))
	}

	// Index 1 is single-field -> 0 prefixes, and its RangeScan "Status" was already claimed by index 0.
	if len(indexes[1].Prefixes) != 0 || len(indexes[1].RangeScans) != 0 {
		t.Fatalf("expected 0 prefixes and 0 range scans on indexes[1], got prefixes=%d, rangeScans=%d", len(indexes[1].Prefixes), len(indexes[1].RangeScans))
	}

	// Index 2's prefixes ("Status" and "StatusCreatedAt") are already covered -> 0 prefixes, and only 3-field range scan remains.
	if len(indexes[2].Prefixes) != 0 || len(indexes[2].RangeScans) != 1 {
		t.Fatalf("expected 0 prefixes and 1 range scan on indexes[2], got prefixes=%d, rangeScans=%d", len(indexes[2].Prefixes), len(indexes[2].RangeScans))
	}
}

func TestFileSpec_AnnotatedMessages(t *testing.T) {
	idField := FieldSpec{ProtoName: "id", GoName: "Id", Kind: KindString}
	file := &FileSpec{
		GoPackageName: "store",
		Messages: []MessageSpec{
			{Name: "Helper"},
			{Name: "User", TypeID: 1, PrimaryKeyFields: []FieldSpec{idField}},
			{Name: "Order", TypeID: 2, PrimaryKeyFields: []FieldSpec{idField}},
		},
	}
	annotated := file.AnnotatedMessages()
	if len(annotated) != 2 || annotated[0].Name != "User" || annotated[1].Name != "Order" {
		t.Fatalf("unexpected AnnotatedMessages: %+v", annotated)
	}
}

func TestNilGuards(t *testing.T) {
	if _, err := ParsePlugin(nil); err == nil {
		t.Fatal("expected error for ParsePlugin(nil)")
	}
	if _, err := ProcessMessage(nil, nil); err == nil {
		t.Fatal("expected error for ProcessMessage(nil, nil)")
	}
	if _, err := ProcessMessageAll(nil, nil); err == nil {
		t.Fatal("expected error for ProcessMessageAll(nil, nil)")
	}
	if got := ProcessEnum(nil); got.Name != "" || len(got.Values) != 0 {
		t.Fatalf("expected empty EnumSpec for ProcessEnum(nil), got %+v", got)
	}
	if got := ProcessEnum(&protogen.Enum{GoIdent: protogen.GoIdent{GoName: "Role"}}); got.Name != "" {
		t.Fatalf("expected empty EnumSpec when Desc is nil, got %+v", got)
	}
	fieldMap := map[string]FieldSpec{
		"email": {ProtoName: "email", GoName: "Email", Kind: KindString},
	}
	if _, err := BuildSecondaryIndex(nil, fieldMap, nil, "User"); err == nil {
		t.Fatal("expected error for BuildSecondaryIndex(nil, ...)")
	}
	si, err := BuildSecondaryIndex(&annotationspb.SecondaryIndex{Id: 1, Fields: []string{"email"}}, fieldMap, nil, "User")
	if err != nil || si.IndexID != 1 {
		t.Fatalf("expected BuildSecondaryIndex with nil usedHashes to succeed, got si=%+v, err=%v", si, err)
	}
}
