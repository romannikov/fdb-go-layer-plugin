package ir

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"

	annotationspb "github.com/romannikov/fdb-layer/proto/fdb-layer"
	"github.com/romannikov/fdb-layer/tests/go/store"
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

func TestExplicitTypeID_RequiredAndCollision(t *testing.T) {
	fields := []FieldSpec{
		{ProtoName: "id", GoName: "Id", Kind: KindString},
	}
	fieldMap := map[string]FieldSpec{
		"id": fields[0],
	}

	// Missing type_id (0) must fail
	if _, err := BuildMessageSpecWithTypeID("User", fields, fieldMap, []string{"id"}, nil, false, 0); err == nil || !strings.Contains(err.Error(), "explicit non-zero option (annotations.type_id) is required") {
		t.Fatalf("expected missing type_id error, got %v", err)
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

func TestBuildSecondaryIndex_ExplicitIDAndCollision(t *testing.T) {
	fieldMap := map[string]FieldSpec{
		"email": {ProtoName: "email", GoName: "Email", Kind: KindString},
		"name":  {ProtoName: "name", GoName: "Name", Kind: KindString},
		"tags":  {ProtoName: "tags", GoName: "Tags", Kind: KindString, IsRepeated: true},
		"roles": {ProtoName: "roles", GoName: "Roles", Kind: KindString, IsRepeated: true},
	}

	usedHashes := make(map[int64]string)

	// Missing id (0) must fail
	if _, err := BuildSecondaryIndex(&annotationspb.SecondaryIndex{
		Fields: []string{"email"},
		Id:     0,
	}, fieldMap, usedHashes, "User"); err == nil || !strings.Contains(err.Error(), "requires an explicit non-zero id") {
		t.Fatalf("expected missing index id error, got %v", err)
	}

	si, err := BuildSecondaryIndex(&annotationspb.SecondaryIndex{
		Fields: []string{"email"},
		Id:     42,
	}, fieldMap, usedHashes, "User")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if si.IndexID != 42 {
		t.Fatalf("expected explicit IndexID 42, got %d", si.IndexID)
	}

	// Colliding explicit ID should fail
	_, err = BuildSecondaryIndex(&annotationspb.SecondaryIndex{
		Fields: []string{"name"},
		Id:     42,
	}, fieldMap, usedHashes, "User")
	if err == nil || !strings.Contains(err.Error(), "collision") {
		t.Fatalf("expected collision error, got %v", err)
	}

	// Multiple repeated fields in one index should fail
	_, err = BuildSecondaryIndex(&annotationspb.SecondaryIndex{
		Fields: []string{"tags", "roles"},
		Id:     43,
	}, fieldMap, usedHashes, "User")
	if err == nil || !strings.Contains(err.Error(), "multiple repeated fields") {
		t.Fatalf("expected multiple repeated fields error, got %v", err)
	}
}

func TestUnimplementedOptions_Rejected(t *testing.T) {
	fieldMap := map[string]FieldSpec{
		"email": {ProtoName: "email", GoName: "Email", Kind: KindString},
	}

	// SecondaryIndex reserved options
	unimplementedIdxCases := []struct {
		name string
		idx  *annotationspb.SecondaryIndex
	}{
		{"unique", &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"email"}, Unique: true}},
		{"storing", &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"email"}, Storing: []string{"email"}}},
		{"sparse", &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"email"}, Sparse: true}},
	}
	for _, tc := range unimplementedIdxCases {
		t.Run("index_"+tc.name, func(t *testing.T) {
			_, err := BuildSecondaryIndex(tc.idx, fieldMap, nil, "User")
			if err == nil || !strings.Contains(err.Error(), "is reserved and not implemented yet") {
				t.Fatalf("expected reserved option error for %s, got %v", tc.name, err)
			}
		})
	}

	// Field option: mutation
	for _, mut := range []MutationType{MutationAdd, MutationMax, MutationMin} {
		err := ValidateFieldSpec(FieldSpec{
			ProtoName: "views",
			Kind:      KindInt64,
			Mutation:  mut,
		}, "User")
		if err == nil || !strings.Contains(err.Error(), "option (annotations.mutation) is reserved and not implemented yet") {
			t.Fatalf("expected reserved mutation option error, got %v", err)
		}
	}

	// Message options: interleave, split_large_records, ttl_field
	msg := &protogen.Message{GoIdent: protogen.GoIdent{GoName: "User"}}

	optsInterleave := &descriptorpb.MessageOptions{}
	proto.SetExtension(optsInterleave, annotationspb.E_Interleave, &annotationspb.InterleaveSpec{Parent: "Account"})
	if _, err := ProcessMessageAll(msg, optsInterleave); err == nil || !strings.Contains(err.Error(), "option (annotations.interleave) is reserved and not implemented yet") {
		t.Fatalf("expected interleave reserved error, got %v", err)
	}

	optsSplit := &descriptorpb.MessageOptions{}
	proto.SetExtension(optsSplit, annotationspb.E_SplitLargeRecords, true)
	if _, err := ProcessMessageAll(msg, optsSplit); err == nil || !strings.Contains(err.Error(), "option (annotations.split_large_records) is reserved and not implemented yet") {
		t.Fatalf("expected split_large_records reserved error, got %v", err)
	}

	optsTTL := &descriptorpb.MessageOptions{}
	proto.SetExtension(optsTTL, annotationspb.E_TtlField, "expires_at")
	if _, err := ProcessMessageAll(msg, optsTTL); err == nil || !strings.Contains(err.Error(), "option (annotations.ttl_field) is reserved and not implemented yet") {
		t.Fatalf("expected ttl_field reserved error, got %v", err)
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
}

func TestBuildMessageSpec_ValidationErrors(t *testing.T) {
	fields := []FieldSpec{
		{ProtoName: "id", GoName: "Id", Kind: KindString},
		{ProtoName: "tags", GoName: "Tags", Kind: KindString, IsRepeated: true},
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
	singleVsFields := fields[:3]
	singleVsMap := map[string]FieldSpec{
		"id":   fields[0],
		"tags": fields[1],
		"vs1":  fields[2],
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

	// 4. Versionstamp field not in primary_key
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", singleVsFields, singleVsMap, []string{"id"}, nil, false, 1)
	if err == nil || !strings.Contains(err.Error(), "is not in primary_key") {
		t.Fatalf("expected versionstamp not in primary_key error, got %v", err)
	}

	// 5. Queue without versionstamp primary_key field
	noVsFields := fields[:2]
	noVsMap := map[string]FieldSpec{
		"id":   fields[0],
		"tags": fields[1],
	}
	_, err = BuildMessageSpecWithTypeID("InvalidQueue", noVsFields, noVsMap, []string{"id"}, nil, true, 1)
	if err == nil || !strings.Contains(err.Error(), "must have an is_versionstamp=true primary key field") {
		t.Fatalf("expected queue missing versionstamp error, got %v", err)
	}

	// 6. Secondary index referencing non-existent field
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", noVsFields, noVsMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"unknown_idx"}},
	}, false, 1)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected secondary index field not found error, got %v", err)
	}

	// 7. Secondary index referencing a versionstamp field
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", singleVsFields, singleVsMap, []string{"id", "vs1"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"vs1"}},
	}, false, 1)
	if err == nil || !strings.Contains(err.Error(), "cannot be a versionstamp field") {
		t.Fatalf("expected secondary index on versionstamp field error, got %v", err)
	}

	// 8. Duplicate primary key field
	_, err = BuildMessageSpecWithTypeID("InvalidMsg", noVsFields, noVsMap, []string{"id", "id"}, nil, false, 1)
	if err == nil || !strings.Contains(err.Error(), "duplicate primary key field") {
		t.Fatalf("expected duplicate primary key field error, got %v", err)
	}

	// 9. Queue with only 1 primary key field (missing partition key)
	_, err = BuildMessageSpecWithTypeID("InvalidQueue", singleVsFields, singleVsMap, []string{"vs1"}, nil, true, 1)
	if err == nil || !strings.Contains(err.Error(), "at least 2 primary key fields") {
		t.Fatalf("expected queue with 1 PK field error, got %v", err)
	}

	// 10. Empty secondary index fields or duplicate secondary index fields
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

	// 11. Repeated versionstamp field in ValidateFieldSpec
	err = ValidateFieldSpec(FieldSpec{
		ProtoName:      "vs",
		Kind:           KindBytes,
		IsVersionstamp: true,
		IsRepeated:     true,
	}, "InvalidMsg")
	if err == nil || !strings.Contains(err.Error(), "cannot be repeated") {
		t.Fatalf("expected repeated versionstamp error, got %v", err)
	}

	// 12. ReorderQueuePrimaryKeyFields when versionstamp is first in 2-field PKs
	reordered2 := ReorderQueuePrimaryKeyFields([]FieldSpec{fields[2], fields[0]})
	if reordered2[0].ProtoName != "id" || reordered2[1].ProtoName != "vs1" {
		t.Fatalf("expected [id, vs1], got [%s, %s]", reordered2[0].ProtoName, reordered2[1].ProtoName)
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

func TestBuildMessageSpec_DuplicateSecondaryIndexFieldsRejected(t *testing.T) {
	fields := []FieldSpec{
		{ProtoName: "id", GoName: "Id", Kind: KindString},
		{ProtoName: "status", GoName: "Status", Kind: KindString},
		{ProtoName: "created_at", GoName: "CreatedAt", Kind: KindInt64},
	}
	fieldMap := map[string]FieldSpec{
		"id":         fields[0],
		"status":     fields[1],
		"created_at": fields[2],
	}

	_, err := BuildMessageSpecWithTypeID("Order", fields, fieldMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"status", "created_at"}},
		{Id: 2, Fields: []string{"status", "created_at"}},
	}, false, 100)
	if err == nil || !strings.Contains(err.Error(), "duplicate secondary index") {
		t.Fatalf("expected duplicate secondary index error, got %v", err)
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

func TestBuildSecondaryIndex_Aggregates(t *testing.T) {
	fields := []FieldSpec{
		{ProtoName: "id", GoName: "Id", Kind: KindString},
		{ProtoName: "tenant_id", GoName: "TenantId", Kind: KindString},
		{ProtoName: "status", GoName: "Status", Kind: KindString},
		{ProtoName: "total_cents", GoName: "TotalCents", Kind: KindInt64},
		{ProtoName: "items_count", GoName: "ItemsCount", Kind: KindUint32, IsUnsigned: true},
		{ProtoName: "weights", GoName: "Weights", Kind: KindInt64, IsRepeated: true},
	}
	fieldMap := make(map[string]FieldSpec, len(fields))
	for _, f := range fields {
		fieldMap[f.ProtoName] = f
	}

	// Valid coexistence of standard and aggregate indexes on overlapping grouping fields (and ungrouped)
	msg, err := BuildMessageSpecWithTypeID("Order", fields, fieldMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"tenant_id", "status"}},
		{Id: 2, Fields: []string{"tenant_id", "status"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_COUNT},
		{Id: 3, Fields: []string{}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_COUNT},
		{Id: 4, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_SUM, AggregateField: "total_cents"},
		{Id: 5, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_MIN, AggregateField: "total_cents"},
		{Id: 6, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_MAX, AggregateField: "total_cents"},
		{Id: 7, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_SUM, AggregateField: "items_count"},
	}, false, 10)
	if err != nil {
		t.Fatalf("expected valid aggregate indexes to succeed, got %v", err)
	}
	if len(msg.SecondaryIndexes) != 7 {
		t.Fatalf("expected 7 indexes, got %d", len(msg.SecondaryIndexes))
	}
	if msg.SecondaryIndexes[0].IsAggregate() {
		t.Fatal("expected index 0 to be non-aggregate")
	}
	if !msg.SecondaryIndexes[1].IsAggregate() || msg.SecondaryIndexes[1].Aggregate != AggregateCount {
		t.Fatalf("expected index 1 to be AggregateCount, got %+v", msg.SecondaryIndexes[1])
	}
	if msg.SecondaryIndexes[3].Aggregate != AggregateSum || msg.SecondaryIndexes[3].AggregateField.GoName != "TotalCents" {
		t.Fatalf("unexpected sum index spec: %+v", msg.SecondaryIndexes[3])
	}

	// Duplicate aggregate index rejected
	_, err = BuildMessageSpecWithTypeID("Order", fields, fieldMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_SUM, AggregateField: "total_cents"},
		{Id: 2, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_SUM, AggregateField: "total_cents"},
	}, false, 10)
	if err == nil || !strings.Contains(err.Error(), "duplicate secondary index") {
		t.Fatalf("expected duplicate aggregate index error, got %v", err)
	}

	// Invalid aggregate configurations
	invalidCases := []struct {
		name string
		idx  *annotationspb.SecondaryIndex
		want string
	}{
		{
			name: "aggregate_field_on_none",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"tenant_id"}, AggregateField: "total_cents"},
			want: "aggregate_field cannot be set",
		},
		{
			name: "aggregate_field_on_count",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_COUNT, AggregateField: "total_cents"},
			want: "aggregate_field cannot be set",
		},
		{
			name: "missing_aggregate_field_on_sum",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_SUM},
			want: "aggregate_field is required",
		},
		{
			name: "missing_aggregate_field_on_min",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_MIN},
			want: "aggregate_field is required",
		},
		{
			name: "missing_aggregate_field_on_max",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_MAX},
			want: "aggregate_field is required",
		},
		{
			name: "unknown_aggregate_field",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_SUM, AggregateField: "nonexistent"},
			want: "not found",
		},
		{
			name: "repeated_aggregate_field",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_SUM, AggregateField: "weights"},
			want: "cannot be repeated",
		},
		{
			name: "non_integer_aggregate_field",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_SUM, AggregateField: "status"},
			want: "must be an integer type",
		},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildSecondaryIndex(tc.idx, fieldMap, nil, "Order")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestBuildSecondaryIndex_Mutations(t *testing.T) {
	fields := []FieldSpec{
		{ProtoName: "id", GoName: "Id", Kind: KindString},
		{ProtoName: "tenant_id", GoName: "TenantId", Kind: KindString},
		{ProtoName: "total_cents", GoName: "TotalCents", Kind: KindInt64},
		{ProtoName: "tags", GoName: "Tags", Kind: KindString, IsRepeated: true},
	}
	fieldMap := make(map[string]FieldSpec, len(fields))
	for _, f := range fields {
		fieldMap[f.ProtoName] = f
	}

	msg, err := BuildMessageSpecWithTypeID("Post", fields, fieldMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"id"}, Mutation: annotationspb.MutationType_MUTATION_COUNT},
		{Id: 2, Fields: []string{"id"}, Mutation: annotationspb.MutationType_MUTATION_COUNT, Name: "views"},
		{Id: 3, Fields: []string{"id"}, Mutation: annotationspb.MutationType_MUTATION_ADD, Name: "likes"},
		{Id: 4, Fields: []string{"id"}, Mutation: annotationspb.MutationType_MUTATION_MIN, Name: "latency"},
		{Id: 5, Fields: []string{"id"}, Mutation: annotationspb.MutationType_MUTATION_MAX, Name: "max_latency"},
		{Id: 6, Fields: []string{}, Mutation: annotationspb.MutationType_MUTATION_COUNT, Name: "global_hits"},
	}, false, 20)
	if err != nil {
		t.Fatalf("expected valid mutation indexes to succeed, got %v", err)
	}
	if len(msg.SecondaryIndexes) != 6 {
		t.Fatalf("expected 6 indexes, got %d", len(msg.SecondaryIndexes))
	}
	wantSuffixes := []string{
		"CountById",
		"CountViewsById",
		"AddLikesById",
		"MinLatencyById",
		"MaxLatencyById",
		"CountGlobalHits",
	}
	for i, want := range wantSuffixes {
		if !msg.SecondaryIndexes[i].IsMutation() || msg.SecondaryIndexes[i].IsAggregate() {
			t.Fatalf("expected index %d to be mutation and non-aggregate", i)
		}
		if got := IndexConstSuffix(msg.SecondaryIndexes[i]); got != want {
			t.Errorf("index %d: got suffix %q, want %q", i, got, want)
		}
	}

	// Constant name collision between AGGREGATE_COUNT and unnamed MUTATION_COUNT on same fields
	_, err = BuildMessageSpecWithTypeID("Post", fields, fieldMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_COUNT},
		{Id: 2, Fields: []string{"tenant_id"}, Mutation: annotationspb.MutationType_MUTATION_COUNT},
	}, false, 20)
	if err == nil || !strings.Contains(err.Error(), "duplicate secondary index constant name") {
		t.Fatalf("expected constant name collision error, got %v", err)
	}

	// Coexistence succeeds when a distinct name is provided on the mutation index
	_, err = BuildMessageSpecWithTypeID("Post", fields, fieldMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Id: 1, Fields: []string{"tenant_id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_COUNT},
		{Id: 2, Fields: []string{"tenant_id"}, Mutation: annotationspb.MutationType_MUTATION_COUNT, Name: "views"},
	}, false, 20)
	if err != nil {
		t.Fatalf("expected named mutation index to coexist with aggregate count index, got %v", err)
	}

	invalidCases := []struct {
		name string
		idx  *annotationspb.SecondaryIndex
		want string
	}{
		{
			name: "both_aggregate_and_mutation",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_COUNT, Mutation: annotationspb.MutationType_MUTATION_ADD},
			want: "cannot specify both aggregate and mutation",
		},
		{
			name: "name_on_standard_index",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"id"}, Name: "custom"},
			want: "option 'name' is only supported on mutation indexes",
		},
		{
			name: "name_on_aggregate_index",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"id"}, Aggregate: annotationspb.IndexAggregateType_AGGREGATE_COUNT, Name: "custom"},
			want: "option 'name' is only supported on mutation indexes",
		},
		{
			name: "invalid_name_characters",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"id"}, Mutation: annotationspb.MutationType_MUTATION_ADD, Name: "123-bad"},
			want: "invalid name",
		},
		{
			name: "aggregate_field_on_mutation",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"id"}, Mutation: annotationspb.MutationType_MUTATION_ADD, AggregateField: "total_cents"},
			want: "aggregate_field cannot be set on a mutation index",
		},
		{
			name: "repeated_field_on_mutation",
			idx:  &annotationspb.SecondaryIndex{Id: 1, Fields: []string{"tags"}, Mutation: annotationspb.MutationType_MUTATION_COUNT},
			want: "cannot be repeated on a mutation index",
		},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildSecondaryIndex(tc.idx, fieldMap, nil, "Post")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestIndexConstSuffix_UngroupedAndPrefixDeduplication(t *testing.T) {
	idField := FieldSpec{ProtoName: "id", GoName: "Id", Kind: KindString}
	centsField := FieldSpec{ProtoName: "total_cents", GoName: "TotalCents", Kind: KindInt64}

	cases := []struct {
		spec SecondaryIndexSpec
		want string
	}{
		// Prefix deduplication when custom name already starts with operation prefix
		{
			spec: SecondaryIndexSpec{Fields: []FieldSpec{idField}, Mutation: MutationCount, Name: "count_views"},
			want: "CountViewsById",
		},
		{
			spec: SecondaryIndexSpec{Fields: []FieldSpec{idField}, Mutation: MutationAdd, Name: "add_score"},
			want: "AddScoreById",
		},
		// Ungrouped mutation indexes
		{
			spec: SecondaryIndexSpec{Fields: nil, Mutation: MutationAdd, Name: "likes"},
			want: "AddLikes",
		},
		{
			spec: SecondaryIndexSpec{Fields: nil, Mutation: MutationMin, Name: "latency"},
			want: "MinLatency",
		},
		{
			spec: SecondaryIndexSpec{Fields: nil, Mutation: MutationMax, Name: "latency"},
			want: "MaxLatency",
		},
		// Ungrouped SUM / MIN / MAX aggregate indexes
		{
			spec: SecondaryIndexSpec{Fields: nil, Aggregate: AggregateSum, AggregateField: centsField},
			want: "SumTotalCents",
		},
		{
			spec: SecondaryIndexSpec{Fields: nil, Aggregate: AggregateMin, AggregateField: centsField},
			want: "MinTotalCents",
		},
		{
			spec: SecondaryIndexSpec{Fields: nil, Aggregate: AggregateMax, AggregateField: centsField},
			want: "MaxTotalCents",
		},
	}

	for _, tc := range cases {
		if got := IndexConstSuffix(tc.spec); got != tc.want {
			t.Errorf("IndexConstSuffix(%+v) = %q, want %q", tc.spec, got, tc.want)
		}
	}
}

func TestParsePlugin_StoreProtoDescriptor(t *testing.T) {
	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{store.File_store_proto.Path()},
		ProtoFile: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
			protodesc.ToFileDescriptorProto(annotationspb.File_fdb_layer_annotations_proto),
			protodesc.ToFileDescriptorProto(store.File_store_proto),
		},
	}
	plugin, err := protogen.Options{}.New(req)
	if err != nil {
		t.Fatalf("protogen.Options.New failed: %v", err)
	}
	msgs, err := ParsePlugin(plugin)
	if err != nil {
		t.Fatalf("ParsePlugin failed: %v", err)
	}
	if len(msgs) != 6 {
		t.Fatalf("expected 6 annotated messages from ParsePlugin, got %d", len(msgs))
	}
	fileSpec, err := ParseFile(plugin.Files[len(plugin.Files)-1])
	if err != nil {
		t.Fatalf("ParseFile failed: %v", err)
	}
	annotated := fileSpec.AnnotatedMessages()
	if len(annotated) != 6 {
		t.Fatalf("expected 6 annotated messages in store.proto FileSpec, got %d", len(annotated))
	}
}

