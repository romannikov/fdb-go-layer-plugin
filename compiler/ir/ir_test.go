package ir

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
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

func TestComputeIndexID_MatchesReferenceHashes(t *testing.T) {
	if got := ComputeIndexID("email"); got != 2324124615 {
		t.Fatalf("ComputeIndexID(\"email\") = %d, want 2324124615", got)
	}
	if got := ComputeIndexID("category"); got != 3475980913 {
		t.Fatalf("ComputeIndexID(\"category\") = %d, want 3475980913", got)
	}
	if got := ComputeIndexID("tags"); got != 4095142816 {
		t.Fatalf("ComputeIndexID(\"tags\") = %d, want 4095142816", got)
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
	}, fieldMap, usedHashes, "User")
	if err == nil || !strings.Contains(err.Error(), "multiple repeated fields") {
		t.Fatalf("expected multiple repeated fields error, got %v", err)
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
	_, err := BuildMessageSpec("InvalidMsg", fields, fieldMap, []string{"id", "vs1", "vs2"}, nil, false)
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
	_, err = BuildMessageSpec("InvalidMsg", singleVsFields, singleVsMap, []string{"missing"}, nil, false)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected missing primary key error, got %v", err)
	}

	// 3. Repeated primary_key field
	_, err = BuildMessageSpec("InvalidMsg", singleVsFields, singleVsMap, []string{"tags", "vs1"}, nil, false)
	if err == nil || !strings.Contains(err.Error(), "cannot be repeated") {
		t.Fatalf("expected repeated primary key error, got %v", err)
	}

	// 4. Atomic mutation field in primary_key
	_, err = BuildMessageSpec("InvalidMsg", singleVsFields, singleVsMap, []string{"count", "vs1"}, nil, false)
	if err == nil || !strings.Contains(err.Error(), "cannot have an atomic mutation") {
		t.Fatalf("expected atomic mutation primary key error, got %v", err)
	}

	// 5. Versionstamp field not in primary_key
	_, err = BuildMessageSpec("InvalidMsg", singleVsFields, singleVsMap, []string{"id"}, nil, false)
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
	_, err = BuildMessageSpec("InvalidQueue", noVsFields, noVsMap, []string{"id"}, nil, true)
	if err == nil || !strings.Contains(err.Error(), "must have an is_versionstamp=true primary key field") {
		t.Fatalf("expected queue missing versionstamp error, got %v", err)
	}

	// 7. Secondary index referencing non-existent field
	_, err = BuildMessageSpec("InvalidMsg", noVsFields, noVsMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Fields: []string{"unknown_idx"}},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected secondary index field not found error, got %v", err)
	}

	// 8. Secondary index referencing atomic mutation field
	_, err = BuildMessageSpec("InvalidMsg", noVsFields, noVsMap, []string{"id"}, []*annotationspb.SecondaryIndex{
		{Fields: []string{"count"}},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "cannot have an atomic mutation") {
		t.Fatalf("expected secondary index on atomic mutation field error, got %v", err)
	}
}


