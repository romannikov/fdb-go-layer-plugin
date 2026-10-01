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
