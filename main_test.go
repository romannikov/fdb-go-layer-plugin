package main

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestGoType_AllKinds(t *testing.T) {
	tests := []struct {
		kind     protoreflect.Kind
		expected string
	}{
		{protoreflect.Int32Kind, "int32"},
		{protoreflect.Sint32Kind, "int32"},
		{protoreflect.Uint32Kind, "uint32"},
		{protoreflect.Fixed32Kind, "uint32"},
		{protoreflect.Sfixed32Kind, "int32"},
		{protoreflect.Int64Kind, "int64"},
		{protoreflect.Sint64Kind, "int64"},
		{protoreflect.Uint64Kind, "uint64"},
		{protoreflect.Fixed64Kind, "uint64"},
		{protoreflect.Sfixed64Kind, "int64"},
		{protoreflect.FloatKind, "float32"},
		{protoreflect.DoubleKind, "float64"},
		{protoreflect.StringKind, "string"},
		{protoreflect.BoolKind, "bool"},
		{protoreflect.BytesKind, "[]byte"},
		{protoreflect.MessageKind, "interface{}"},
	}

	for _, tt := range tests {
		got := GoType(tt.kind)
		if got != tt.expected {
			t.Errorf("GoType(%v) = %q, want %q", tt.kind, got, tt.expected)
		}
	}
}

func TestPackField_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		field    Field
		expected string
	}{
		{"int32", "entity.Age", Field{Type: "int32"}, "int64(entity.Age)"},
		{"int64", "entity.Id", Field{Type: "int64"}, "int64(entity.Id)"},
		{"uint32", "entity.ShardId", Field{Type: "uint32", IsUnsigned: true}, "uint64(entity.ShardId)"},
		{"uint64", "entity.Seq", Field{Type: "uint64", IsUnsigned: true}, "uint64(entity.Seq)"},
		{"enum", "entity.Status", Field{Type: "OrderStatus", IsEnum: true}, "int64(entity.Status)"},
		{"string", "entity.Name", Field{Type: "string"}, "entity.Name"},
		{"bytes", "entity.RawKey", Field{Type: "[]byte"}, "entity.RawKey"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PackField(tt.expr, tt.field)
			if got != tt.expected {
				t.Errorf("PackField(%q, %+v) = %q, want %q", tt.expr, tt.field, got, tt.expected)
			}
		})
	}
}

func TestJoinFieldNames_Various(t *testing.T) {
	tests := []struct {
		name     string
		fields   []Field
		expected string
	}{
		{"empty", nil, ""},
		{"single", []Field{{Name: "Email"}}, "Email"},
		{"two", []Field{{Name: "First"}, {Name: "Last"}}, "FirstAndLast"},
		{"three", []Field{{Name: "A"}, {Name: "B"}, {Name: "C"}}, "AAndBAndC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := JoinFieldNames(tt.fields)
			if got != tt.expected {
				t.Errorf("JoinFieldNames() = %q, want %q", got, tt.expected)
			}
		})
	}
}
