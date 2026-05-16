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
		{protoreflect.Uint32Kind, "int32"},
		{protoreflect.Fixed32Kind, "int32"},
		{protoreflect.Sfixed32Kind, "int32"},
		{protoreflect.Int64Kind, "int64"},
		{protoreflect.Sint64Kind, "int64"},
		{protoreflect.Uint64Kind, "int64"},
		{protoreflect.Fixed64Kind, "int64"},
		{protoreflect.Sfixed64Kind, "int64"},
		{protoreflect.FloatKind, "float32"},
		{protoreflect.DoubleKind, "float64"},
		{protoreflect.StringKind, "string"},
		{protoreflect.BoolKind, "bool"},
		{protoreflect.BytesKind, "interface{}"},
		{protoreflect.MessageKind, "interface{}"},
	}

	for _, tt := range tests {
		got := GoType(tt.kind)
		if got != tt.expected {
			t.Errorf("GoType(%v) = %q, want %q", tt.kind, got, tt.expected)
		}
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
