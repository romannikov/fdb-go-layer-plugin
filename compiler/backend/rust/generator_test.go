package rust

import (
	"strings"
	"testing"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

func TestGenerateRustRepository_StandardAndAtomics(t *testing.T) {
	msg := ir.MessageSpec{
		Name: "Counter",
		PrimaryKeyFields: []ir.FieldSpec{
			{ProtoName: "id", GoName: "Id", Number: 1, Kind: ir.KindString},
		},
		Fields: []ir.FieldSpec{
			{ProtoName: "id", GoName: "Id", Number: 1, Kind: ir.KindString},
			{ProtoName: "value", GoName: "Value", Number: 2, Kind: ir.KindInt64, Mutation: ir.MutationAdd},
			{ProtoName: "max_value", GoName: "MaxValue", Number: 3, Kind: ir.KindInt64, Mutation: ir.MutationMax},
			{ProtoName: "min_value", GoName: "MinValue", Number: 4, Kind: ir.KindInt64, Mutation: ir.MutationMin},
		},
	}

	var buf BufferPrinter
	GenerateFile(&buf, "atomic.proto", []ir.MessageSpec{msg})
	code := buf.String()

	expectedSubstrings := []string{
		"pub struct CounterRepository",
		"pub async fn create(",
		"pub async fn get(",
		"pub async fn set(",
		"pub async fn delete(",
		"pub async fn batch_get_counter(",
		"pub async fn list_counter(",
		"pub async fn add_counter_value(",
		"pub async fn max_counter_max_value(",
		"pub async fn min_counter_min_value(",
		"fdb_layer::MutationType::Add",
		"fdb_layer::MutationType::Max",
		"fdb_layer::MutationType::Min",
	}

	for _, exp := range expectedSubstrings {
		if !strings.Contains(code, exp) {
			t.Errorf("expected generated Rust code to contain %q, got:\n%s", exp, code)
		}
	}
}

func TestGenerateRustRepository_Queue(t *testing.T) {
	msg := ir.MessageSpec{
		Name:    "TaskMessage",
		IsQueue: true,
		PrimaryKeyFields: []ir.FieldSpec{
			{ProtoName: "queue_name", GoName: "QueueName", Number: 1, Kind: ir.KindString},
			{ProtoName: "versionstamp", GoName: "Versionstamp", Number: 3, Kind: ir.KindBytes, IsVersionstamp: true},
			{ProtoName: "shard_id", GoName: "ShardId", Number: 2, Kind: ir.KindUint32, IsUnsigned: true},
		},
		Fields: []ir.FieldSpec{
			{ProtoName: "queue_name", GoName: "QueueName", Number: 1, Kind: ir.KindString},
			{ProtoName: "shard_id", GoName: "ShardId", Number: 2, Kind: ir.KindUint32, IsUnsigned: true},
			{ProtoName: "versionstamp", GoName: "Versionstamp", Number: 3, Kind: ir.KindBytes, IsVersionstamp: true},
			{ProtoName: "payload", GoName: "Payload", Number: 4, Kind: ir.KindBytes},
		},
	}

	var buf BufferPrinter
	GenerateMessage(&buf, msg)
	code := buf.String()

	expectedSubstrings := []string{
		"pub struct TaskMessageRepository",
		"pub async fn enqueue(",
		"pub async fn dequeue(",
		"fdb_layer::Versionstamp::incomplete(self.store.next_user_version())",
		"(entity.shard_id as u64)",
		"fdb_layer::MutationType::SetVersionstampedKey",
	}

	for _, exp := range expectedSubstrings {
		if !strings.Contains(code, exp) {
			t.Errorf("expected generated Rust queue code to contain %q, got:\n%s", exp, code)
		}
	}
}
