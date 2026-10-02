package golang

import (
	"fmt"
	"strings"
	"testing"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

type bufferPrinter struct {
	sb strings.Builder
}

func (b *bufferPrinter) P(v ...any) {
	for _, x := range v {
		fmt.Fprint(&b.sb, x)
	}
	fmt.Fprintln(&b.sb)
}

func TestGoType_AllKinds(t *testing.T) {
	tests := []struct {
		kind     ir.FieldKind
		expected string
	}{
		{ir.KindInt32, "int32"},
		{ir.KindSint32, "int32"},
		{ir.KindUint32, "uint32"},
		{ir.KindFixed32, "uint32"},
		{ir.KindSfixed32, "int32"},
		{ir.KindInt64, "int64"},
		{ir.KindSint64, "int64"},
		{ir.KindUint64, "uint64"},
		{ir.KindFixed64, "uint64"},
		{ir.KindSfixed64, "int64"},
		{ir.KindFloat, "float32"},
		{ir.KindDouble, "float64"},
		{ir.KindString, "string"},
		{ir.KindBool, "bool"},
		{ir.KindBytes, "[]byte"},
		{ir.KindMessage, "interface{}"},
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
		field    ir.FieldSpec
		expected string
	}{
		{"int32", "entity.Age", ir.FieldSpec{Kind: ir.KindInt32}, "int64(entity.Age)"},
		{"int64", "entity.Id", ir.FieldSpec{Kind: ir.KindInt64}, "int64(entity.Id)"},
		{"uint32", "entity.ShardId", ir.FieldSpec{Kind: ir.KindUint32, IsUnsigned: true}, "uint64(entity.ShardId)"},
		{"uint64", "entity.Seq", ir.FieldSpec{Kind: ir.KindUint64, IsUnsigned: true}, "uint64(entity.Seq)"},
		{"enum", "entity.Status", ir.FieldSpec{Kind: ir.KindEnum, EnumGoName: "OrderStatus", IsEnum: true}, "int64(entity.Status)"},
		{"string", "entity.Name", ir.FieldSpec{Kind: ir.KindString}, "entity.Name"},
		{"bytes", "entity.RawKey", ir.FieldSpec{Kind: ir.KindBytes}, "entity.RawKey"},
		{"versionstamp", "entity.Vs", ir.FieldSpec{Kind: ir.KindBytes, IsVersionstamp: true}, "fdblayer.BytesToVersionstamp(entity.Vs)"},
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
		fields   []ir.FieldSpec
		expected string
	}{
		{"empty", nil, ""},
		{"single", []ir.FieldSpec{{GoName: "Email"}}, "Email"},
		{"two", []ir.FieldSpec{{GoName: "First"}, {GoName: "Last"}}, "FirstAndLast"},
		{"three", []ir.FieldSpec{{GoName: "A"}, {GoName: "B"}, {GoName: "C"}}, "AAndBAndC"},
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

func TestFieldEqualityAndMapKeyHelpers(t *testing.T) {
	strField := ir.FieldSpec{GoName: "Email", Kind: ir.KindString}
	bytesField := ir.FieldSpec{GoName: "RawHash", Kind: ir.KindBytes}

	if got := FieldNotEqual("old.Email", "entity.Email", strField); got != "old.Email != entity.Email" {
		t.Errorf("FieldNotEqual(string) = %q", got)
	}
	if got := FieldNotEqual("old.RawHash", "entity.RawHash", bytesField); got != "!bytes.Equal(old.RawHash, entity.RawHash)" {
		t.Errorf("FieldNotEqual([]byte) = %q", got)
	}
	if got := FieldEqual("old.RawHash", "entity.RawHash", bytesField); got != "bytes.Equal(old.RawHash, entity.RawHash)" {
		t.Errorf("FieldEqual([]byte) = %q", got)
	}
	if got := MapKeyType(bytesField); got != "string" {
		t.Errorf("MapKeyType([]byte) = %q, want \"string\"", got)
	}
	if got := MapKeyExpr("item", bytesField); got != "string(item)" {
		t.Errorf("MapKeyExpr([]byte) = %q, want \"string(item)\"", got)
	}
}

func TestGenerateMessage_EntityAndQueue(t *testing.T) {
	tenantID := ir.FieldSpec{ProtoName: "tenant_id", GoName: "TenantId", Kind: ir.KindString}
	orderSeq := ir.FieldSpec{ProtoName: "order_seq", GoName: "OrderSeq", Kind: ir.KindUint64, IsUnsigned: true}
	status := ir.FieldSpec{ProtoName: "status", GoName: "Status", Kind: ir.KindEnum, EnumGoName: "OrderStatus", IsEnum: true}
	hashes := ir.FieldSpec{ProtoName: "hashes", GoName: "Hashes", Kind: ir.KindBytes, IsRepeated: true}
	counter := ir.FieldSpec{ProtoName: "counter", GoName: "Counter", Kind: ir.KindInt64, Mutation: ir.MutationAdd}

	orderSpec := ir.MessageSpec{
		Name:             "Order",
		GoPackageName:    "testspb",
		Fields:           []ir.FieldSpec{tenantID, orderSeq, status, hashes, counter},
		PrimaryKeyFields: []ir.FieldSpec{tenantID, orderSeq},
		SecondaryIndexes: []ir.SecondaryIndexSpec{
			{
				Fields:  []ir.FieldSpec{status, orderSeq},
				IndexID: 1001,
			},
			{
				Fields:      []ir.FieldSpec{hashes},
				IsFanOut:    true,
				FanOutField: hashes,
				IndexID:     2002,
			},
		},
	}

	var p bufferPrinter
	GenerateMessage(&p, orderSpec)
	out := p.sb.String()

	for _, expectedSnippet := range []string{
		"type OrderPrimaryKey struct {",
		"func (r *orderRepository) Get(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, pk OrderPrimaryKey)",
		"func (r *orderRepository) GetOrderByStatusAndOrderSeq(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, Status OrderStatus, OrderSeq uint64)",
		"func (r *orderRepository) GetOrderByHashes(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, Hashes []byte)",
		"func (r *orderRepository) AddOrderCounter(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, TenantId string, OrderSeq uint64, val int64)",
		"oldSet := make(map[string]struct{}, len(old.Hashes))",
	} {
		if !strings.Contains(out, expectedSnippet) {
			t.Errorf("expected generated Go code to contain %q, got:\n%s", expectedSnippet, out)
		}
	}

	// Also verify queue message generation
	qName := ir.FieldSpec{ProtoName: "queue_name", GoName: "QueueName", Kind: ir.KindString}
	vs := ir.FieldSpec{ProtoName: "versionstamp", GoName: "Versionstamp", Kind: ir.KindBytes, IsVersionstamp: true}
	shardID := ir.FieldSpec{ProtoName: "shard_id", GoName: "ShardId", Kind: ir.KindUint32, IsUnsigned: true}
	queueSpec := ir.MessageSpec{
		Name:             "TaskMessage",
		GoPackageName:    "testspb",
		Fields:           []ir.FieldSpec{qName, vs, shardID},
		PrimaryKeyFields: []ir.FieldSpec{qName, vs, shardID},
		IsQueue:          true,
	}

	var qp bufferPrinter
	GenerateMessage(&qp, queueSpec)
	qOut := qp.sb.String()

	for _, expectedSnippet := range []string{
		"func (r *taskmessageRepository) Enqueue(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, entity *TaskMessage)",
		"func (r *taskmessageRepository) Dequeue(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, queuename string)",
		"tr.SetVersionstampedKey(key, value)",
		"tuple.IncompleteVersionstamp(r.store.NextUserVersion())",
	} {
		if !strings.Contains(qOut, expectedSnippet) {
			t.Errorf("expected generated Go queue code to contain %q, got:\n%s", expectedSnippet, qOut)
		}
	}
}


