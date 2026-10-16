package golang

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/romannikov/fdb-layer/compiler/backend"
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

	orderSpec := ir.MessageSpec{
		Name:             "Order",
		TypeID:           100,
		GoPackageName:    "testspb",
		Fields:           []ir.FieldSpec{tenantID, orderSeq, status, hashes},
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
		"const OrderTypeID int64 = 100",
		"OrderByStatusOrderSeq fdblayer.IndexID = 1001",
		"OrderByHashes fdblayer.IndexID = 2002",
		"func NewOrderRepository(dir directory.DirectorySubspace) OrderRepository {",
		"func (r *orderRepository) Insert(ctx context.Context, tr fdblayer.Transaction, entity *Order, opts ...fdblayer.WriteOption) error",
		"func (r *orderRepository) Upsert(ctx context.Context, tr fdblayer.Transaction, entity *Order, opts ...fdblayer.WriteOption) error",
		"func (r *orderRepository) Delete(ctx context.Context, tr fdblayer.Transaction, key fdblayer.Key) error",
		"func (r *orderRepository) ReadRow(ctx context.Context, tr fdblayer.Transaction, key fdblayer.Key) (*Order, error)",
		"func (r *orderRepository) ReadRows(ctx context.Context, tr fdblayer.Transaction, keys []fdblayer.Key) ([]*Order, error)",
		"func (r *orderRepository) Scan(ctx context.Context, tr fdblayer.Transaction, kr fdblayer.KeyRange, opts ...fdblayer.ScanOption) (*fdblayer.Page[*Order], error)",
		"func (r *orderRepository) ScanIndex(ctx context.Context, tr fdblayer.Transaction, index fdblayer.IndexID, kr fdblayer.KeyRange, opts ...fdblayer.ScanOption) (*fdblayer.Page[*Order], error)",
		"fdblayer.ErrNotFound",
		"oldSet := make(map[string]struct{}, len(old.Hashes))",
	} {
		if !strings.Contains(out, expectedSnippet) {
			t.Errorf("expected generated Go code to contain %q, got:\n%s", expectedSnippet, out)
		}
	}

	// Verify no GetBy* or OrderPrimaryKey types are generated
	for _, forbidden := range []string{"OrderPrimaryKey", "GetBy"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("expected generated Go code NOT to contain %q, got:\n%s", forbidden, out)
		}
	}

	// Also verify queue message generation
	qName := ir.FieldSpec{ProtoName: "queue_name", GoName: "QueueName", Kind: ir.KindString}
	vs := ir.FieldSpec{ProtoName: "versionstamp", GoName: "Versionstamp", Kind: ir.KindBytes, IsVersionstamp: true}
	shardID := ir.FieldSpec{ProtoName: "shard_id", GoName: "ShardId", Kind: ir.KindUint32, IsUnsigned: true}
	queueSpec := ir.MessageSpec{
		Name:             "TaskMessage",
		GoPackageName:    "testspb",
		TypeID:           42,
		Fields:           []ir.FieldSpec{qName, vs, shardID},
		PrimaryKeyFields: []ir.FieldSpec{qName, vs, shardID},
		IsQueue:          true,
	}

	var qp bufferPrinter
	GenerateMessage(&qp, queueSpec)
	qOut := qp.sb.String()

	for _, expectedSnippet := range []string{
		"const TaskMessageTypeID int64 = 42",
		"func NewTaskMessageRepository(dir directory.DirectorySubspace) TaskMessageRepository {",
		"func (r *taskmessageRepository) Enqueue(ctx context.Context, tr fdblayer.Transaction, entity *TaskMessage, opts ...fdblayer.WriteOption)",
		"func (r *taskmessageRepository) Dequeue(ctx context.Context, tr fdblayer.Transaction, partitionKey fdblayer.Key)",
		"func (r *taskmessageRepository) DequeueBatch(ctx context.Context, tr fdblayer.Transaction, partitionKey fdblayer.Key, limit int)",
		"writeCfg := fdblayer.ApplyWriteOptions(opts)",
		"tr.SetVersionstampedKey(key, value)",
		"tuple.IncompleteVersionstamp(writeCfg.UserVersion)",
	} {
		if !strings.Contains(qOut, expectedSnippet) {
			t.Errorf("expected generated Go queue code to contain %q, got:\n%s", expectedSnippet, qOut)
		}
	}
}

func TestGenerateMessage_NonQueueVersionstampWithIndexes(t *testing.T) {
	logID := ir.FieldSpec{ProtoName: "log_id", GoName: "LogId", Number: 1, Kind: ir.KindBytes, IsVersionstamp: true}
	actor := ir.FieldSpec{ProtoName: "actor", GoName: "Actor", Number: 2, Kind: ir.KindString}
	tags := ir.FieldSpec{ProtoName: "tags", GoName: "Tags", Number: 3, Kind: ir.KindString, IsRepeated: true}
	views := ir.FieldSpec{ProtoName: "views", GoName: "Views", Number: 4, Kind: ir.KindInt64}
	action := ir.FieldSpec{ProtoName: "action", GoName: "Action", Number: 5, Kind: ir.KindString}

	spec := ir.MessageSpec{
		Name:             "AuditLog",
		TypeID:           5,
		GoPackageName:    "testspb",
		Fields:           []ir.FieldSpec{logID, actor, tags, views, action},
		PrimaryKeyFields: []ir.FieldSpec{logID},
		SecondaryIndexes: []ir.SecondaryIndexSpec{
			{
				Fields:  []ir.FieldSpec{actor},
				IndexID: 1001,
			},
			{
				Fields:      []ir.FieldSpec{tags},
				IsFanOut:    true,
				FanOutField: tags,
				IndexID:     1002,
			},
			{
				Fields:  []ir.FieldSpec{action},
				IndexID: 1003,
			},
		},
	}

	var p bufferPrinter
	GenerateMessage(&p, spec)
	out := p.sb.String()

	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "auditlog.fdb.go", out, parser.AllErrors)
	if err != nil {
		t.Fatalf("generated Go code failed to parse: %v\n%s", err, out)
	}
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Insert" {
			continue
		}
		declared := map[string]bool{}
		for _, stmt := range fn.Body.List {
			assign, ok := stmt.(*ast.AssignStmt)
			if !ok || assign.Tok != token.DEFINE {
				continue
			}
			newVars := 0
			for _, expr := range assign.Lhs {
				if ident, ok := expr.(*ast.Ident); ok && ident.Name != "_" && !declared[ident.Name] {
					declared[ident.Name] = true
					newVars++
				}
			}
			if newVars == 0 {
				t.Errorf("Insert has duplicate short variable declaration at top-level function scope")
			}
		}
	}

	for _, expectedSnippet := range []string{
		"const AuditLogTypeID int64 = 5",
		"AuditLogByActor fdblayer.IndexID = 1001",
		"AuditLogByTags fdblayer.IndexID = 1002",
		"AuditLogByAction fdblayer.IndexID = 1003",
		"writeCfg := fdblayer.ApplyWriteOptions(opts)",
		"tuple.IncompleteVersionstamp(writeCfg.UserVersion)",
		"idxKey, err := dir.PackWithVersionstamp(tuple.Tuple{AuditLogTypeID, fdblayer.IndexNamespace, int64(AuditLogByActor), entity.Actor, tuple.IncompleteVersionstamp(writeCfg.UserVersion)})",
		"idxKey, err := dir.PackWithVersionstamp(tuple.Tuple{AuditLogTypeID, fdblayer.IndexNamespace, int64(AuditLogByTags),",
		"idxKey, err := dir.PackWithVersionstamp(tuple.Tuple{AuditLogTypeID, fdblayer.IndexNamespace, int64(AuditLogByAction), entity.Action, tuple.IncompleteVersionstamp(writeCfg.UserVersion)})",
		"if len(entity.LogId) != 0 && len(entity.LogId) != 12 {",
		"if len(entity.LogId) == 0 {",
		"return r.Insert(ctx, tr, entity, opts...)",
		"if _, err := fdblayer.PackVersionstampElement(entity.LogId); err != nil {",
		"unmarshaled.LogId = entity.LogId",
		"pkTuples := make([]tuple.Tuple, len(kvs))",
		"if vs, ok := pkTuples[i][0].(tuple.Versionstamp); ok {",
	} {
		if !strings.Contains(out, expectedSnippet) {
			t.Errorf("expected generated Go code for non-queue versionstamp message to contain %q, got:\n%s", expectedSnippet, out)
		}
	}
}

func TestGenerateMessage_EntityWithoutSecondaryIndexes(t *testing.T) {
	idField := ir.FieldSpec{ProtoName: "id", GoName: "Id", Number: 1, Kind: ir.KindString}
	spec := ir.MessageSpec{
		Name:             "SimpleItem",
		TypeID:           10,
		GoPackageName:    "testspb",
		Fields:           []ir.FieldSpec{idField},
		PrimaryKeyFields: []ir.FieldSpec{idField},
	}

	var p bufferPrinter
	GenerateMessage(&p, spec)
	out := p.sb.String()

	fset := token.NewFileSet()
	if _, err := parser.ParseFile(fset, "simpleitem.fdb.go", out, parser.AllErrors); err != nil {
		t.Fatalf("generated Go code without secondary indexes failed to parse: %v\n%s", err, out)
	}
	if !strings.Contains(out, "fdblayer.ErrUnknownIndex") {
		t.Errorf("expected ScanIndex to return ErrUnknownIndex when message has no secondary indexes, got:\n%s", out)
	}
}

func TestBackend_GenerateFile(t *testing.T) {
	gen := NewBackend()
	if gen.Language() != "go" {
		t.Fatalf("expected Language() = \"go\", got %q", gen.Language())
	}

	idField := ir.FieldSpec{ProtoName: "id", GoName: "Id", Number: 1, Kind: ir.KindString}
	fileSpec := &ir.FileSpec{
		ProtoPath:               "store.proto",
		Package:                 "store",
		GoImportPath:            "example.com/store",
		GoPackageName:           "store",
		GeneratedFilenamePrefix: "tests/go/store/store",
		Messages: []ir.MessageSpec{
			{Name: "Address"},
			{
				Name:             "User",
				TypeID:           1,
				Fields:           []ir.FieldSpec{idField},
				PrimaryKeyFields: []ir.FieldSpec{idField},
			},
		},
	}

	files, err := gen.GenerateFile(fileSpec, backend.Options{})
	if err != nil {
		t.Fatalf("unexpected error from GenerateFile: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 generated file (skipping unannotated Address), got %d", len(files))
	}
	if files[0].Filename != "tests/go/store/store_user.fdb.go" {
		t.Fatalf("unexpected filename: %q", files[0].Filename)
	}
	if files[0].GoImportPath != "example.com/store" {
		t.Fatalf("expected GoImportPath \"example.com/store\", got %q", files[0].GoImportPath)
	}
	fset := token.NewFileSet()
	if _, err := parser.ParseFile(fset, files[0].Filename, files[0].Content, parser.AllErrors); err != nil {
		t.Fatalf("generated file failed to parse: %v", err)
	}
}
