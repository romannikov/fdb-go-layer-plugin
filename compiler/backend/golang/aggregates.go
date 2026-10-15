package golang

import (
	"fmt"
	"strings"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

func encodeAggregateMinMaxExpr(expr string, f ir.FieldSpec) string {
	if !f.IsUnsigned {
		if FieldGoType(f) == "int64" {
			return fmt.Sprintf("uint64(%s)^(1<<63)", expr)
		}
		return fmt.Sprintf("uint64(int64(%s))^(1<<63)", expr)
	}
	return fmt.Sprintf("uint64(%s)", expr)
}

func decodeAggregateMinMaxExpr(sliceExpr string, f ir.FieldSpec) string {
	if !f.IsUnsigned {
		if FieldGoType(f) == "int64" {
			return fmt.Sprintf("int64(binary.LittleEndian.Uint64(%s) ^ (1 << 63))", sliceExpr)
		}
		return fmt.Sprintf("%s(int64(binary.LittleEndian.Uint64(%s) ^ (1 << 63)))", FieldGoType(f), sliceExpr)
	}
	return fmt.Sprintf("%s(binary.LittleEndian.Uint64(%s))", FieldGoType(f), sliceExpr)
}

func emitAggregateIndexCreate(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, indent string) {
	typeIDConst := typeIDConstName(msg)
	aggKeyExpr := fmt.Sprintf("dir.Pack(tuple.Tuple{%s, fdblayer.AggregateNamespace, %d, %s})", typeIDConst, idx.IndexID, indexFieldTupleArgs(idx.Fields, "entity"))

	g.P(indent, "// Aggregate index (", idx.IndexID, ")")
	if idx.IsSparse {
		g.P(indent, "if ", SparseIndexPresentExpr(idx.Fields, "entity"), " {")
		indent += "\t"
	} else {
		g.P(indent, "{")
		indent += "\t"
	}
	g.P(indent, "aggKey := ", aggKeyExpr)
	g.P(indent, "aggBuf := make([]byte, 8)")
	switch idx.Aggregate {
	case ir.AggregateCount:
		g.P(indent, "binary.LittleEndian.PutUint64(aggBuf, uint64(1))")
		g.P(indent, "tr.Add(aggKey, aggBuf)")
	case ir.AggregateSum:
		g.P(indent, "binary.LittleEndian.PutUint64(aggBuf, uint64(int64(entity.", idx.AggregateField.GoName, ")))")
		g.P(indent, "tr.Add(aggKey, aggBuf)")
	case ir.AggregateMin:
		g.P(indent, "binary.LittleEndian.PutUint64(aggBuf, ", encodeAggregateMinMaxExpr("entity."+idx.AggregateField.GoName, *idx.AggregateField), ")")
		g.P(indent, "tr.Min(aggKey, aggBuf)")
	case ir.AggregateMax:
		g.P(indent, "binary.LittleEndian.PutUint64(aggBuf, ", encodeAggregateMinMaxExpr("entity."+idx.AggregateField.GoName, *idx.AggregateField), ")")
		g.P(indent, "tr.Max(aggKey, aggBuf)")
	}
	indent = indent[:len(indent)-1]
	g.P(indent, "}")
}

func emitAggregateIndexSet(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, indent string) {
	typeIDConst := typeIDConstName(msg)
	oldAggKeyExpr := fmt.Sprintf("dir.Pack(tuple.Tuple{%s, fdblayer.AggregateNamespace, %d, %s})", typeIDConst, idx.IndexID, indexFieldTupleArgs(idx.Fields, "old"))
	newAggKeyExpr := fmt.Sprintf("dir.Pack(tuple.Tuple{%s, fdblayer.AggregateNamespace, %d, %s})", typeIDConst, idx.IndexID, indexFieldTupleArgs(idx.Fields, "entity"))

	var diffParts []string
	for _, f := range idx.Fields {
		diffParts = append(diffParts, FieldNotEqual("old."+f.GoName, "entity."+f.GoName, f))
	}
	groupDiffExpr := strings.Join(diffParts, " || ")

	g.P(indent, "// Aggregate index (", idx.IndexID, ")")
	g.P(indent, "{")
	in := indent + "\t"
	if idx.IsSparse {
		g.P(in, "oldPresent := old != nil && (", SparseIndexPresentExpr(idx.Fields, "old"), ")")
		g.P(in, "newPresent := ", SparseIndexPresentExpr(idx.Fields, "entity"))
	} else {
		g.P(in, "oldPresent := old != nil")
		g.P(in, "newPresent := true")
	}

	switch idx.Aggregate {
	case ir.AggregateCount:
		g.P(in, "if oldPresent && (!newPresent || ", groupDiffExpr, ") {")
		g.P(in, "	aggBuf := make([]byte, 8)")
		g.P(in, "	binary.LittleEndian.PutUint64(aggBuf, ^uint64(0))")
		g.P(in, "	tr.Add(", oldAggKeyExpr, ", aggBuf)")
		g.P(in, "}")
		g.P(in, "if newPresent && (!oldPresent || ", groupDiffExpr, ") {")
		g.P(in, "	aggBuf := make([]byte, 8)")
		g.P(in, "	binary.LittleEndian.PutUint64(aggBuf, uint64(1))")
		g.P(in, "	tr.Add(", newAggKeyExpr, ", aggBuf)")
		g.P(in, "}")
	case ir.AggregateSum:
		aggField := idx.AggregateField.GoName
		g.P(in, "if oldPresent && newPresent && !(", groupDiffExpr, ") {")
		g.P(in, "	delta := int64(entity.", aggField, ") - int64(old.", aggField, ")")
		g.P(in, "	if delta != 0 {")
		g.P(in, "		aggBuf := make([]byte, 8)")
		g.P(in, "		binary.LittleEndian.PutUint64(aggBuf, uint64(delta))")
		g.P(in, "		tr.Add(", newAggKeyExpr, ", aggBuf)")
		g.P(in, "	}")
		g.P(in, "} else {")
		g.P(in, "	if oldPresent {")
		g.P(in, "		aggBuf := make([]byte, 8)")
		g.P(in, "		binary.LittleEndian.PutUint64(aggBuf, uint64(-int64(old.", aggField, ")))")
		g.P(in, "		tr.Add(", oldAggKeyExpr, ", aggBuf)")
		g.P(in, "	}")
		g.P(in, "	if newPresent {")
		g.P(in, "		aggBuf := make([]byte, 8)")
		g.P(in, "		binary.LittleEndian.PutUint64(aggBuf, uint64(int64(entity.", aggField, ")))")
		g.P(in, "		tr.Add(", newAggKeyExpr, ", aggBuf)")
		g.P(in, "	}")
		g.P(in, "}")
	case ir.AggregateMin:
		g.P(in, "_ = oldPresent")
		g.P(in, "if newPresent {")
		g.P(in, "	aggBuf := make([]byte, 8)")
		g.P(in, "	binary.LittleEndian.PutUint64(aggBuf, ", encodeAggregateMinMaxExpr("entity."+idx.AggregateField.GoName, *idx.AggregateField), ")")
		g.P(in, "	tr.Min(", newAggKeyExpr, ", aggBuf)")
		g.P(in, "}")
	case ir.AggregateMax:
		g.P(in, "_ = oldPresent")
		g.P(in, "if newPresent {")
		g.P(in, "	aggBuf := make([]byte, 8)")
		g.P(in, "	binary.LittleEndian.PutUint64(aggBuf, ", encodeAggregateMinMaxExpr("entity."+idx.AggregateField.GoName, *idx.AggregateField), ")")
		g.P(in, "	tr.Max(", newAggKeyExpr, ", aggBuf)")
		g.P(in, "}")
	}
	g.P(indent, "}")
}

func emitAggregateIndexDelete(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, entityVar string, indent string) {
	if idx.Aggregate != ir.AggregateCount && idx.Aggregate != ir.AggregateSum {
		return
	}
	typeIDConst := typeIDConstName(msg)
	aggKeyExpr := fmt.Sprintf("dir.Pack(tuple.Tuple{%s, fdblayer.AggregateNamespace, %d, %s})", typeIDConst, idx.IndexID, indexFieldTupleArgs(idx.Fields, entityVar))

	g.P(indent, "// Aggregate index (", idx.IndexID, ")")
	if idx.IsSparse {
		g.P(indent, "if ", SparseIndexPresentExpr(idx.Fields, entityVar), " {")
		indent += "\t"
	} else {
		g.P(indent, "{")
		indent += "\t"
	}
	g.P(indent, "aggBuf := make([]byte, 8)")
	if idx.Aggregate == ir.AggregateCount {
		g.P(indent, "binary.LittleEndian.PutUint64(aggBuf, ^uint64(0))")
	} else {
		g.P(indent, "binary.LittleEndian.PutUint64(aggBuf, uint64(-int64(", entityVar, ".", idx.AggregateField.GoName, ")))")
	}
	g.P(indent, "tr.Add(", aggKeyExpr, ", aggBuf)")
	indent = indent[:len(indent)-1]
	g.P(indent, "}")
}

func generateAggregateGetter(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec) {
	lowerName := strings.ToLower(msg.Name)
	joined := JoinFieldNames(idx.Fields)
	typeIDConst := typeIDConstName(msg)

	var idxPackArgs []string
	for _, f := range idx.Fields {
		idxPackArgs = append(idxPackArgs, PackField(f.GoName, f))
	}
	aggKeyTuple := fmt.Sprintf("tuple.Tuple{%s, fdblayer.AggregateNamespace, %d, %s}", typeIDConst, idx.IndexID, strings.Join(idxPackArgs, ", "))

	switch idx.Aggregate {
	case ir.AggregateCount:
		g.P("// GetCountBy", joined, " retrieves the atomic count of ", msg.Name, " records grouped by ", joined, ".")
		g.P("func (r *", lowerName, "Repository) GetCountBy", joined, "(ctx context.Context, tx fdblayer.ReadTx, ", fieldParamDecls(idx.Fields), ") (int64, error) {")
		g.P("	if err := ctx.Err(); err != nil {")
		g.P("		return 0, err")
		g.P("	}")
		g.P("	tr := tx.ReadTransaction()")
		g.P("	dir := tx.Dir()")
		g.P("	val := tr.Get(dir.Pack(", aggKeyTuple, ")).MustGet()")
		g.P("	if len(val) < 8 {")
		g.P("		return 0, nil")
		g.P("	}")
		g.P("	return int64(binary.LittleEndian.Uint64(val)), nil")
		g.P("}")
		g.P()
	case ir.AggregateSum:
		aggField := idx.AggregateField
		g.P("// GetSum", aggField.GoName, "By", joined, " retrieves the atomic sum of ", aggField.GoName, " grouped by ", joined, ".")
		g.P("func (r *", lowerName, "Repository) GetSum", aggField.GoName, "By", joined, "(ctx context.Context, tx fdblayer.ReadTx, ", fieldParamDecls(idx.Fields), ") (int64, error) {")
		g.P("	if err := ctx.Err(); err != nil {")
		g.P("		return 0, err")
		g.P("	}")
		g.P("	tr := tx.ReadTransaction()")
		g.P("	dir := tx.Dir()")
		g.P("	val := tr.Get(dir.Pack(", aggKeyTuple, ")).MustGet()")
		g.P("	if len(val) < 8 {")
		g.P("		return 0, nil")
		g.P("	}")
		g.P("	return int64(binary.LittleEndian.Uint64(val)), nil")
		g.P("}")
		g.P()
	case ir.AggregateMin:
		aggField := idx.AggregateField
		g.P("// GetMin", aggField.GoName, "By", joined, " retrieves the atomic minimum of ", aggField.GoName, " grouped by ", joined, ".")
		g.P("func (r *", lowerName, "Repository) GetMin", aggField.GoName, "By", joined, "(ctx context.Context, tx fdblayer.ReadTx, ", fieldParamDecls(idx.Fields), ") (", FieldGoType(*aggField), ", bool, error) {")
		g.P("	if err := ctx.Err(); err != nil {")
		g.P("		return 0, false, err")
		g.P("	}")
		g.P("	tr := tx.ReadTransaction()")
		g.P("	dir := tx.Dir()")
		g.P("	val := tr.Get(dir.Pack(", aggKeyTuple, ")).MustGet()")
		g.P("	if len(val) < 8 {")
		g.P("		return 0, false, nil")
		g.P("	}")
		g.P("	return ", decodeAggregateMinMaxExpr("val", *aggField), ", true, nil")
		g.P("}")
		g.P()
	case ir.AggregateMax:
		aggField := idx.AggregateField
		g.P("// GetMax", aggField.GoName, "By", joined, " retrieves the atomic maximum of ", aggField.GoName, " grouped by ", joined, ".")
		g.P("func (r *", lowerName, "Repository) GetMax", aggField.GoName, "By", joined, "(ctx context.Context, tx fdblayer.ReadTx, ", fieldParamDecls(idx.Fields), ") (", FieldGoType(*aggField), ", bool, error) {")
		g.P("	if err := ctx.Err(); err != nil {")
		g.P("		return 0, false, err")
		g.P("	}")
		g.P("	tr := tx.ReadTransaction()")
		g.P("	dir := tx.Dir()")
		g.P("	val := tr.Get(dir.Pack(", aggKeyTuple, ")).MustGet()")
		g.P("	if len(val) < 8 {")
		g.P("		return 0, false, nil")
		g.P("	}")
		g.P("	return ", decodeAggregateMinMaxExpr("val", *aggField), ", true, nil")
		g.P("}")
		g.P()
	}
}
