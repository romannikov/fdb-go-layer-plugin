package golang

import (
	"fmt"
	"strings"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

func generateAtomicMutators(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	pkType := pkTypeExpr(msg)
	pkArgsStr := pkTupleArgsFromParam(msg)
	typeIDConst := typeIDConstName(msg)

	for _, f := range msg.Fields {
		if f.Mutation == ir.MutationNone {
			continue
		}
		verb := MutationVerb(f.Mutation)
		g.P("// ", verb, f.GoName, " applies an atomic mutation to the ", f.GoName, " field of ", msg.Name, ".")
		g.P("func (r *", lowerName, "Repository) ", verb, f.GoName, "(ctx context.Context, tx *fdblayer.Tx, pk ", pkType, ", val ", FieldGoType(f), ") error {")
		g.P("	if err := ctx.Err(); err != nil {")
		g.P("		return err")
		g.P("	}")
		g.P("	tr := tx.Transaction()")
		g.P("	dir := tx.Dir()")
		g.P("	key := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgsStr, ", ", f.Number, "})")
		g.P()
		g.P("	buf := make([]byte, 8)")
		g.P("	binary.LittleEndian.PutUint64(buf, ", encodeAtomicUint64Expr("val", f), ")")
		g.P()
		g.P("	tr.", verb, "(key, buf)")
		g.P("	return nil")
		g.P("}")
		g.P()

		g.P("// ", verb, f.GoName, "Mutation returns a buffered Mutation that atomically updates the ", f.GoName, " field of ", msg.Name, ".")
		g.P("func (r *", lowerName, "Repository) ", verb, f.GoName, "Mutation(pk ", pkType, ", val ", FieldGoType(f), ") fdblayer.Mutation {")
		g.P("	return fdblayer.MutationFunc(func(ctx context.Context, tx *fdblayer.Tx) (func(context.Context) error, error) {")
		g.P("		return func(ctx context.Context) error {")
		g.P("			return r.", verb, f.GoName, "(ctx, tx, pk, val)")
		g.P("		}, nil")
		g.P("	})")
		g.P("}")
		g.P()

		g.P("// Set", f.GoName, " explicitly sets or resets the atomic ", f.GoName, " field of ", msg.Name, " in FieldNamespace.")
		g.P("func (r *", lowerName, "Repository) Set", f.GoName, "(ctx context.Context, tx *fdblayer.Tx, pk ", pkType, ", val ", FieldGoType(f), ") error {")
		g.P("	if err := ctx.Err(); err != nil {")
		g.P("		return err")
		g.P("	}")
		g.P("	tr := tx.Transaction()")
		g.P("	dir := tx.Dir()")
		g.P("	key := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgsStr, ", ", f.Number, "})")
		if f.Mutation == ir.MutationMax && !f.IsUnsigned {
			g.P("	buf := make([]byte, 8)")
			g.P("	binary.LittleEndian.PutUint64(buf, ", encodeAtomicUint64Expr("val", f), ")")
			g.P("	tr.Set(key, buf)")
		} else {
			g.P("	if val == 0 {")
			g.P("		tr.Clear(key)")
			g.P("	} else {")
			g.P("		buf := make([]byte, 8)")
			g.P("		binary.LittleEndian.PutUint64(buf, ", encodeAtomicUint64Expr("val", f), ")")
			g.P("		tr.Set(key, buf)")
			g.P("	}")
		}
		g.P("	return nil")
		g.P("}")
		g.P()

		g.P("// Set", f.GoName, "Mutation returns a buffered Mutation that explicitly sets or resets the ", f.GoName, " field of ", msg.Name, ".")
		g.P("func (r *", lowerName, "Repository) Set", f.GoName, "Mutation(pk ", pkType, ", val ", FieldGoType(f), ") fdblayer.Mutation {")
		g.P("	return fdblayer.MutationFunc(func(ctx context.Context, tx *fdblayer.Tx) (func(context.Context) error, error) {")
		g.P("		return func(ctx context.Context) error {")
		g.P("			return r.Set", f.GoName, "(ctx, tx, pk, val)")
		g.P("		}, nil")
		g.P("	})")
		g.P("}")
		g.P()
	}
}

func encodeAtomicUint64Expr(expr string, f ir.FieldSpec) string {
	if !f.IsUnsigned && (f.Mutation == ir.MutationMax || f.Mutation == ir.MutationMin) {
		if FieldGoType(f) == "int64" {
			return fmt.Sprintf("uint64(%s)^(1<<63)", expr)
		}
		return fmt.Sprintf("uint64(int64(%s))^(1<<63)", expr)
	}
	return fmt.Sprintf("uint64(%s)", expr)
}

func decodeAtomicGoExpr(sliceExpr string, f ir.FieldSpec) string {
	if !f.IsUnsigned && (f.Mutation == ir.MutationMax || f.Mutation == ir.MutationMin) {
		if FieldGoType(f) == "int64" {
			return fmt.Sprintf("int64(binary.LittleEndian.Uint64(%s) ^ (1 << 63))", sliceExpr)
		}
		return fmt.Sprintf("%s(int64(binary.LittleEndian.Uint64(%s) ^ (1 << 63)))", FieldGoType(f), sliceExpr)
	}
	return fmt.Sprintf("%s(binary.LittleEndian.Uint64(%s))", FieldGoType(f), sliceExpr)
}

func generateMarshalWithZeroedAtomics(g Printer, msg ir.MessageSpec) {
	hasMut := ir.HasMutationFields(msg)
	hasVS := ir.MsgHasVersionstampPK(msg)
	if hasMut || hasVS {
		var nonZeroChecks []string
		for _, f := range msg.Fields {
			if f.Mutation != ir.MutationNone {
				nonZeroChecks = append(nonZeroChecks, fmt.Sprintf("entity.%s != 0", f.GoName))
			} else if f.IsVersionstamp {
				nonZeroChecks = append(nonZeroChecks, fmt.Sprintf("len(entity.%s) > 0", f.GoName))
			}
		}
		g.P("	// Marshal without mutating the caller's struct in-place.")
		g.P("	marshalTarget := entity")
		g.P("	if ", strings.Join(nonZeroChecks, " || "), " {")
		g.P("		marshalTarget = &", msg.Name, "{")
		for _, f := range msg.Fields {
			if f.Mutation == ir.MutationNone && !f.IsVersionstamp {
				g.P("			", f.GoName, ": entity.", f.GoName, ",")
			}
		}
		g.P("		}")
		g.P("	}")
		g.P("	value, err := proto.Marshal(marshalTarget)")
	} else {
		g.P("	value, err := proto.Marshal(entity)")
	}
	g.P("	if err != nil {")
	g.P("		return err")
	g.P("	}")
	g.P()
}

func generateUnmarshalAtomicFields(g Printer, msg ir.MessageSpec, sliceExpr string, indent string) {
	for _, f := range msg.Fields {
		if f.Mutation != ir.MutationNone {
			g.P(indent, "entity.", f.GoName, " = 0")
		}
	}
	g.P(indent, "for _, fkv := range ", sliceExpr, " {")
	g.P(indent, "	if len(fkv.Value) < 8 {")
	g.P(indent, "		continue")
	g.P(indent, "	}")
	g.P(indent, "	ftpl, err := dir.Unpack(fkv.Key)")
	g.P(indent, "	if err != nil || len(ftpl) == 0 {")
	g.P(indent, "		continue")
	g.P(indent, "	}")
	g.P(indent, "	fieldNum, ok := ftpl[len(ftpl)-1].(int64)")
	g.P(indent, "	if !ok {")
	g.P(indent, "		continue")
	g.P(indent, "	}")
	g.P(indent, "	switch fieldNum {")
	for _, f := range msg.Fields {
		if f.Mutation != ir.MutationNone {
			g.P(indent, "	case ", f.Number, ":")
			g.P(indent, "		entity.", f.GoName, " = ", decodeAtomicGoExpr("fkv.Value", f))
		}
	}
	g.P(indent, "	}")
	g.P(indent, "}")
}
