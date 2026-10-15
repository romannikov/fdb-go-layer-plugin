package golang

import (
	"strconv"
	"strings"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

func generateGetWithChildren(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	pkType := pkTypeExpr(msg)
	pkArgs := pkTupleArgsFromParam(msg)
	hasMut := ir.HasMutationFields(msg)
	typeIDConst := typeIDConstName(msg)

	g.P("// GetWithChildren retrieves a parent ", msg.Name, " entity and all interleaved child entities in a single range scan.")
	g.P("func (r *", lowerName, "Repository) GetWithChildren(ctx context.Context, tx fdblayer.ReadTx, pk ", pkType, ") (*", msg.Name, "WithChildren, error) {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	tr := tx.ReadTransaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	g.P("	parentKey := dir.Pack(", dataKeyTupleExprFromParam(msg, "pk"), ")")
	g.P("	endKey := make(fdb.Key, len(parentKey)+1)")
	g.P("	copy(endKey, parentKey)")
	g.P("	endKey[len(parentKey)] = 0xff")
	g.P("	rangeFuture := fdblayer.GetRange(tr, fdb.KeyRange{Begin: parentKey, End: endKey}, fdb.RangeOptions{})")
	if hasMut {
		g.P("	fieldPrefixRange, err := fdb.PrefixRange(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgs, "}))")
		g.P("	if err != nil {")
		g.P("		return nil, err")
		g.P("	}")
		g.P("	fieldFuture := fdblayer.GetRange(tr, fieldPrefixRange, fdb.RangeOptions{})")
	}
	g.P("	kvs := rangeFuture.GetSliceOrPanic()")
	g.P("	result := &", msg.Name, "WithChildren{}")
	g.P("	parentPKLen := ", len(msg.PrimaryKeyFields))
	g.P("	for _, kv := range kvs {")
	g.P("		if err := ctx.Err(); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		if bytes.Equal(kv.Key, parentKey) {")
	g.P("			parent := &", msg.Name, "{}")
	g.P("			if err := proto.Unmarshal(kv.Value, parent); err != nil {")
	g.P("				return nil, err")
	g.P("			}")
	g.P("			result.", msg.Name, " = parent")
	g.P("			continue")
	g.P("		}")
	g.P("		tpl, err := dir.Unpack(kv.Key)")
	g.P("		if err != nil || len(tpl) <= 2+parentPKLen {")
	g.P("			continue")
	g.P("		}")
	g.P("		childTypeID, ok := tpl[2+parentPKLen].(int64)")
	g.P("		if !ok {")
	g.P("			continue")
	g.P("		}")
	g.P("		switch childTypeID {")
	for _, child := range msg.Children {
		g.P("		case ", child.TypeID, ":")
		g.P("			childEntity := &", child.Name, "{}")
		g.P("			if err := proto.Unmarshal(kv.Value, childEntity); err != nil {")
		g.P("				return nil, err")
		g.P("			}")
		g.P("			result.", child.Name, "s = append(result.", child.Name, "s, childEntity)")
	}
	g.P("		}")
	g.P("	}")
	g.P("	if result.", msg.Name, " == nil {")
	g.P(`		return nil, fmt.Errorf("`, lowerName, ` %w", fdblayer.ErrNotFound)`)
	g.P("	}")
	if hasMut {
		g.P("	entity := result.", msg.Name)
		generateUnmarshalAtomicFields(g, msg, "fieldFuture.GetSliceOrPanic()", "\t")
	}
	g.P("	return result, nil")
	g.P("}")
	g.P()
}

func emitCascadeDeleteChildren(g Printer, msg ir.MessageSpec, indent string) {
	if len(msg.Children) == 0 {
		return
	}
	var cascadeChildren []ir.ChildSpec
	allCascadeSimple := true
	for _, c := range msg.Children {
		if c.OnDeleteCascade {
			cascadeChildren = append(cascadeChildren, c)
			if len(c.SecondaryIndexes) > 0 || c.HasMutationFields {
				allCascadeSimple = false
			}
		} else {
			allCascadeSimple = false
		}
	}
	if len(cascadeChildren) == 0 {
		return
	}

	g.P(indent, "// Cascade delete interleaved child records")
	g.P(indent, "{")
	in := indent + "\t"
	g.P(in, "childBegin := make(fdb.Key, len(key)+1)")
	g.P(in, "copy(childBegin, key)")
	g.P(in, "childBegin[len(key)] = 0x00")
	g.P(in, "childEnd := make(fdb.Key, len(key)+1)")
	g.P(in, "copy(childEnd, key)")
	g.P(in, "childEnd[len(key)] = 0xff")
	if allCascadeSimple {
		g.P(in, "tr.ClearRange(fdb.KeyRange{Begin: childBegin, End: childEnd})")
	} else {
		g.P(in, "childKVs := fdblayer.GetRange(tr, fdb.KeyRange{Begin: childBegin, End: childEnd}, fdb.RangeOptions{}).GetSliceOrPanic()")
		g.P(in, "parentPKLen := ", len(msg.PrimaryKeyFields))
		g.P(in, "for _, ckv := range childKVs {")
		g.P(in, "	ctpl, err := dir.Unpack(ckv.Key)")
		g.P(in, "	if err != nil || len(ctpl) <= 2+parentPKLen {")
		g.P(in, "		continue")
		g.P(in, "	}")
		g.P(in, "	childTypeID, ok := ctpl[2+parentPKLen].(int64)")
		g.P(in, "	if !ok {")
		g.P(in, "		continue")
		g.P(in, "	}")
		g.P(in, "	switch childTypeID {")
		for _, child := range cascadeChildren {
			g.P(in, "	case ", child.TypeID, ":")
			if len(child.SecondaryIndexes) == 0 && !child.HasMutationFields {
				g.P(in, "		tr.Clear(ckv.Key)")
			} else {
				g.P(in, "		var childPK ", child.Name, "PrimaryKey")
				for j, pf := range msg.PrimaryKeyFields {
					if len(msg.PrimaryKeyFields) == 1 {
						g.P(in, "		childPK.", pf.GoName, " = pk")
					} else {
						g.P(in, "		childPK.", pf.GoName, " = pk.", pf.GoName)
					}
					_ = j
				}
				for j, sf := range child.ChildPKSuffixFields {
					tplIdx := 3 + len(msg.PrimaryKeyFields) + j
					g.P(in, "		if len(ctpl) > ", tplIdx, " {")
					emitUnpackTupleElementToField(g, "ctpl["+itoa(tplIdx)+"]", "childPK."+sf.GoName, sf, in+"\t\t\t")
					g.P(in, "		}")
				}
				g.P(in, "		if err := New", child.Name, "Repository().Delete(ctx, tx, childPK); err != nil {")
				g.P(in, "			return err")
				g.P(in, "		}")
			}
		}
		g.P(in, "	}")
		g.P(in, "}")
		parentPKArgs := pkTupleArgsFromParam(msg)
		for _, child := range cascadeChildren {
			for _, idx := range child.SecondaryIndexes {
				if idx.Aggregate == ir.AggregateNone || !indexStartsWithParentPK(idx, msg.PrimaryKeyFields) {
					continue
				}
				if len(idx.Fields) == len(msg.PrimaryKeyFields) {
					g.P(in, "tr.Clear(dir.Pack(tuple.Tuple{int64(", child.TypeID, "), fdblayer.AggregateNamespace, ", idx.IndexID, ", ", parentPKArgs, "}))")
				} else {
					g.P(in, "if aggPrefix, err := fdb.PrefixRange(dir.Pack(tuple.Tuple{int64(", child.TypeID, "), fdblayer.AggregateNamespace, ", idx.IndexID, ", ", parentPKArgs, "})); err == nil {")
					g.P(in, "	tr.ClearRange(aggPrefix)")
					g.P(in, "}")
				}
			}
		}
	}
	g.P(indent, "}")
}

func indexStartsWithParentPK(idx ir.SecondaryIndexSpec, parentPK []ir.FieldSpec) bool {
	if len(idx.Fields) < len(parentPK) {
		return false
	}
	for i, pf := range parentPK {
		if idx.Fields[i].ProtoName != pf.ProtoName {
			return false
		}
	}
	return true
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
