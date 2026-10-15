package golang

import (
	"fmt"
	"strings"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

func emitStoringValueExpr(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, entityVar string, valVar string, indent string) {
	if !idx.IsCovering() {
		if idx.IsUnique {
			g.P(indent, valVar, " := tuple.Tuple{", pkTupleArgsFromEntity(msg, entityVar), "}.Pack()")
		} else {
			g.P(indent, valVar, " := []byte{}")
		}
		return
	}
	g.P(indent, "projectedBytes, err := proto.Marshal(&", msg.Name, "{")
	for _, sf := range idx.StoringFields {
		g.P(indent, "	", sf.GoName, ": ", entityVar, ".", sf.GoName, ",")
	}
	g.P(indent, "})")
	g.P(indent, "if err != nil {")
	g.P(indent, "	return err")
	g.P(indent, "}")
	if idx.IsUnique {
		g.P(indent, valVar, " := tuple.Tuple{", pkTupleArgsFromEntity(msg, entityVar), ", projectedBytes}.Pack()")
	} else {
		g.P(indent, valVar, " := projectedBytes")
	}
}

func emitIndexCreate(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, hasVS bool, indent string) {
	if idx.Aggregate != ir.AggregateNone {
		emitAggregateIndexCreate(g, msg, idx, indent)
		return
	}
	lowerName := strings.ToLower(msg.Name)
	typeIDConst := typeIDConstName(msg)
	pkArgs := pkTupleArgsFromEntity(msg, "entity")
	vsPKArgs := versionstampedPKTupleArgsWithVar(msg, "userVersion")

	if idx.IsFanOut {
		g.P(indent, "// Fan-out index (", idx.IndexID, ")")
		for i, f := range idx.Fields {
			if f.IsRepeated {
				mapType := MapKeyType(f)
				mapExpr := MapKeyExpr("item", f)
				g.P(indent, "{")
				if idx.IsSparse && HasNonRepeatedFields(idx.Fields) {
					var nonRep []ir.FieldSpec
					for _, nf := range idx.Fields {
						if !nf.IsRepeated {
							nonRep = append(nonRep, nf)
						}
					}
					g.P(indent, "	if ", SparseIndexPresentExpr(nonRep, "entity"), " {")
				}
				g.P(indent, "	seen := make(map[", mapType, "]struct{}, len(entity.", f.GoName, "))")
				g.P(indent, "	for _, item := range entity.", f.GoName, " {")
				g.P(indent, "		if _, dup := seen[", mapExpr, "]; dup {")
				g.P(indent, "			continue")
				g.P(indent, "		}")
				g.P(indent, "		seen[", mapExpr, "] = struct{}{}")
				if hasVS {
					emitVersionstampedFanOutIndexWrite(g, msg, idx, i, "entity", "userVersion", indent+"\t\t")
				} else {
					emitFanOutIndexWrite(g, msg, idx, i, "entity", true, indent+"\t\t")
				}
				g.P(indent, "	}")
				if idx.IsSparse && HasNonRepeatedFields(idx.Fields) {
					g.P(indent, "	}")
				}
				g.P(indent, "}")
			}
		}
		return
	}

	g.P(indent, "// Secondary index (", idx.IndexID, ")")
	in := indent
	if idx.IsSparse {
		g.P(indent, "if ", SparseIndexPresentExpr(idx.Fields, "entity"), " {")
		in = indent + "\t"
	} else {
		g.P(indent, "{")
		in = indent + "\t"
	}

	if idx.IsUnique {
		joined := JoinFieldNames(idx.Fields)
		g.P(in, "idxKey := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "entity"), "})")
		g.P(in, "if existingIdx := tr.Get(idxKey).MustGet(); existingIdx != nil {")
		g.P(in, `	return fmt.Errorf("`, lowerName, ` unique index `, joined, `: %w", fdblayer.ErrUniqueConstraintViolation)`)
		g.P(in, "}")
		emitStoringValueExpr(g, msg, idx, "entity", "idxVal", in)
		g.P(in, "tr.Set(idxKey, idxVal)")
	} else if hasVS {
		g.P(in, "idxKey, err := dir.PackWithVersionstamp(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "entity"), ", ", vsPKArgs, "})")
		g.P(in, "if err != nil {")
		g.P(in, "	return err")
		g.P(in, "}")
		emitStoringValueExpr(g, msg, idx, "entity", "idxVal", in)
		g.P(in, "tr.SetVersionstampedKey(idxKey, idxVal)")
	} else {
		emitStoringValueExpr(g, msg, idx, "entity", "idxVal", in)
		g.P(in, "tr.Set(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "entity"), ", ", pkArgs, "}), idxVal)")
	}

	g.P(indent, "}")
}

func emitIndexSet(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, indent string) {
	if idx.Aggregate != ir.AggregateNone {
		emitAggregateIndexSet(g, msg, idx, indent)
		return
	}
	lowerName := strings.ToLower(msg.Name)
	typeIDConst := typeIDConstName(msg)
	pkArgsEntity := pkTupleArgsFromEntity(msg, "entity")
	pkArgsOld := pkTupleArgsFromEntity(msg, "old")

	if idx.IsFanOut {
		g.P(indent, "// Fan-out index (", idx.IndexID, ")")
		for i, f := range idx.Fields {
			if f.IsRepeated {
				cond := "old != nil"
				if HasNonRepeatedFields(idx.Fields) {
					cond += " && " + NonRepeatedFieldsEqual(idx.Fields)
				}
				mapType := MapKeyType(f)
				mapExpr := MapKeyExpr("item", f)

				g.P(indent, "if ", cond, " {")
				g.P(indent, "	oldSet := make(map[", mapType, "]struct{}, len(old.", f.GoName, "))")
				g.P(indent, "	for _, item := range old.", f.GoName, " {")
				g.P(indent, "		oldSet[", mapExpr, "] = struct{}{}")
				g.P(indent, "	}")
				g.P(indent, "	newSet := make(map[", mapType, "]struct{}, len(entity.", f.GoName, "))")
				g.P(indent, "	for _, item := range entity.", f.GoName, " {")
				g.P(indent, "		newSet[", mapExpr, "] = struct{}{}")
				g.P(indent, "	}")
				g.P(indent, "	for _, item := range old.", f.GoName, " {")
				g.P(indent, "		if _, keep := newSet[", mapExpr, "]; !keep {")
				g.P(indent, "			newSet[", mapExpr, "] = struct{}{}")
				emitFanOutIndexWrite(g, msg, idx, i, "old", false, indent+"\t\t\t")
				g.P(indent, "		}")
				g.P(indent, "	}")
				g.P(indent, "	for _, item := range entity.", f.GoName, " {")
				g.P(indent, "		if _, existed := oldSet[", mapExpr, "]; !existed {")
				g.P(indent, "			oldSet[", mapExpr, "] = struct{}{}")
				emitFanOutIndexWrite(g, msg, idx, i, "entity", true, indent+"\t\t\t")
				g.P(indent, "		}")
				g.P(indent, "	}")
				g.P(indent, "} else {")
				if HasNonRepeatedFields(idx.Fields) {
					g.P(indent, "	if old != nil {")
					g.P(indent, "		seenOld := make(map[", mapType, "]struct{}, len(old.", f.GoName, "))")
					g.P(indent, "		for _, item := range old.", f.GoName, " {")
					g.P(indent, "			if _, dup := seenOld[", mapExpr, "]; dup {")
					g.P(indent, "				continue")
					g.P(indent, "			}")
					g.P(indent, "			seenOld[", mapExpr, "] = struct{}{}")
					emitFanOutIndexWrite(g, msg, idx, i, "old", false, indent+"\t\t\t")
					g.P(indent, "		}")
					g.P(indent, "	}")
				}
				g.P(indent, "	seen := make(map[", mapType, "]struct{}, len(entity.", f.GoName, "))")
				g.P(indent, "	for _, item := range entity.", f.GoName, " {")
				g.P(indent, "		if _, dup := seen[", mapExpr, "]; dup {")
				g.P(indent, "			continue")
				g.P(indent, "		}")
				g.P(indent, "		seen[", mapExpr, "] = struct{}{}")
				emitFanOutIndexWrite(g, msg, idx, i, "entity", true, indent+"\t\t")
				g.P(indent, "	}")
				g.P(indent, "}")
			}
		}
		return
	}

	var keyDiffParts []string
	for _, f := range idx.Fields {
		keyDiffParts = append(keyDiffParts, FieldNotEqual("old."+f.GoName, "entity."+f.GoName, f))
	}
	keyDiffExpr := strings.Join(keyDiffParts, " || ")

	var storeDiffParts []string
	for _, sf := range idx.StoringFields {
		storeDiffParts = append(storeDiffParts, FieldNotEqual("old."+sf.GoName, "entity."+sf.GoName, sf))
	}
	anyDiffExpr := keyDiffExpr
	if len(storeDiffParts) > 0 {
		anyDiffExpr = keyDiffExpr + " || " + strings.Join(storeDiffParts, " || ")
	}

	g.P(indent, "// Secondary index (", idx.IndexID, ")")
	if !idx.IsSparse && !idx.IsUnique && !idx.IsCovering() {
		g.P(indent, "if old == nil || ", keyDiffExpr, " {")
		g.P(indent, "	if old != nil {")
		g.P(indent, "		tr.Clear(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "old"), ", ", pkArgsOld, "}))")
		g.P(indent, "	}")
		g.P(indent, "	tr.Set(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "entity"), ", ", pkArgsEntity, "}), []byte{})")
		g.P(indent, "}")
		return
	}

	g.P(indent, "{")
	in := indent + "\t"
	if idx.IsSparse {
		g.P(in, "oldPresent := old != nil && (", SparseIndexPresentExpr(idx.Fields, "old"), ")")
		g.P(in, "newPresent := ", SparseIndexPresentExpr(idx.Fields, "entity"))
	} else {
		g.P(in, "oldPresent := old != nil")
		g.P(in, "newPresent := true")
	}

	if idx.IsUnique {
		joined := JoinFieldNames(idx.Fields)
		g.P(in, "if oldPresent && (!newPresent || ", keyDiffExpr, ") {")
		g.P(in, "	tr.Clear(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "old"), "}))")
		g.P(in, "}")
		g.P(in, "if newPresent && (!oldPresent || ", anyDiffExpr, ") {")
		g.P(in, "	newIdxKey := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "entity"), "})")
		g.P(in, "	if !oldPresent || ", keyDiffExpr, " {")
		g.P(in, "		if existingIdx := tr.Get(newIdxKey).MustGet(); existingIdx != nil {")
		g.P(in, `			return fmt.Errorf("`, lowerName, ` unique index `, joined, `: %w", fdblayer.ErrUniqueConstraintViolation)`)
		g.P(in, "		}")
		g.P(in, "	}")
		emitStoringValueExpr(g, msg, idx, "entity", "idxVal", in+"\t")
		g.P(in, "	tr.Set(newIdxKey, idxVal)")
		g.P(in, "}")
	} else {
		g.P(in, "if oldPresent && (!newPresent || ", keyDiffExpr, ") {")
		g.P(in, "	tr.Clear(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "old"), ", ", pkArgsOld, "}))")
		g.P(in, "}")
		g.P(in, "if newPresent && (!oldPresent || ", anyDiffExpr, ") {")
		emitStoringValueExpr(g, msg, idx, "entity", "idxVal", in+"\t")
		g.P(in, "	tr.Set(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "entity"), ", ", pkArgsEntity, "}), idxVal)")
		g.P(in, "}")
	}
	g.P(indent, "}")
}

func emitIndexDelete(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, entityVar string, indent string) {
	if idx.Aggregate != ir.AggregateNone {
		emitAggregateIndexDelete(g, msg, idx, entityVar, indent)
		return
	}
	typeIDConst := typeIDConstName(msg)
	pkArgsEntity := pkTupleArgsFromEntity(msg, entityVar)

	if idx.IsFanOut {
		g.P(indent, "// Fan-out index (", idx.IndexID, ")")
		for i, f := range idx.Fields {
			if f.IsRepeated {
				mapType := MapKeyType(f)
				mapExpr := MapKeyExpr("item", f)
				g.P(indent, "{")
				g.P(indent, "	seen := make(map[", mapType, "]struct{}, len(", entityVar, ".", f.GoName, "))")
				g.P(indent, "	for _, item := range ", entityVar, ".", f.GoName, " {")
				g.P(indent, "		if _, dup := seen[", mapExpr, "]; dup {")
				g.P(indent, "			continue")
				g.P(indent, "		}")
				g.P(indent, "		seen[", mapExpr, "] = struct{}{}")
				emitFanOutIndexWrite(g, msg, idx, i, entityVar, false, indent+"\t\t")
				g.P(indent, "	}")
				g.P(indent, "}")
			}
		}
		return
	}

	g.P(indent, "// Secondary index (", idx.IndexID, ")")
	in := indent
	if idx.IsSparse {
		g.P(indent, "if ", SparseIndexPresentExpr(idx.Fields, entityVar), " {")
		in = indent + "\t"
	}
	if idx.IsUnique {
		g.P(in, "tr.Clear(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, entityVar), "}))")
	} else {
		g.P(in, "tr.Clear(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, entityVar), ", ", pkArgsEntity, "}))")
	}
	if idx.IsSparse {
		g.P(indent, "}")
	}
}

func generateGetUniqueByIndex(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec) {
	lowerName := strings.ToLower(msg.Name)
	joinedNames := JoinFieldNames(idx.Fields)
	hasMut := ir.HasMutationFields(msg)
	typeIDConst := typeIDConstName(msg)

	var idxPackArgs []string
	for _, f := range idx.Fields {
		idxPackArgs = append(idxPackArgs, PackField(f.GoName, f))
	}

	g.P("// GetUniqueBy", joinedNames, " retrieves a single ", msg.Name, " entity by its unique ", joinedNames, " index.")
	g.P("func (r *", lowerName, "Repository) GetUniqueBy", joinedNames, "(ctx context.Context, tx fdblayer.ReadTx, ", fieldParamDecls(idx.Fields), ") (*", msg.Name, ", error) {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	tr := tx.ReadTransaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	g.P("	idxKey := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", strings.Join(idxPackArgs, ", "), "})")
	g.P("	idxVal := tr.Get(idxKey).MustGet()")
	g.P("	if idxVal == nil {")
	g.P(`		return nil, fmt.Errorf("`, lowerName, ` %w", fdblayer.ErrNotFound)`)
	g.P("	}")
	g.P("	pkTpl, err := tuple.Unpack(fdb.Key(idxVal))")
	g.P("	if err != nil || len(pkTpl) < ", len(msg.PrimaryKeyFields), " {")
	g.P(`		return nil, fmt.Errorf("failed to unpack unique index primary key: %w", err)`)
	g.P("	}")
	g.P("	pkTuple := pkTpl[:", len(msg.PrimaryKeyFields), "]")
	emitDataKeyFromPKTuple(g, msg, "pkTuple", "keyTpl", "\t")
	g.P("	valueFuture := tr.Get(dir.Pack(keyTpl))")
	if hasMut {
		g.P("	fieldTpl := make(tuple.Tuple, 2+len(pkTuple))")
		g.P("	fieldTpl[0] = ", typeIDConst)
		g.P("	fieldTpl[1] = fdblayer.FieldNamespace")
		g.P("	copy(fieldTpl[2:], pkTuple)")
		g.P("	fieldRange, err := fdb.PrefixRange(dir.Pack(fieldTpl))")
		g.P("	if err != nil {")
		g.P("		return nil, err")
		g.P("	}")
		g.P("	fieldFuture := fdblayer.GetRange(tr, fieldRange, fdb.RangeOptions{})")
	}
	g.P("	value := valueFuture.MustGet()")
	g.P("	if value == nil {")
	g.P(`		return nil, fmt.Errorf("`, lowerName, ` %w", fdblayer.ErrNotFound)`)
	g.P("	}")
	g.P("	entity := &", msg.Name, "{}")
	g.P("	if err := proto.Unmarshal(value, entity); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	if hasMut {
		generateUnmarshalAtomicFields(g, msg, "fieldFuture.GetSliceOrPanic()", "\t")
	}
	g.P("	return entity, nil")
	g.P("}")
	g.P()
}

func generateGetByIndex(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, queryFields []ir.FieldSpec) {
	lowerName := strings.ToLower(msg.Name)
	joinedNames := JoinFieldNames(queryFields)
	hasVS := ir.MsgHasVersionstampPK(msg)
	isPartialPrefix := len(queryFields) < len(idx.Fields) || idx.IsUnique
	typeIDConst := typeIDConstName(msg)

	var idxPackArgs []string
	for _, f := range queryFields {
		idxPackArgs = append(idxPackArgs, PackField(f.GoName, f))
	}

	g.P("// GetBy", joinedNames, " retrieves a paginated page of ", msg.Name, " entities by their ", joinedNames, " index.")
	g.P("func (r *", lowerName, "Repository) GetBy", joinedNames, "(ctx context.Context, tx fdblayer.ReadTx, ", fieldParamDecls(queryFields), ", opts fdblayer.PaginationOptions) (*fdblayer.PaginatedResult[*", msg.Name, "], error) {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	tr := tx.ReadTransaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	g.P("	cursorBegin, err := opts.EffectiveBegin()")
	g.P("	if err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	result := &fdblayer.PaginatedResult[*", msg.Name, "]{")
	g.P("		Items: make([]*", msg.Name, ", 0),")
	g.P("	}")
	g.P("	baseTpl := tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", strings.Join(idxPackArgs, ", "), "}")
	g.P("	indexKeyPrefix := dir.Pack(baseTpl)")
	g.P("	indexRange, err := fdb.PrefixRange(indexKeyPrefix)")
	g.P("	if err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	rangeBegin := indexRange.Begin")
	g.P("	rangeEnd := indexRange.End")
	g.P("	if len(cursorBegin) > 0 {")
	g.P("		cursorTpl := make(tuple.Tuple, len(baseTpl)+len(cursorBegin))")
	g.P("		copy(cursorTpl, baseTpl)")
	g.P("		for j, el := range cursorBegin {")
	g.P("			cursorTpl[len(baseTpl)+j] = fdblayer.NormalizeTupleElement(el)")
	g.P("		}")
	if hasVS {
		suffixIdxFields := len(idx.Fields) - len(queryFields)
		for j, f := range msg.PrimaryKeyFields {
			if f.IsVersionstamp {
				vsOffset := suffixIdxFields + j
				g.P("		if len(cursorBegin) > ", vsOffset, " {")
				g.P("			if rawVS, ok := cursorBegin[", vsOffset, "].([]byte); ok {")
				g.P("				cursorTpl[len(baseTpl)+", vsOffset, "] = fdblayer.BytesToVersionstamp(rawVS)")
				g.P("			}")
				g.P("		}")
			}
		}
	}
	g.P("		cursorKey := dir.Pack(cursorTpl)")
	g.P("		if opts.Reverse {")
	g.P("			cursorRange, err := fdb.PrefixRange(cursorKey)")
	g.P("			if err != nil {")
	g.P("				return nil, err")
	g.P("			}")
	g.P("			if string(cursorRange.End.FDBKey()) < string(rangeEnd.FDBKey()) {")
	g.P("				rangeEnd = cursorRange.End")
	g.P("			}")
	g.P("		} else if string(cursorKey) > string(rangeBegin.FDBKey()) {")
	g.P("			rangeBegin = cursorKey")
	g.P("		}")
	g.P("	}")
	g.P("	if string(rangeBegin.FDBKey()) >= string(rangeEnd.FDBKey()) {")
	g.P("		return result, nil")
	g.P("	}")
	g.P("	rangeOpts := fdb.RangeOptions{Reverse: opts.Reverse}")
	g.P("	if opts.Limit > 0 {")
	g.P("		rangeOpts.Limit = opts.Limit + 1")
	g.P("	}")
	g.P("	kvs := fdblayer.GetRange(tr, fdb.KeyRange{Begin: rangeBegin, End: rangeEnd}, rangeOpts).GetSliceOrPanic()")
	emitIndexKVsToEntities(g, msg, idx, len(queryFields), isPartialPrefix)
	g.P("	return result, nil")
	g.P("}")
	g.P()
}

func generateScanByIndex(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, rs ir.IndexRangeScanSpec) {
	lowerName := strings.ToLower(msg.Name)
	allFields := rs.AllFields()
	joinedNames := JoinFieldNames(allFields)
	typeIDConst := typeIDConstName(msg)
	rangeParam := rs.RangeField.GoName + "Range"

	var baseTupleParts []string
	baseTupleParts = append(baseTupleParts, typeIDConst, "fdblayer.IndexNamespace", fmt.Sprintf("%d", idx.IndexID))
	for _, f := range rs.EqualityFields {
		baseTupleParts = append(baseTupleParts, PackField(f.GoName, f))
	}

	g.P("// ScanBy", joinedNames, " performs a bounded range scan on the ", joinedNames, " secondary index.")
	g.P("func (r *", lowerName, "Repository) ScanBy", joinedNames, "(ctx context.Context, tx fdblayer.ReadTx, ", rangeScanParamDecls(rs), ", opts fdblayer.PaginationOptions) (*fdblayer.PaginatedResult[*", msg.Name, "], error) {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	tr := tx.ReadTransaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	g.P("	cursorBegin, err := opts.EffectiveBegin()")
	g.P("	if err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	result := &fdblayer.PaginatedResult[*", msg.Name, "]{")
	g.P("		Items: make([]*", msg.Name, ", 0),")
	g.P("	}")
	g.P("	baseTpl := tuple.Tuple{", strings.Join(baseTupleParts, ", "), "}")
	g.P("	kr, ok, err := fdblayer.ComputeBoundedKeyRange(dir, baseTpl, ", PackField(rangeParam+".Start", rs.RangeField), ", ", PackField(rangeParam+".End", rs.RangeField), ", ", rangeParam, ".UnboundedStart, ", rangeParam, ".UnboundedEnd, ", rangeParam, ".Kind, cursorBegin, opts.Reverse)")
	g.P("	if err != nil || !ok {")
	g.P("		return result, err")
	g.P("	}")
	g.P("	rangeOpts := fdb.RangeOptions{Reverse: opts.Reverse}")
	g.P("	if opts.Limit > 0 {")
	g.P("		rangeOpts.Limit = opts.Limit + 1")
	g.P("	}")
	g.P("	kvs := fdblayer.GetRange(tr, kr, rangeOpts).GetSliceOrPanic()")
	emitIndexKVsToEntities(g, msg, idx, len(rs.EqualityFields), true)
	g.P("	return result, nil")
	g.P("}")
	g.P()
}

func emitIndexKVsToEntities(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, cursorPrefixFields int, isPartialPrefix bool) {
	hasMut := ir.HasMutationFields(msg)
	hasVS := ir.MsgHasVersionstampPK(msg)
	typeIDConst := typeIDConstName(msg)

	g.P("	pageLen := len(kvs)")
	g.P("	pkIndexStart := 3 + ", len(idx.Fields))
	cursorStartVar := "pkIndexStart"
	if isPartialPrefix {
		g.P("	cursorStart := 3 + ", cursorPrefixFields)
		cursorStartVar = "cursorStart"
	}
	g.P("	if opts.Limit > 0 && pageLen > opts.Limit {")
	g.P("		nextTpl, err := dir.Unpack(kvs[opts.Limit].Key)")
	g.P("		if err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		if len(nextTpl) > ", cursorStartVar, " {")
	g.P("			result.HasMore = true")
	g.P("			result.NextKey = nextTpl[", cursorStartVar, ":]")
	g.P("			result.NextPageToken = result.NextKey.Pack()")
	g.P("		}")
	g.P("		kvs = kvs[:opts.Limit]")
	g.P("	}")
	g.P("	futures := make([]fdb.FutureByteSlice, len(kvs))")
	if hasVS {
		g.P("	pkTuples := make([]tuple.Tuple, len(kvs))")
	}
	if hasMut {
		g.P("	fieldFutures := make([]fdblayer.RangeFuture, len(kvs))")
	}
	g.P("	for i, kv := range kvs {")
	g.P("		if err := ctx.Err(); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	if idx.IsUnique {
		g.P("		_ = pkIndexStart")
		g.P("		valTpl, err := tuple.Unpack(fdb.Key(kv.Value))")
		g.P("		if err != nil || len(valTpl) < ", len(msg.PrimaryKeyFields), " {")
		g.P("			continue")
		g.P("		}")
		g.P("		pkTuple := valTpl[:", len(msg.PrimaryKeyFields), "]")
	} else {
		g.P("		tpl, err := dir.Unpack(kv.Key)")
		g.P("		if err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		if len(tpl) <= pkIndexStart {")
		g.P("			continue")
		g.P("		}")
		g.P("		pkTuple := tpl[pkIndexStart:]")
	}
	if hasVS {
		g.P("		pkTuples[i] = pkTuple")
	}
	emitDataKeyFromPKTuple(g, msg, "pkTuple", "keyTpl", "\t\t")
	g.P("		futures[i] = tr.Get(dir.Pack(keyTpl))")
	if hasMut {
		g.P("		fieldTpl := make(tuple.Tuple, 2+len(pkTuple))")
		g.P("		fieldTpl[0] = ", typeIDConst)
		g.P("		fieldTpl[1] = fdblayer.FieldNamespace")
		g.P("		copy(fieldTpl[2:], pkTuple)")
		g.P("		fieldRange, err := fdb.PrefixRange(dir.Pack(fieldTpl))")
		g.P("		if err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		fieldFutures[i] = fdblayer.GetRange(tr, fieldRange, fdb.RangeOptions{})")
	}
	g.P("	}")
	iterVar := "_"
	if hasMut || hasVS {
		iterVar = "i"
	}
	g.P("	for ", iterVar, ", future := range futures {")
	g.P("		if err := ctx.Err(); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		if future == nil {")
	g.P("			continue")
	g.P("		}")
	g.P("		value := future.MustGet()")
	g.P("		if value == nil {")
	g.P("			continue")
	g.P("		}")
	g.P("		entity := &", msg.Name, "{}")
	g.P("		if err := proto.Unmarshal(value, entity); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	if hasVS {
		for j, f := range msg.PrimaryKeyFields {
			if f.IsVersionstamp {
				g.P("		if len(pkTuples[i]) > ", j, " {")
				g.P("			if vs, ok := pkTuples[i][", j, "].(tuple.Versionstamp); ok {")
				g.P("				entity.", f.GoName, " = vs.Bytes()")
				g.P("			}")
				g.P("		}")
			}
		}
	}
	if hasMut {
		generateUnmarshalAtomicFields(g, msg, "fieldFutures[i].GetSliceOrPanic()", "\t\t")
	}
	g.P("		result.Items = append(result.Items, entity)")
	g.P("	}")
}

func generateGetCoveredByIndex(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, queryFields []ir.FieldSpec) {
	lowerName := strings.ToLower(msg.Name)
	joinedNames := JoinFieldNames(queryFields)
	isPartialPrefix := len(queryFields) < len(idx.Fields) || idx.IsUnique
	typeIDConst := typeIDConstName(msg)

	var idxPackArgs []string
	for _, f := range queryFields {
		idxPackArgs = append(idxPackArgs, PackField(f.GoName, f))
	}

	g.P("// GetCoveredBy", joinedNames, " reconstructs projected ", msg.Name, " entities directly from the covering ", joinedNames, " index in 1 RTT.")
	g.P("func (r *", lowerName, "Repository) GetCoveredBy", joinedNames, "(ctx context.Context, tx fdblayer.ReadTx, ", fieldParamDecls(queryFields), ", opts fdblayer.PaginationOptions) (*fdblayer.PaginatedResult[*", msg.Name, "], error) {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	tr := tx.ReadTransaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	g.P("	cursorBegin, err := opts.EffectiveBegin()")
	g.P("	if err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	result := &fdblayer.PaginatedResult[*", msg.Name, "]{")
	g.P("		Items: make([]*", msg.Name, ", 0),")
	g.P("	}")
	g.P("	baseTpl := tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", strings.Join(idxPackArgs, ", "), "}")
	g.P("	kr, ok, err := fdblayer.ComputeBoundedKeyRange(dir, baseTpl, nil, nil, true, true, fdblayer.ClosedOpen, cursorBegin, opts.Reverse)")
	g.P("	if err != nil || !ok {")
	g.P("		return result, err")
	g.P("	}")
	g.P("	rangeOpts := fdb.RangeOptions{Reverse: opts.Reverse}")
	g.P("	if opts.Limit > 0 {")
	g.P("		rangeOpts.Limit = opts.Limit + 1")
	g.P("	}")
	g.P("	kvs := fdblayer.GetRange(tr, kr, rangeOpts).GetSliceOrPanic()")
	emitCoveredIndexKVsToEntities(g, msg, idx, len(queryFields), isPartialPrefix)
	g.P("	return result, nil")
	g.P("}")
	g.P()
}

func generateScanCoveredByIndex(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, rs ir.IndexRangeScanSpec) {
	lowerName := strings.ToLower(msg.Name)
	allFields := rs.AllFields()
	joinedNames := JoinFieldNames(allFields)
	typeIDConst := typeIDConstName(msg)
	rangeParam := rs.RangeField.GoName + "Range"

	var baseTupleParts []string
	baseTupleParts = append(baseTupleParts, typeIDConst, "fdblayer.IndexNamespace", fmt.Sprintf("%d", idx.IndexID))
	for _, f := range rs.EqualityFields {
		baseTupleParts = append(baseTupleParts, PackField(f.GoName, f))
	}

	g.P("// ScanCoveredBy", joinedNames, " performs a 1-RTT covering range scan on the ", joinedNames, " secondary index.")
	g.P("func (r *", lowerName, "Repository) ScanCoveredBy", joinedNames, "(ctx context.Context, tx fdblayer.ReadTx, ", rangeScanParamDecls(rs), ", opts fdblayer.PaginationOptions) (*fdblayer.PaginatedResult[*", msg.Name, "], error) {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	tr := tx.ReadTransaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	g.P("	cursorBegin, err := opts.EffectiveBegin()")
	g.P("	if err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	result := &fdblayer.PaginatedResult[*", msg.Name, "]{")
	g.P("		Items: make([]*", msg.Name, ", 0),")
	g.P("	}")
	g.P("	baseTpl := tuple.Tuple{", strings.Join(baseTupleParts, ", "), "}")
	g.P("	kr, ok, err := fdblayer.ComputeBoundedKeyRange(dir, baseTpl, ", PackField(rangeParam+".Start", rs.RangeField), ", ", PackField(rangeParam+".End", rs.RangeField), ", ", rangeParam, ".UnboundedStart, ", rangeParam, ".UnboundedEnd, ", rangeParam, ".Kind, cursorBegin, opts.Reverse)")
	g.P("	if err != nil || !ok {")
	g.P("		return result, err")
	g.P("	}")
	g.P("	rangeOpts := fdb.RangeOptions{Reverse: opts.Reverse}")
	g.P("	if opts.Limit > 0 {")
	g.P("		rangeOpts.Limit = opts.Limit + 1")
	g.P("	}")
	g.P("	kvs := fdblayer.GetRange(tr, kr, rangeOpts).GetSliceOrPanic()")
	emitCoveredIndexKVsToEntities(g, msg, idx, len(rs.EqualityFields), true)
	g.P("	return result, nil")
	g.P("}")
	g.P()
}

func emitCoveredIndexKVsToEntities(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, cursorPrefixFields int, isPartialPrefix bool) {
	g.P("	pageLen := len(kvs)")
	g.P("	pkIndexStart := 3 + ", len(idx.Fields))
	cursorStartVar := "pkIndexStart"
	if isPartialPrefix {
		g.P("	cursorStart := 3 + ", cursorPrefixFields)
		cursorStartVar = "cursorStart"
	}
	g.P("	if opts.Limit > 0 && pageLen > opts.Limit {")
	g.P("		nextTpl, err := dir.Unpack(kvs[opts.Limit].Key)")
	g.P("		if err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		if len(nextTpl) > ", cursorStartVar, " {")
	g.P("			result.HasMore = true")
	g.P("			result.NextKey = nextTpl[", cursorStartVar, ":]")
	g.P("			result.NextPageToken = result.NextKey.Pack()")
	g.P("		}")
	g.P("		kvs = kvs[:opts.Limit]")
	g.P("	}")
	g.P("	for _, kv := range kvs {")
	g.P("		if err := ctx.Err(); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		tpl, err := dir.Unpack(kv.Key)")
	g.P("		if err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		entity := &", msg.Name, "{}")
	if idx.IsUnique {
		g.P("		_ = pkIndexStart")
		g.P("		valTpl, err := tuple.Unpack(fdb.Key(kv.Value))")
		g.P("		if err != nil || len(valTpl) < ", len(msg.PrimaryKeyFields)+1, " {")
		g.P("			continue")
		g.P("		}")
		g.P("		if storingBytes, ok := valTpl[", len(msg.PrimaryKeyFields), "].([]byte); ok && len(storingBytes) > 0 {")
		g.P("			if err := proto.Unmarshal(storingBytes, entity); err != nil {")
		g.P("				return nil, err")
		g.P("			}")
		g.P("		}")
		g.P("		pkTuple := valTpl[:", len(msg.PrimaryKeyFields), "]")
	} else {
		g.P("		if len(tpl) <= pkIndexStart {")
		g.P("			continue")
		g.P("		}")
		g.P("		if len(kv.Value) > 0 {")
		g.P("			if err := proto.Unmarshal(kv.Value, entity); err != nil {")
		g.P("				return nil, err")
		g.P("			}")
		g.P("		}")
		g.P("		pkTuple := tpl[pkIndexStart:]")
	}
	for j, idxField := range idx.Fields {
		if idxField.IsRepeated {
			continue
		}
		g.P("		if len(tpl) > ", 3+j, " {")
		emitUnpackTupleElementToField(g, fmt.Sprintf("tpl[%d]", 3+j), "entity."+idxField.GoName, idxField, "\t\t\t")
		g.P("		}")
	}
	for j, pkField := range msg.PrimaryKeyFields {
		g.P("		if len(pkTuple) > ", j, " {")
		emitUnpackTupleElementToField(g, fmt.Sprintf("pkTuple[%d]", j), "entity."+pkField.GoName, pkField, "\t\t\t")
		g.P("		}")
	}
	g.P("		result.Items = append(result.Items, entity)")
	g.P("	}")
}

func emitFanOutIndexWrite(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, repeatedIdx int, varName string, isSet bool, indent string) {
	op := "Clear"
	if isSet {
		op = "Set"
	}
	g.P(indent, "tr.", op, "(dir.Pack(tuple.Tuple{", typeIDConstName(msg), ", fdblayer.IndexNamespace, ", idx.IndexID, ",")
	for j, sf := range idx.Fields {
		if j == repeatedIdx {
			g.P(indent, "\t", PackField("item", sf), ",")
		} else {
			g.P(indent, "\t", PackField(varName+"."+sf.GoName, sf), ",")
		}
	}
	for _, pkf := range msg.PrimaryKeyFields {
		g.P(indent, "\t", PackField(varName+"."+pkf.GoName, pkf), ",")
	}
	if isSet {
		g.P(indent, "}), []byte{})")
	} else {
		g.P(indent, "}))")
	}
}

func emitVersionstampedFanOutIndexWrite(g Printer, msg ir.MessageSpec, idx ir.SecondaryIndexSpec, repeatedIdx int, varName string, userVersionVar string, indent string) {
	g.P(indent, "idxKey, err := dir.PackWithVersionstamp(tuple.Tuple{", typeIDConstName(msg), ", fdblayer.IndexNamespace, ", idx.IndexID, ",")
	for j, sf := range idx.Fields {
		if j == repeatedIdx {
			g.P(indent, "\t", PackField("item", sf), ",")
		} else {
			g.P(indent, "\t", PackField(varName+"."+sf.GoName, sf), ",")
		}
	}
	for _, pkf := range msg.PrimaryKeyFields {
		if pkf.IsVersionstamp {
			g.P(indent, "\t", "tuple.IncompleteVersionstamp(", userVersionVar, "),")
		} else {
			g.P(indent, "\t", PackField(varName+"."+pkf.GoName, pkf), ",")
		}
	}
	g.P(indent, "})")
	g.P(indent, "if err != nil {")
	g.P(indent, "	return err")
	g.P(indent, "}")
	g.P(indent, "tr.SetVersionstampedKey(idxKey, []byte{})")
}
