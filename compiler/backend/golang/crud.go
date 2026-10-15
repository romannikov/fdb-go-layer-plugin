package golang

import (
	"fmt"
	"strings"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

func generateCreate(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	hasVS := ir.MsgHasVersionstampPK(msg)
	hasMut := ir.HasMutationFields(msg)
	typeIDConst := typeIDConstName(msg)

	g.P("// Create creates a new ", msg.Name, " entity in the database.")
	g.P("func (r *", lowerName, "Repository) Create(ctx context.Context, tx *fdblayer.Tx, entity *", msg.Name, ") error {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return err")
	g.P("	}")
	g.P("	tr := tx.Transaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	if hasVS {
		g.P("	userVersion, err := tx.NextUserVersion()")
		g.P("	if err != nil {")
		g.P("		return err")
		g.P("	}")
		g.P("	key, err := dir.PackWithVersionstamp(tuple.Tuple{", typeIDConst, ", fdblayer.DataNamespace, ", versionstampedPKTupleArgsWithVar(msg, "userVersion"), "})")
		g.P("	if err != nil {")
		g.P("		return err")
		g.P("	}")
	} else {
		g.P("	key := dir.Pack(", dataKeyTupleExprFromEntity(msg, "entity"), ")")
		g.P("	if existing := tr.Get(key).MustGet(); existing != nil {")
		g.P(`		return fmt.Errorf("`, lowerName, ` %w", fdblayer.ErrAlreadyExists)`)
		g.P("	}")
	}
	g.P()

	for _, idx := range msg.SecondaryIndexes {
		emitIndexCreate(g, msg, idx, hasVS, "\t")
		g.P()
	}

	generateMarshalWithZeroedAtomics(g, msg)

	if hasVS {
		g.P("	tr.SetVersionstampedKey(key, value)")
	} else {
		g.P("	tr.Set(key, value)")
	}
	g.P()

	if hasMut {
		g.P("	// Store non-zero initial atomic fields in separate keys")
		pkArgs := pkTupleArgsFromEntity(msg, "entity")
		vsPKArgs := versionstampedPKTupleArgsWithVar(msg, "userVersion")
		for _, f := range msg.Fields {
			if f.Mutation != ir.MutationNone {
				if f.Mutation == ir.MutationMax && !f.IsUnsigned {
					g.P("	{")
				} else {
					g.P("	if entity.", f.GoName, " != 0 {")
				}
				if hasVS {
					g.P("		fieldKey, err := dir.PackWithVersionstamp(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", vsPKArgs, ", ", f.Number, "})")
					g.P("		if err != nil {")
					g.P("			return err")
					g.P("		}")
					g.P("		buf := make([]byte, 8)")
					g.P("		binary.LittleEndian.PutUint64(buf, ", encodeAtomicUint64Expr("entity."+f.GoName, f), ")")
					g.P("		tr.SetVersionstampedKey(fieldKey, buf)")
				} else {
					g.P("		fieldKey := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgs, ", ", f.Number, "})")
					g.P("		buf := make([]byte, 8)")
					g.P("		binary.LittleEndian.PutUint64(buf, ", encodeAtomicUint64Expr("entity."+f.GoName, f), ")")
					g.P("		tr.", MutationVerb(f.Mutation), "(fieldKey, buf)")
				}
				g.P("	}")
			}
		}
		g.P()
	}

	g.P("	return nil")
	g.P("}")
	g.P()
}

func generateGet(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	pkType := pkTypeExpr(msg)
	pkArgs := pkTupleArgsFromParam(msg)
	hasMut := ir.HasMutationFields(msg)
	typeIDConst := typeIDConstName(msg)

	g.P("// Get retrieves a ", msg.Name, " entity by its primary key.")
	g.P("func (r *", lowerName, "Repository) Get(ctx context.Context, tx fdblayer.ReadTx, pk ", pkType, ") (*", msg.Name, ", error) {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	tr := tx.ReadTransaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	g.P("	key := dir.Pack(", dataKeyTupleExprFromParam(msg, "pk"), ")")
	g.P("	valueFuture := tr.Get(key)")
	if hasMut {
		g.P("	// Issue a single prefix range read for all atomic fields concurrently with the primary record read")
		g.P("	fieldPrefixRange, err := fdb.PrefixRange(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgs, "}))")
		g.P("	if err != nil {")
		g.P("		return nil, err")
		g.P("	}")
		g.P("	fieldFuture := fdblayer.GetRange(tr, fieldPrefixRange, fdb.RangeOptions{})")
	}
	g.P("	value := valueFuture.MustGet()")
	g.P("	if value == nil {")
	g.P(`		return nil, fmt.Errorf("`, lowerName, ` %w", fdblayer.ErrNotFound)`)
	g.P("	}")
	g.P("	entity := &", msg.Name, "{}")
	g.P("	if err := proto.Unmarshal(value, entity); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P()
	if ir.MsgHasVersionstampPK(msg) {
		for _, f := range msg.PrimaryKeyFields {
			if f.IsVersionstamp {
				if len(msg.PrimaryKeyFields) == 1 {
					g.P("	entity.", f.GoName, " = fdblayer.BytesToVersionstamp(pk).Bytes()")
				} else {
					g.P("	entity.", f.GoName, " = fdblayer.BytesToVersionstamp(pk.", f.GoName, ").Bytes()")
				}
			}
		}
	}
	if hasMut {
		generateUnmarshalAtomicFields(g, msg, "fieldFuture.GetSliceOrPanic()", "\t")
	}
	g.P("	return entity, nil")
	g.P("}")
	g.P()
}

func generateSet(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	hasVS := ir.MsgHasVersionstampPK(msg)
	hasMut := ir.HasMutationFields(msg)
	hasIdx := len(msg.SecondaryIndexes) > 0
	pkArgsEntity := pkTupleArgsFromEntity(msg, "entity")
	typeIDConst := typeIDConstName(msg)

	var vsFieldName string
	for _, f := range msg.PrimaryKeyFields {
		if f.IsVersionstamp {
			vsFieldName = f.GoName
			break
		}
	}

	g.P("// Set updates an existing ", msg.Name, " entity in the database (or inserts it if not present).")
	g.P("func (r *", lowerName, "Repository) Set(ctx context.Context, tx *fdblayer.Tx, entity *", msg.Name, ") error {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return err")
	g.P("	}")
	if hasVS {
		g.P("	if len(entity.", vsFieldName, ") != 0 && len(entity.", vsFieldName, ") != 12 {")
		g.P(`		return fmt.Errorf("invalid versionstamp length %d for `, vsFieldName, `: must be 0 or 12 bytes", len(entity.`, vsFieldName, `))`)
		g.P("	}")
		g.P("	hasOldVS := len(entity.", vsFieldName, ") == 12")
		g.P("	if !hasOldVS {")
		g.P("		return r.Create(ctx, tx, entity)")
		g.P("	}")
		g.P()
	}
	g.P("	tr := tx.Transaction()")
	g.P("	dir := tx.Dir()")
	g.P()

	if hasIdx {
		g.P("	var old *", msg.Name)
		g.P()
	}

	g.P("	key := dir.Pack(", dataKeyTupleExprFromEntity(msg, "entity"), ")")
	if hasIdx {
		g.P()
		g.P("	if oldValue := tr.Get(key).MustGet(); oldValue != nil {")
		g.P("		var unmarshaled ", msg.Name)
		g.P("		if err := proto.Unmarshal(oldValue, &unmarshaled); err != nil {")
		g.P(`			return fmt.Errorf("failed to unmarshal existing `, lowerName, `: %w", err)`)
		g.P("		}")
		for _, f := range msg.PrimaryKeyFields {
			g.P("		unmarshaled.", f.GoName, " = entity.", f.GoName)
		}
		g.P("		old = &unmarshaled")
		g.P("	}")
	} else if hasMut {
		g.P("	isNewRecord := tr.Get(key).MustGet() == nil")
	}
	g.P()

	for _, idx := range msg.SecondaryIndexes {
		emitIndexSet(g, msg, idx, "\t")
		g.P()
	}

	generateMarshalWithZeroedAtomics(g, msg)

	g.P("	tr.Set(key, value)")
	g.P()

	if hasMut {
		newCond := "isNewRecord"
		if hasIdx {
			newCond = "old == nil"
		}
		g.P("	// Only initialize FieldNamespace when Set inserts a brand-new record (upsert).")
		g.P("	if ", newCond, " {")
		for _, f := range msg.Fields {
			if f.Mutation != ir.MutationNone {
				if f.Mutation == ir.MutationMax && !f.IsUnsigned {
					g.P("		{")
				} else {
					g.P("		if entity.", f.GoName, " != 0 {")
				}
				g.P("			fieldKey := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgsEntity, ", ", f.Number, "})")
				g.P("			buf := make([]byte, 8)")
				g.P("			binary.LittleEndian.PutUint64(buf, ", encodeAtomicUint64Expr("entity."+f.GoName, f), ")")
				g.P("			tr.", MutationVerb(f.Mutation), "(fieldKey, buf)")
				g.P("		}")
			}
		}
		g.P("	}")
		g.P()
	}

	g.P("	return nil")
	g.P("}")
	g.P()
}

func generateDelete(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	pkType := pkTypeExpr(msg)
	pkArgs := pkTupleArgsFromParam(msg)
	typeIDConst := typeIDConstName(msg)

	g.P("// Delete removes a ", msg.Name, " entity from the database.")
	g.P("func (r *", lowerName, "Repository) Delete(ctx context.Context, tx *fdblayer.Tx, pk ", pkType, ") error {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return err")
	g.P("	}")
	g.P("	tr := tx.Transaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	g.P("	key := dir.Pack(", dataKeyTupleExprFromParam(msg, "pk"), ")")
	if len(msg.SecondaryIndexes) > 0 {
		g.P("	value := tr.Get(key).MustGet()")
		g.P("	if value != nil {")
		g.P("		entity := &", msg.Name, "{}")
		g.P("		if err := proto.Unmarshal(value, entity); err != nil {")
		g.P(`			return fmt.Errorf("failed to unmarshal existing `, lowerName, `: %w", err)`)
		g.P("		}")
		for _, f := range msg.PrimaryKeyFields {
			if len(msg.PrimaryKeyFields) == 1 {
				g.P("		entity.", f.GoName, " = pk")
			} else {
				g.P("		entity.", f.GoName, " = pk.", f.GoName)
			}
		}
		for _, idx := range msg.SecondaryIndexes {
			emitIndexDelete(g, msg, idx, "entity", "\t\t")
		}
		g.P("	}")
	}
	g.P("	tr.Clear(key)")
	g.P()
	if ir.HasMutationFields(msg) {
		g.P("	// Clear atomic fields")
		for _, f := range msg.Fields {
			if f.Mutation != ir.MutationNone {
				g.P("	{")
				g.P("		fieldKey := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgs, ", ", f.Number, "})")
				g.P("		tr.Clear(fieldKey)")
				g.P("	}")
			}
		}
	}
	emitCascadeDeleteChildren(g, msg, "\t")
	g.P("	return nil")
	g.P("}")
	g.P()
}

func generateBatchGet(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	pkType := pkTypeExpr(msg)
	hasMut := ir.HasMutationFields(msg)
	hasVS := ir.MsgHasVersionstampPK(msg)
	pkArgsFromPK := pkTupleArgsFromParam(msg)
	typeIDConst := typeIDConstName(msg)

	g.P("// BatchGet retrieves multiple ", msg.Name, " entities by their primary keys.")
	g.P("func (r *", lowerName, "Repository) BatchGet(ctx context.Context, tx fdblayer.ReadTx, pks []", pkType, ") ([]*", msg.Name, ", error) {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	if len(pks) == 0 {")
	g.P("		return nil, nil")
	g.P("	}")
	g.P("	tr := tx.ReadTransaction()")
	g.P("	dir := tx.Dir()")
	g.P("	futures := make([]fdb.FutureByteSlice, len(pks))")
	if hasMut {
		g.P("	fieldFutures := make([]fdblayer.RangeFuture, len(pks))")
	}
	g.P("	for i, pk := range pks {")
	g.P("		if err := ctx.Err(); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		key := dir.Pack(", dataKeyTupleExprFromParam(msg, "pk"), ")")
	g.P("		futures[i] = tr.Get(key)")
	if hasMut {
		g.P("		fieldRange, err := fdb.PrefixRange(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgsFromPK, "}))")
		g.P("		if err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		fieldFutures[i] = fdblayer.GetRange(tr, fieldRange, fdb.RangeOptions{})")
	}
	g.P("	}")
	g.P("	entities := make([]*", msg.Name, ", 0, len(pks))")
	g.P("	for i, future := range futures {")
	g.P("		if err := ctx.Err(); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		value := future.MustGet()")
	g.P("		if value == nil {")
	g.P("			continue")
	g.P("		}")
	g.P("		entity := &", msg.Name, "{}")
	g.P("		if err := proto.Unmarshal(value, entity); err != nil {")
	g.P(`			return nil, fmt.Errorf("failed to unmarshal entity at index %d: %w", i, err)`)
	g.P("		}")
	if hasVS {
		for _, f := range msg.PrimaryKeyFields {
			if f.IsVersionstamp {
				if len(msg.PrimaryKeyFields) == 1 {
					g.P("		entity.", f.GoName, " = fdblayer.BytesToVersionstamp(pks[i]).Bytes()")
				} else {
					g.P("		entity.", f.GoName, " = fdblayer.BytesToVersionstamp(pks[i].", f.GoName, ").Bytes()")
				}
			}
		}
	}
	if hasMut {
		generateUnmarshalAtomicFields(g, msg, "fieldFutures[i].GetSliceOrPanic()", "\t\t")
	}
	g.P("		entities = append(entities, entity)")
	g.P("	}")
	g.P("	return entities, nil")
	g.P("}")
	g.P()
}

func generateList(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	hasMut := ir.HasMutationFields(msg)
	typeIDConst := typeIDConstName(msg)
	isInterleaved := msg.Interleave != nil
	hasChildren := len(msg.Children) > 0

	g.P("// List retrieves a list of ", msg.Name, " entities starting from the given key.")
	g.P("func (r *", lowerName, "Repository) List(ctx context.Context, tx fdblayer.ReadTx, opts fdblayer.PaginationOptions) (*fdblayer.PaginatedResult[*", msg.Name, "], error) {")
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
	g.P()
	if isInterleaved {
		parentLen := len(msg.Interleave.ParentPKFields)
		g.P("	var prefixTpl tuple.Tuple")
		g.P("	if len(opts.Prefix) >= ", parentLen, " {")
		g.P("		prefixTpl = make(tuple.Tuple, 3+len(opts.Prefix))")
		g.P("		prefixTpl[0] = int64(", msg.Interleave.ParentTypeID, ")")
		g.P("		prefixTpl[1] = fdblayer.DataNamespace")
		g.P("		for j := 0; j < ", parentLen, "; j++ {")
		g.P("			prefixTpl[2+j] = fdblayer.NormalizeTupleElement(opts.Prefix[j])")
		g.P("		}")
		g.P("		prefixTpl[2+", parentLen, "] = ", typeIDConst)
		g.P("		for j := ", parentLen, "; j < len(opts.Prefix); j++ {")
		g.P("			prefixTpl[3+j] = fdblayer.NormalizeTupleElement(opts.Prefix[j])")
		g.P("		}")
		g.P("	} else {")
		g.P("		prefixTpl = make(tuple.Tuple, 2+len(opts.Prefix))")
		g.P("		prefixTpl[0] = int64(", msg.Interleave.ParentTypeID, ")")
		g.P("		prefixTpl[1] = fdblayer.DataNamespace")
		g.P("		for j, el := range opts.Prefix {")
		g.P("			prefixTpl[2+j] = fdblayer.NormalizeTupleElement(el)")
		g.P("		}")
		g.P("	}")
	} else {
		g.P("	// Compute data prefix range (scoped to opts.Prefix when provided).")
		g.P("	prefixTpl := make(tuple.Tuple, 2+len(opts.Prefix))")
		g.P("	prefixTpl[0] = ", typeIDConst)
		g.P("	prefixTpl[1] = fdblayer.DataNamespace")
		g.P("	for j, el := range opts.Prefix {")
		g.P("		prefixTpl[2+j] = fdblayer.NormalizeTupleElement(el)")
		g.P("	}")
		if ir.MsgHasVersionstampPK(msg) {
			for j, f := range msg.PrimaryKeyFields {
				if f.IsVersionstamp {
					g.P("	if len(opts.Prefix) > ", j, " {")
					g.P("		if rawVS, ok := opts.Prefix[", j, "].([]byte); ok {")
					g.P("			prefixTpl[2+", j, "] = fdblayer.BytesToVersionstamp(rawVS)")
					g.P("		}")
					g.P("	}")
				}
			}
		}
	}
	g.P("	dataPrefix := dir.Pack(prefixTpl)")
	g.P("	dataPrefixRange, err := fdb.PrefixRange(dataPrefix)")
	g.P("	if err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P()
	if isInterleaved {
		parentLen := len(msg.Interleave.ParentPKFields)
		g.P("	var beginTpl tuple.Tuple")
		g.P("	if len(cursorBegin) >= ", parentLen, " {")
		g.P("		beginTpl = make(tuple.Tuple, 3+len(cursorBegin))")
		g.P("		beginTpl[0] = int64(", msg.Interleave.ParentTypeID, ")")
		g.P("		beginTpl[1] = fdblayer.DataNamespace")
		g.P("		for j := 0; j < ", parentLen, "; j++ {")
		g.P("			beginTpl[2+j] = fdblayer.NormalizeTupleElement(cursorBegin[j])")
		g.P("		}")
		g.P("		beginTpl[2+", parentLen, "] = ", typeIDConst)
		g.P("		for j := ", parentLen, "; j < len(cursorBegin); j++ {")
		g.P("			beginTpl[3+j] = fdblayer.NormalizeTupleElement(cursorBegin[j])")
		g.P("		}")
		g.P("	} else {")
		g.P("		beginTpl = make(tuple.Tuple, 2+len(cursorBegin))")
		g.P("		beginTpl[0] = int64(", msg.Interleave.ParentTypeID, ")")
		g.P("		beginTpl[1] = fdblayer.DataNamespace")
		g.P("		for j, el := range cursorBegin {")
		g.P("			beginTpl[2+j] = fdblayer.NormalizeTupleElement(el)")
		g.P("		}")
		g.P("	}")
	} else {
		g.P("	beginTpl := make(tuple.Tuple, 2+len(cursorBegin))")
		g.P("	beginTpl[0] = ", typeIDConst)
		g.P("	beginTpl[1] = fdblayer.DataNamespace")
		g.P("	for j, el := range cursorBegin {")
		g.P("		beginTpl[2+j] = fdblayer.NormalizeTupleElement(el)")
		g.P("	}")
		if ir.MsgHasVersionstampPK(msg) {
			for j, f := range msg.PrimaryKeyFields {
				if f.IsVersionstamp {
					g.P("	if len(cursorBegin) > ", j, " {")
					g.P("		if rawVS, ok := cursorBegin[", j, "].([]byte); ok {")
					g.P("			beginTpl[2+", j, "] = fdblayer.BytesToVersionstamp(rawVS)")
					g.P("		}")
					g.P("	}")
				}
			}
		}
	}
	g.P("	begin := dir.Pack(beginTpl)")
	g.P()
	g.P("	rangeBegin := dataPrefixRange.Begin")
	g.P("	rangeEnd := dataPrefixRange.End")
	g.P("	if len(cursorBegin) > 0 {")
	g.P("		if opts.Reverse {")
	g.P("			beginRange, err := fdb.PrefixRange(begin)")
	g.P("			if err != nil {")
	g.P("				return nil, err")
	g.P("			}")
	g.P("			if string(beginRange.End.FDBKey()) < string(rangeEnd.FDBKey()) {")
	g.P("				rangeEnd = beginRange.End")
	g.P("			}")
	g.P("		} else if string(begin) > string(rangeBegin.FDBKey()) {")
	g.P("			rangeBegin = begin")
	g.P("		}")
	g.P("	}")
	g.P("	if string(rangeBegin.FDBKey()) >= string(rangeEnd.FDBKey()) {")
	g.P("		return result, nil")
	g.P("	}")
	g.P()
	g.P("	rangeOpts := fdb.RangeOptions{")
	g.P("		Reverse: opts.Reverse,")
	g.P("	}")
	if !isInterleaved && !hasChildren {
		g.P("	if opts.Limit > 0 {")
		g.P("		rangeOpts.Limit = opts.Limit + 1")
		g.P("	}")
	}
	g.P()
	g.P("	kvs := fdblayer.GetRange(tr, fdb.KeyRange{")
	g.P("		Begin: rangeBegin,")
	g.P("		End:   rangeEnd,")
	g.P("	}, rangeOpts).GetSliceOrPanic()")
	g.P()
	if isInterleaved {
		parentLen := len(msg.Interleave.ParentPKFields)
		expectedLen := 3 + len(msg.PrimaryKeyFields)
		g.P("	for _, kv := range kvs {")
		g.P("		if err := ctx.Err(); err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		tpl, err := dir.Unpack(kv.Key)")
		g.P("		if err != nil || len(tpl) != ", expectedLen, " {")
		g.P("			continue")
		g.P("		}")
		g.P("		if childID, ok := tpl[2+", parentLen, "].(int64); !ok || childID != ", typeIDConst, " {")
		g.P("			continue")
		g.P("		}")
		g.P("		if opts.Limit > 0 && len(result.Items) == opts.Limit {")
		g.P("			nextPK := make(tuple.Tuple, ", len(msg.PrimaryKeyFields), ")")
		g.P("			copy(nextPK[:", parentLen, "], tpl[2:2+", parentLen, "])")
		g.P("			copy(nextPK[", parentLen, ":], tpl[3+", parentLen, ":])")
		g.P("			result.HasMore = true")
		g.P("			result.NextKey = nextPK")
		g.P("			result.NextPageToken = result.NextKey.Pack()")
		g.P("			break")
		g.P("		}")
		g.P("		entity := &", msg.Name, "{}")
		g.P("		if err := proto.Unmarshal(kv.Value, entity); err != nil {")
		g.P(`			return nil, fmt.Errorf("failed to unmarshal entity: %w", err)`)
		g.P("		}")
		g.P("		result.Items = append(result.Items, entity)")
		g.P("	}")
	} else if hasChildren {
		expectedLen := 2 + len(msg.PrimaryKeyFields)
		g.P("	for _, kv := range kvs {")
		g.P("		if err := ctx.Err(); err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		tpl, err := dir.Unpack(kv.Key)")
		g.P("		if err != nil || len(tpl) != ", expectedLen, " {")
		g.P("			continue")
		g.P("		}")
		g.P("		if opts.Limit > 0 && len(result.Items) == opts.Limit {")
		g.P("			result.HasMore = true")
		g.P("			result.NextKey = tpl[2:]")
		g.P("			result.NextPageToken = result.NextKey.Pack()")
		g.P("			break")
		g.P("		}")
		g.P("		entity := &", msg.Name, "{}")
		g.P("		if err := proto.Unmarshal(kv.Value, entity); err != nil {")
		g.P(`			return nil, fmt.Errorf("failed to unmarshal entity: %w", err)`)
		g.P("		}")
		g.P("		result.Items = append(result.Items, entity)")
		g.P("	}")
	} else if ir.MsgHasVersionstampPK(msg) {
		g.P("	for i, kv := range kvs {")
		g.P("		if err := ctx.Err(); err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		if opts.Limit > 0 && i == opts.Limit {")
		g.P("			tpl, err := dir.Unpack(kv.Key)")
		g.P("			if err != nil {")
		g.P(`				return nil, fmt.Errorf("failed to unpack next key: %w", err)`)
		g.P("			}")
		g.P("			result.HasMore = true")
		g.P("			result.NextKey = tpl[2:]")
		g.P("			result.NextPageToken = result.NextKey.Pack()")
		g.P("			break")
		g.P("		}")
		g.P()
		g.P("		entity := &", msg.Name, "{}")
		g.P("		err = proto.Unmarshal(kv.Value, entity)")
		g.P("		if err != nil {")
		g.P(`			return nil, fmt.Errorf("failed to unmarshal entity: %w", err)`)
		g.P("		}")
		g.P("		tpl, unpackErr := dir.Unpack(kv.Key)")
		g.P("		if unpackErr != nil {")
		g.P(`			return nil, fmt.Errorf("failed to unpack key: %w", unpackErr)`)
		g.P("		}")
		for i, f := range msg.PrimaryKeyFields {
			if f.IsVersionstamp {
				g.P("		if len(tpl) > ", i, "+2 {")
				g.P("			if vs, ok := tpl[", i, "+2].(tuple.Versionstamp); ok {")
				g.P("				entity.", f.GoName, " = vs.Bytes()")
				g.P("			}")
				g.P("		}")
			}
		}
		g.P("		result.Items = append(result.Items, entity)")
		g.P("	}")
	} else {
		g.P("	for i, kv := range kvs {")
		g.P("		if err := ctx.Err(); err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		if opts.Limit > 0 && i == opts.Limit {")
		g.P("			tpl, err := dir.Unpack(kv.Key)")
		g.P("			if err != nil {")
		g.P(`				return nil, fmt.Errorf("failed to unpack next key: %w", err)`)
		g.P("			}")
		g.P("			result.HasMore = true")
		g.P("			result.NextKey = tpl[2:]")
		g.P("			result.NextPageToken = result.NextKey.Pack()")
		g.P("			break")
		g.P("		}")
		g.P()
		g.P("		entity := &", msg.Name, "{}")
		g.P("		err = proto.Unmarshal(kv.Value, entity)")
		g.P("		if err != nil {")
		g.P(`			return nil, fmt.Errorf("failed to unmarshal entity: %w", err)`)
		g.P("		}")
		g.P("		result.Items = append(result.Items, entity)")
		g.P("	}")
	}
	g.P()
	if hasMut {
		emitPopulateListAtomicFields(g, msg)
	}
	g.P("	return result, nil")
	g.P("}")
	g.P()
}

func emitPopulateListAtomicFields(g Printer, msg ir.MessageSpec) {
	typeIDConst := typeIDConstName(msg)
	firstPKArgs := pkTupleArgsFromEntity(msg, "first")
	lastPKArgs := pkTupleArgsFromEntity(msg, "last")
	entityPKArgs := pkTupleArgsFromEntity(msg, "entity")

	g.P("	// Populate atomic fields for all returned items in a single range read")
	g.P("	if len(result.Items) > 0 {")
	g.P("		first := result.Items[0]")
	g.P("		last := result.Items[len(result.Items)-1]")
	g.P("		if opts.Reverse {")
	g.P("			first, last = last, first")
	g.P("		}")
	g.P("		fieldBegin := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", firstPKArgs, "})")
	g.P("		fieldEndPrefix, err := fdb.PrefixRange(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", lastPKArgs, "}))")
	g.P("		if err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		itemByPK := make(map[string]*", msg.Name, ", len(result.Items))")
	g.P("		for _, entity := range result.Items {")
	for _, f := range msg.Fields {
		if f.Mutation != ir.MutationNone {
			g.P("			entity.", f.GoName, " = 0")
		}
	}
	g.P("			pkKey := tuple.Tuple{", entityPKArgs, "}.String()")
	g.P("			itemByPK[pkKey] = entity")
	g.P("		}")
	g.P("		fieldKVs := fdblayer.GetRange(tr, fdb.KeyRange{Begin: fieldBegin, End: fieldEndPrefix.End}, fdb.RangeOptions{}).GetSliceOrPanic()")
	g.P("		pkLen := ", len(msg.PrimaryKeyFields))
	g.P("		for _, fkv := range fieldKVs {")
	g.P("			if len(fkv.Value) < 8 {")
	g.P("				continue")
	g.P("			}")
	g.P("			ftpl, err := dir.Unpack(fkv.Key)")
	g.P("			if err != nil || len(ftpl) < 3+pkLen {")
	g.P("				continue")
	g.P("			}")
	g.P("			entity, ok := itemByPK[ftpl[2:2+pkLen].String()]")
	g.P("			if !ok {")
	g.P("				continue")
	g.P("			}")
	g.P("			fieldNum, ok := ftpl[2+pkLen].(int64)")
	g.P("			if !ok {")
	g.P("				continue")
	g.P("			}")
	g.P("			switch fieldNum {")
	for _, f := range msg.Fields {
		if f.Mutation != ir.MutationNone {
			g.P("			case ", f.Number, ":")
			g.P("				entity.", f.GoName, " = ", decodeAtomicGoExpr("fkv.Value", f))
		}
	}
	g.P("			}")
	g.P("		}")
	g.P("	}")
	g.P()
}

func generateListByPKPrefix(g Printer, msg ir.MessageSpec, prefixFields []ir.FieldSpec) {
	lowerName := strings.ToLower(msg.Name)
	joined := JoinFieldNames(prefixFields)
	var tupleArgs []string
	for _, f := range prefixFields {
		tupleArgs = append(tupleArgs, PackField(f.GoName, f))
	}

	g.P("// ListBy", joined, " retrieves ", msg.Name, " entities matching the leading primary key prefix ", joined, ".")
	g.P("func (r *", lowerName, "Repository) ListBy", joined, "(ctx context.Context, tx fdblayer.ReadTx, ", fieldParamDecls(prefixFields), ", opts fdblayer.PaginationOptions) (*fdblayer.PaginatedResult[*", msg.Name, "], error) {")
	g.P("	opts.Prefix = tuple.Tuple{", strings.Join(tupleArgs, ", "), "}")
	g.P("	return r.List(ctx, tx, opts)")
	g.P("}")
	g.P()
}

func generateScanByPKRange(g Printer, msg ir.MessageSpec, rs ir.IndexRangeScanSpec) {
	lowerName := strings.ToLower(msg.Name)
	allFields := rs.AllFields()
	joined := JoinFieldNames(allFields)
	typeIDConst := typeIDConstName(msg)
	rangeParam := rs.RangeField.GoName + "Range"
	isInterleaved := msg.Interleave != nil
	hasChildren := len(msg.Children) > 0
	hasMut := ir.HasMutationFields(msg)

	var baseTupleParts []string
	if isInterleaved && len(rs.EqualityFields) >= len(msg.Interleave.ParentPKFields) {
		parentLen := len(msg.Interleave.ParentPKFields)
		baseTupleParts = append(baseTupleParts, fmt.Sprintf("int64(%d)", msg.Interleave.ParentTypeID), "fdblayer.DataNamespace")
		for _, f := range rs.EqualityFields[:parentLen] {
			baseTupleParts = append(baseTupleParts, PackField(f.GoName, f))
		}
		baseTupleParts = append(baseTupleParts, typeIDConst)
		for _, f := range rs.EqualityFields[parentLen:] {
			baseTupleParts = append(baseTupleParts, PackField(f.GoName, f))
		}
	} else if isInterleaved {
		baseTupleParts = append(baseTupleParts, fmt.Sprintf("int64(%d)", msg.Interleave.ParentTypeID), "fdblayer.DataNamespace")
		for _, f := range rs.EqualityFields {
			baseTupleParts = append(baseTupleParts, PackField(f.GoName, f))
		}
	} else {
		baseTupleParts = append(baseTupleParts, typeIDConst, "fdblayer.DataNamespace")
		for _, f := range rs.EqualityFields {
			baseTupleParts = append(baseTupleParts, PackField(f.GoName, f))
		}
	}

	g.P("// ScanBy", joined, " performs a bounded range scan on the primary key fields ", joined, ".")
	g.P("func (r *", lowerName, "Repository) ScanBy", joined, "(ctx context.Context, tx fdblayer.ReadTx, ", rangeScanParamDecls(rs), ", opts fdblayer.PaginationOptions) (*fdblayer.PaginatedResult[*", msg.Name, "], error) {")
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
	scopingAllInterleaved := isInterleaved && len(rs.EqualityFields) < len(msg.Interleave.ParentPKFields)
	if !scopingAllInterleaved && !hasChildren {
		g.P("	if opts.Limit > 0 {")
		g.P("		rangeOpts.Limit = opts.Limit + 1")
		g.P("	}")
	}
	g.P("	kvs := fdblayer.GetRange(tr, kr, rangeOpts).GetSliceOrPanic()")
	g.P("	cursorStart := len(baseTpl)")
	if scopingAllInterleaved || hasChildren {
		expectedLen := 2 + len(msg.PrimaryKeyFields)
		if isInterleaved {
			expectedLen = 3 + len(msg.PrimaryKeyFields)
		}
		g.P("	for _, kv := range kvs {")
		g.P("		if err := ctx.Err(); err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		tpl, err := dir.Unpack(kv.Key)")
		g.P("		if err != nil || len(tpl) != ", expectedLen, " {")
		g.P("			continue")
		g.P("		}")
		if isInterleaved {
			parentLen := len(msg.Interleave.ParentPKFields)
			g.P("		if childID, ok := tpl[2+", parentLen, "].(int64); !ok || childID != ", typeIDConst, " {")
			g.P("			continue")
			g.P("		}")
		}
		g.P("		if opts.Limit > 0 && len(result.Items) == opts.Limit {")
		g.P("			result.HasMore = true")
		g.P("			result.NextKey = tpl[cursorStart:]")
		g.P("			result.NextPageToken = result.NextKey.Pack()")
		g.P("			break")
		g.P("		}")
		g.P("		entity := &", msg.Name, "{}")
		g.P("		if err := proto.Unmarshal(kv.Value, entity); err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		result.Items = append(result.Items, entity)")
		g.P("	}")
	} else {
		g.P("	if opts.Limit > 0 && len(kvs) > opts.Limit {")
		g.P("		nextTpl, err := dir.Unpack(kvs[opts.Limit].Key)")
		g.P("		if err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		if len(nextTpl) > cursorStart {")
		g.P("			result.HasMore = true")
		g.P("			result.NextKey = nextTpl[cursorStart:]")
		g.P("			result.NextPageToken = result.NextKey.Pack()")
		g.P("		}")
		g.P("		kvs = kvs[:opts.Limit]")
		g.P("	}")
		g.P("	for _, kv := range kvs {")
		g.P("		if err := ctx.Err(); err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		entity := &", msg.Name, "{}")
		g.P("		if err := proto.Unmarshal(kv.Value, entity); err != nil {")
		g.P("			return nil, err")
		g.P("		}")
		g.P("		result.Items = append(result.Items, entity)")
		g.P("	}")
	}
	if hasMut {
		emitPopulateListAtomicFields(g, msg)
	}
	g.P("	return result, nil")
	g.P("}")
	g.P()
}
