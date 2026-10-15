package golang

import (
	"fmt"
	"strings"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

func generateMutations(g Printer, msg ir.MessageSpec) {
	generateInsertMutation(g, msg)
	generateUpsertMutation(g, msg)
	if canEmitReplace(msg) {
		generateReplaceMutation(g, msg)
	}
	generateDeleteMutation(g, msg)
}

func generateInsertMutation(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	hasVS := ir.MsgHasVersionstampPK(msg)
	hasMut := ir.HasMutationFields(msg)
	typeIDConst := typeIDConstName(msg)

	g.P("// Insert returns a pipelined Mutation that creates a new ", msg.Name, " entity, failing with ErrAlreadyExists if the primary key or a unique index already exists.")
	g.P("func (r *", lowerName, "Repository) Insert(entity *", msg.Name, ") fdblayer.Mutation {")
	if hasVS {
		g.P("	return fdblayer.MutationFunc(func(ctx context.Context, tx *fdblayer.Tx) (func(context.Context) error, error) {")
		g.P("		return func(ctx context.Context) error {")
		g.P("			return r.Create(ctx, tx, entity)")
		g.P("		}, nil")
		g.P("	})")
		g.P("}")
		g.P()
		return
	}

	g.P("	return fdblayer.MutationFunc(func(ctx context.Context, tx *fdblayer.Tx) (func(context.Context) error, error) {")
	g.P("		if err := ctx.Err(); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		tr := tx.Transaction()")
	g.P("		dir := tx.Dir()")
	g.P("		key := dir.Pack(", dataKeyTupleExprFromEntity(msg, "entity"), ")")
	g.P("		if !tx.ClaimKey(key, \"\") {")
	g.P(`			return nil, fmt.Errorf("`, lowerName, ` %w", fdblayer.ErrAlreadyExists)`)
	g.P("		}")
	g.P("		existingFuture := tr.Get(key)")

	for _, idx := range msg.SecondaryIndexes {
		if idx.Aggregate == ir.AggregateNone && idx.IsUnique {
			joined := JoinFieldNames(idx.Fields)
			fVar := fmt.Sprintf("uniqFuture_%d", idx.IndexID)
			kVar := fmt.Sprintf("uniqKey_%d", idx.IndexID)
			g.P("		var ", fVar, " fdb.FutureByteSlice")
			g.P("		var ", kVar, " fdb.Key")
			if idx.IsSparse {
				g.P("		if ", SparseIndexPresentExpr(idx.Fields, "entity"), " {")
				g.P("			", kVar, " = dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "entity"), "})")
				g.P("			if !tx.ClaimKey(", kVar, ", string(key)) {")
				g.P(`				return nil, fmt.Errorf("`, lowerName, ` unique index `, joined, `: %w", fdblayer.ErrUniqueConstraintViolation)`)
				g.P("			}")
				g.P("			", fVar, " = tr.Get(", kVar, ")")
				g.P("		}")
			} else {
				g.P("		", kVar, " = dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "entity"), "})")
				g.P("		if !tx.ClaimKey(", kVar, ", string(key)) {")
				g.P(`			return nil, fmt.Errorf("`, lowerName, ` unique index `, joined, `: %w", fdblayer.ErrUniqueConstraintViolation)`)
				g.P("		}")
				g.P("		", fVar, " = tr.Get(", kVar, ")")
			}
		}
	}

	g.P("		return func(ctx context.Context) error {")
	g.P("			if err := ctx.Err(); err != nil {")
	g.P("				return err")
	g.P("			}")
	g.P("			if existingFuture.MustGet() != nil {")
	g.P(`				return fmt.Errorf("`, lowerName, ` %w", fdblayer.ErrAlreadyExists)`)
	g.P("			}")

	pkArgs := pkTupleArgsFromEntity(msg, "entity")
	for _, idx := range msg.SecondaryIndexes {
		if idx.Aggregate != ir.AggregateNone {
			emitAggregateIndexCreate(g, msg, idx, "\t\t\t")
			continue
		}
		if idx.IsUnique {
			joined := JoinFieldNames(idx.Fields)
			fVar := fmt.Sprintf("uniqFuture_%d", idx.IndexID)
			kVar := fmt.Sprintf("uniqKey_%d", idx.IndexID)
			g.P("			if ", fVar, " != nil {")
			g.P("				if ", fVar, ".MustGet() != nil {")
			g.P(`					return fmt.Errorf("`, lowerName, ` unique index `, joined, `: %w", fdblayer.ErrUniqueConstraintViolation)`)
			g.P("				}")
			emitStoringValueExpr(g, msg, idx, "entity", "idxVal", "\t\t\t\t")
			g.P("				tr.Set(", kVar, ", idxVal)")
			g.P("			}")
			continue
		}
		emitIndexCreate(g, msg, idx, false, "\t\t\t")
	}

	generateMarshalWithZeroedAtomics(g, msg)
	g.P("			tr.Set(key, value)")
	if hasMut {
		for _, f := range msg.Fields {
			if f.Mutation != ir.MutationNone {
				if f.Mutation == ir.MutationMax && !f.IsUnsigned {
					g.P("			{")
				} else {
					g.P("			if entity.", f.GoName, " != 0 {")
				}
				g.P("				fieldKey := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgs, ", ", f.Number, "})")
				g.P("				buf := make([]byte, 8)")
				g.P("				binary.LittleEndian.PutUint64(buf, ", encodeAtomicUint64Expr("entity."+f.GoName, f), ")")
				g.P("				tr.", MutationVerb(f.Mutation), "(fieldKey, buf)")
				g.P("			}")
			}
		}
	}
	g.P("			return nil")
	g.P("		}, nil")
	g.P("	})")
	g.P("}")
	g.P()
}

func generateUpsertMutation(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	hasVS := ir.MsgHasVersionstampPK(msg)
	hasMut := ir.HasMutationFields(msg)
	hasIdx := len(msg.SecondaryIndexes) > 0
	typeIDConst := typeIDConstName(msg)

	g.P("// Upsert returns a pipelined Mutation that inserts or updates a ", msg.Name, " entity, pre-reading the old record in parallel when needed.")
	g.P("func (r *", lowerName, "Repository) Upsert(entity *", msg.Name, ") fdblayer.Mutation {")
	if hasVS {
		g.P("	return fdblayer.MutationFunc(func(ctx context.Context, tx *fdblayer.Tx) (func(context.Context) error, error) {")
		g.P("		return func(ctx context.Context) error {")
		g.P("			return r.Set(ctx, tx, entity)")
		g.P("		}, nil")
		g.P("	})")
		g.P("}")
		g.P()
		return
	}

	g.P("	return fdblayer.MutationFunc(func(ctx context.Context, tx *fdblayer.Tx) (func(context.Context) error, error) {")
	g.P("		if err := ctx.Err(); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		tr := tx.Transaction()")
	g.P("		dir := tx.Dir()")
	g.P("		key := dir.Pack(", dataKeyTupleExprFromEntity(msg, "entity"), ")")
	if hasIdx || hasMut {
		g.P("		oldFuture := tr.Get(key)")
	}
	for _, idx := range msg.SecondaryIndexes {
		if idx.Aggregate == ir.AggregateNone && idx.IsUnique {
			joined := JoinFieldNames(idx.Fields)
			fVar := fmt.Sprintf("uniqFuture_%d", idx.IndexID)
			kVar := fmt.Sprintf("uniqKey_%d", idx.IndexID)
			g.P("		var ", fVar, " fdb.FutureByteSlice")
			g.P("		var ", kVar, " fdb.Key")
			if idx.IsSparse {
				g.P("		if ", SparseIndexPresentExpr(idx.Fields, "entity"), " {")
				g.P("			", kVar, " = dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "entity"), "})")
				g.P("			if !tx.ClaimKey(", kVar, ", string(key)) {")
				g.P(`				return nil, fmt.Errorf("`, lowerName, ` unique index `, joined, `: %w", fdblayer.ErrUniqueConstraintViolation)`)
				g.P("			}")
				g.P("			", fVar, " = tr.Get(", kVar, ")")
				g.P("		}")
			} else {
				g.P("		", kVar, " = dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "entity"), "})")
				g.P("		if !tx.ClaimKey(", kVar, ", string(key)) {")
				g.P(`			return nil, fmt.Errorf("`, lowerName, ` unique index `, joined, `: %w", fdblayer.ErrUniqueConstraintViolation)`)
				g.P("		}")
				g.P("		", fVar, " = tr.Get(", kVar, ")")
			}
		}
	}

	g.P("		return func(ctx context.Context) error {")
	g.P("			if err := ctx.Err(); err != nil {")
	g.P("				return err")
	g.P("			}")
	if hasIdx {
		g.P("			var old *", msg.Name)
		g.P("			if oldValue := oldFuture.MustGet(); oldValue != nil {")
		g.P("				var unmarshaled ", msg.Name)
		g.P("				if err := proto.Unmarshal(oldValue, &unmarshaled); err != nil {")
		g.P(`					return fmt.Errorf("failed to unmarshal existing `, lowerName, `: %w", err)`)
		g.P("				}")
		for _, f := range msg.PrimaryKeyFields {
			g.P("				unmarshaled.", f.GoName, " = entity.", f.GoName)
		}
		g.P("				old = &unmarshaled")
		g.P("			}")
	} else if hasMut {
		g.P("			isNewRecord := oldFuture.MustGet() == nil")
	}

	for _, idx := range msg.SecondaryIndexes {
		if idx.Aggregate == ir.AggregateNone && idx.IsUnique {
			joined := JoinFieldNames(idx.Fields)
			fVar := fmt.Sprintf("uniqFuture_%d", idx.IndexID)
			kVar := fmt.Sprintf("uniqKey_%d", idx.IndexID)
			var keyDiffParts []string
			for _, f := range idx.Fields {
				keyDiffParts = append(keyDiffParts, FieldNotEqual("old."+f.GoName, "entity."+f.GoName, f))
			}
			keyDiffExpr := strings.Join(keyDiffParts, " || ")
			anyDiffExpr := keyDiffExpr
			for _, sf := range idx.StoringFields {
				anyDiffExpr += " || " + FieldNotEqual("old."+sf.GoName, "entity."+sf.GoName, sf)
			}
			g.P("			{")
			if idx.IsSparse {
				g.P("				oldPresent := old != nil && (", SparseIndexPresentExpr(idx.Fields, "old"), ")")
				g.P("				newPresent := ", fVar, " != nil")
			} else {
				g.P("				oldPresent := old != nil")
				g.P("				newPresent := true")
			}
			g.P("				if oldPresent && (!newPresent || ", keyDiffExpr, ") {")
			g.P("					tr.Clear(dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.IndexNamespace, ", idx.IndexID, ", ", indexFieldTupleArgs(idx.Fields, "old"), "}))")
			g.P("				}")
			g.P("				if newPresent && (!oldPresent || ", anyDiffExpr, ") {")
			g.P("					if !oldPresent || ", keyDiffExpr, " {")
			g.P("						if ", fVar, ".MustGet() != nil {")
			g.P(`							return fmt.Errorf("`, lowerName, ` unique index `, joined, `: %w", fdblayer.ErrUniqueConstraintViolation)`)
			g.P("						}")
			g.P("					}")
			emitStoringValueExpr(g, msg, idx, "entity", "idxVal", "\t\t\t\t\t")
			g.P("					tr.Set(", kVar, ", idxVal)")
			g.P("				}")
			g.P("			}")
			continue
		}
		emitIndexSet(g, msg, idx, "\t\t\t")
	}

	generateMarshalWithZeroedAtomics(g, msg)
	g.P("			tr.Set(key, value)")

	if hasMut {
		pkArgsEntity := pkTupleArgsFromEntity(msg, "entity")
		newCond := "isNewRecord"
		if hasIdx {
			newCond = "old == nil"
		}
		g.P("			if ", newCond, " {")
		for _, f := range msg.Fields {
			if f.Mutation != ir.MutationNone {
				if f.Mutation == ir.MutationMax && !f.IsUnsigned {
					g.P("				{")
				} else {
					g.P("				if entity.", f.GoName, " != 0 {")
				}
				g.P("					fieldKey := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgsEntity, ", ", f.Number, "})")
				g.P("					buf := make([]byte, 8)")
				g.P("					binary.LittleEndian.PutUint64(buf, ", encodeAtomicUint64Expr("entity."+f.GoName, f), ")")
				g.P("					tr.", MutationVerb(f.Mutation), "(fieldKey, buf)")
				g.P("				}")
			}
		}
		g.P("			}")
	}

	g.P("			return nil")
	g.P("		}, nil")
	g.P("	})")
	g.P("}")
	g.P()
}

func generateReplaceMutation(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	hasMut := ir.HasMutationFields(msg)

	g.P("// Replace returns a blind write Mutation that writes a ", msg.Name, " entity without reading the existing record.")
	g.P("func (r *", lowerName, "Repository) Replace(entity *", msg.Name, ") fdblayer.Mutation {")
	g.P("	return fdblayer.MutationFunc(func(ctx context.Context, tx *fdblayer.Tx) (func(context.Context) error, error) {")
	g.P("		return func(ctx context.Context) error {")
	g.P("			if err := ctx.Err(); err != nil {")
	g.P("				return err")
	g.P("			}")
	g.P("			tr := tx.Transaction()")
	g.P("			dir := tx.Dir()")
	g.P("			key := dir.Pack(", dataKeyTupleExprFromEntity(msg, "entity"), ")")
	generateMarshalWithZeroedAtomics(g, msg)
	g.P("			tr.Set(key, value)")
	if hasMut {
		pkExpr := "entity." + msg.PrimaryKeyFields[0].GoName
		if len(msg.PrimaryKeyFields) > 1 {
			var pkFields []string
			for _, f := range msg.PrimaryKeyFields {
				pkFields = append(pkFields, f.GoName+": entity."+f.GoName)
			}
			pkExpr = msg.Name + "PrimaryKey{" + strings.Join(pkFields, ", ") + "}"
		}
		for _, f := range msg.Fields {
			if f.Mutation != ir.MutationNone {
				g.P("			if err := r.Set", f.GoName, "(ctx, tx, ", pkExpr, ", entity.", f.GoName, "); err != nil {")
				g.P("				return err")
				g.P("			}")
			}
		}
	}
	g.P("			return nil")
	g.P("		}, nil")
	g.P("	})")
	g.P("}")
	g.P()
}

func generateDeleteMutation(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	pkType := pkTypeExpr(msg)
	pkArgs := pkTupleArgsFromParam(msg)
	typeIDConst := typeIDConstName(msg)

	g.P("// DeleteMutation returns a pipelined Mutation that deletes a ", msg.Name, " entity by its primary key.")
	g.P("func (r *", lowerName, "Repository) DeleteMutation(pk ", pkType, ") fdblayer.Mutation {")
	g.P("	return fdblayer.MutationFunc(func(ctx context.Context, tx *fdblayer.Tx) (func(context.Context) error, error) {")
	g.P("		if err := ctx.Err(); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		tr := tx.Transaction()")
	g.P("		dir := tx.Dir()")
	g.P("		key := dir.Pack(", dataKeyTupleExprFromParam(msg, "pk"), ")")
	if len(msg.SecondaryIndexes) > 0 {
		g.P("		valFuture := tr.Get(key)")
	}
	g.P("		return func(ctx context.Context) error {")
	g.P("			if err := ctx.Err(); err != nil {")
	g.P("				return err")
	g.P("			}")
	if len(msg.SecondaryIndexes) > 0 {
		g.P("			if value := valFuture.MustGet(); value != nil {")
		g.P("				entity := &", msg.Name, "{}")
		g.P("				if err := proto.Unmarshal(value, entity); err != nil {")
		g.P(`					return fmt.Errorf("failed to unmarshal existing `, lowerName, `: %w", err)`)
		g.P("				}")
		for _, f := range msg.PrimaryKeyFields {
			if len(msg.PrimaryKeyFields) == 1 {
				g.P("				entity.", f.GoName, " = pk")
			} else {
				g.P("				entity.", f.GoName, " = pk.", f.GoName)
			}
		}
		for _, idx := range msg.SecondaryIndexes {
			emitIndexDelete(g, msg, idx, "entity", "\t\t\t\t")
		}
		g.P("			}")
	}
	g.P("			tr.Clear(key)")
	if ir.HasMutationFields(msg) {
		for _, f := range msg.Fields {
			if f.Mutation != ir.MutationNone {
				g.P("			{")
				g.P("				fieldKey := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.FieldNamespace, ", pkArgs, ", ", f.Number, "})")
				g.P("				tr.Clear(fieldKey)")
				g.P("			}")
			}
		}
	}
	emitCascadeDeleteChildren(g, msg, "\t\t\t")
	g.P("			return nil")
	g.P("		}, nil")
	g.P("	})")
	g.P("}")
	g.P()
}
