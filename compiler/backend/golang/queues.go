package golang

import (
	"strings"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

func generateQueueRepository(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	firstPK := msg.PrimaryKeyFields[0]
	firstPKLower := safeGoParamName(strings.ToLower(firstPK.GoName))
	typeIDConst := typeIDConstName(msg)

	g.P("// ", typeIDConst, " is the compile-time type prefix for ", msg.Name, " records in FoundationDB.")
	g.P("const ", typeIDConst, " int64 = ", effectiveTypeID(msg))
	g.P()
	g.P("// ", msg.Name, "Repository defines the repository interface for the ", msg.Name, " queue.")
	g.P("type ", msg.Name, "Repository interface {")
	g.P("	Enqueue(ctx context.Context, tx *fdblayer.Tx, msg *", msg.Name, ") error")
	g.P("	Dequeue(ctx context.Context, tx *fdblayer.Tx, ", firstPKLower, " ", FieldGoType(firstPK), ") (*", msg.Name, ", error)")
	g.P("	DequeueBatch(ctx context.Context, tx *fdblayer.Tx, ", firstPKLower, " ", FieldGoType(firstPK), ", limit int) ([]*", msg.Name, ", error)")
	g.P("}")
	g.P()
	g.P("type ", lowerName, "Repository struct{}")
	g.P()
	g.P("// New", msg.Name, "Repository creates a new ", msg.Name, "Repository instance.")
	g.P("func New", msg.Name, "Repository() ", msg.Name, "Repository {")
	g.P("	return &", lowerName, "Repository{}")
	g.P("}")
	g.P()

	generateEnqueue(g, msg)
	generateDequeue(g, msg)
	generateDequeueBatch(g, msg)
}

func generateEnqueue(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	hasVS := ir.MsgHasVersionstampPK(msg)
	typeIDConst := typeIDConstName(msg)

	g.P("// Enqueue adds a ", msg.Name, " to the queue.")
	g.P("func (r *", lowerName, "Repository) Enqueue(ctx context.Context, tx *fdblayer.Tx, entity *", msg.Name, ") error {")
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
		g.P("	key := dir.Pack(tuple.Tuple{", typeIDConst, ", fdblayer.DataNamespace, ", pkTupleArgsFromEntity(msg, "entity"), "})")
	}
	g.P()
	generateMarshalWithZeroedAtomics(g, msg)
	if hasVS {
		g.P("	tr.SetVersionstampedKey(key, value)")
	} else {
		g.P("	tr.Set(key, value)")
	}
	g.P()
	g.P("	return nil")
	g.P("}")
	g.P()
}

func generateDequeue(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	firstPK := msg.PrimaryKeyFields[0]
	firstPKLower := safeGoParamName(strings.ToLower(firstPK.GoName))
	typeIDConst := typeIDConstName(msg)

	g.P("// Dequeue reads and removes the single oldest message from the queue.")
	g.P("func (r *", lowerName, "Repository) Dequeue(ctx context.Context, tx *fdblayer.Tx, ", firstPKLower, " ", FieldGoType(firstPK), ") (*", msg.Name, ", error) {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	tr := tx.Transaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	g.P("	prefixTuple := tuple.Tuple{", typeIDConst, ", fdblayer.DataNamespace, ", PackField(firstPKLower, firstPK), "}")
	g.P("	prefixRange, err := fdb.PrefixRange(dir.Pack(prefixTuple))")
	g.P("	if err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P()
	g.P("	options := fdb.RangeOptions{Limit: 1}")
	g.P("	rows := fdblayer.GetRange(tr, prefixRange, options).GetSliceOrPanic()")
	g.P()
	g.P("	if len(rows) == 0 {")
	g.P("		return nil, nil")
	g.P("	}")
	g.P()
	g.P("	firstItem := rows[0]")
	g.P("	keyToDelete := firstItem.Key")
	g.P("	payload := firstItem.Value")
	g.P()
	g.P("	entity := &", msg.Name, "{}")
	g.P("	err = proto.Unmarshal(payload, entity)")
	g.P("	if err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P()
	if ir.MsgHasVersionstampPK(msg) {
		g.P("	tpl, unpackErr := dir.Unpack(keyToDelete)")
		g.P("	if unpackErr != nil {")
		g.P("		return nil, unpackErr")
		g.P("	}")
		for i, f := range msg.PrimaryKeyFields {
			if f.IsVersionstamp {
				g.P("	if len(tpl) > ", i, "+2 {")
				g.P("		if vs, ok := tpl[", i, "+2].(tuple.Versionstamp); ok {")
				g.P("			entity.", f.GoName, " = vs.Bytes()")
				g.P("		}")
				g.P("	}")
			}
		}
		g.P()
	}
	g.P("	tr.Clear(keyToDelete)")
	g.P("	return entity, nil")
	g.P("}")
	g.P()
}

func generateDequeueBatch(g Printer, msg ir.MessageSpec) {
	lowerName := strings.ToLower(msg.Name)
	firstPK := msg.PrimaryKeyFields[0]
	firstPKLower := safeGoParamName(strings.ToLower(firstPK.GoName))
	typeIDConst := typeIDConstName(msg)

	g.P("// DequeueBatch reads and removes up to limit oldest messages from the queue in a single range scan.")
	g.P("func (r *", lowerName, "Repository) DequeueBatch(ctx context.Context, tx *fdblayer.Tx, ", firstPKLower, " ", FieldGoType(firstPK), ", limit int) ([]*", msg.Name, ", error) {")
	g.P("	if err := ctx.Err(); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	if limit <= 0 {")
	g.P("		return nil, nil")
	g.P("	}")
	g.P("	tr := tx.Transaction()")
	g.P("	dir := tx.Dir()")
	g.P()
	g.P("	prefixTuple := tuple.Tuple{", typeIDConst, ", fdblayer.DataNamespace, ", PackField(firstPKLower, firstPK), "}")
	g.P("	prefixRange, err := fdb.PrefixRange(dir.Pack(prefixTuple))")
	g.P("	if err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P()
	g.P("	rows := fdblayer.GetRange(tr, prefixRange, fdb.RangeOptions{Limit: limit}).GetSliceOrPanic()")
	g.P("	if len(rows) == 0 {")
	g.P("		return nil, nil")
	g.P("	}")
	g.P()
	g.P("	items := make([]*", msg.Name, ", 0, len(rows))")
	g.P("	for _, row := range rows {")
	g.P("		if err := ctx.Err(); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	g.P("		entity := &", msg.Name, "{}")
	g.P("		if err := proto.Unmarshal(row.Value, entity); err != nil {")
	g.P("			return nil, err")
	g.P("		}")
	if ir.MsgHasVersionstampPK(msg) {
		g.P("		tpl, unpackErr := dir.Unpack(row.Key)")
		g.P("		if unpackErr != nil {")
		g.P("			return nil, unpackErr")
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
	}
	g.P("		tr.Clear(row.Key)")
	g.P("		items = append(items, entity)")
	g.P("	}")
	g.P("	return items, nil")
	g.P("}")
}
