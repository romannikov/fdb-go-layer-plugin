package ir

import (
	"fmt"
	"hash/fnv"
	"strings"
	"unicode"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	annotationspb "github.com/romannikov/fdb-layer/proto/fdb-layer"
)

// FieldKind represents a language-neutral Protobuf scalar, enum, or message kind.
type FieldKind int

const (
	KindUnknown FieldKind = iota
	KindInt32
	KindSint32
	KindSfixed32
	KindInt64
	KindSint64
	KindSfixed64
	KindUint32
	KindFixed32
	KindUint64
	KindFixed64
	KindFloat
	KindDouble
	KindString
	KindBool
	KindBytes
	KindEnum
	KindMessage
)

// IsInteger returns true if k is a 32-bit or 64-bit signed or unsigned integer kind.
func (k FieldKind) IsInteger() bool {
	switch k {
	case KindInt32, KindSint32, KindSfixed32,
		KindInt64, KindSint64, KindSfixed64,
		KindUint32, KindFixed32, KindUint64, KindFixed64:
		return true
	default:
		return false
	}
}

// IsValidKeyKind returns true if k can be used in a primary key or secondary index tuple.
func (k FieldKind) IsValidKeyKind() bool {
	switch k {
	case KindInt32, KindSint32, KindSfixed32,
		KindInt64, KindSint64, KindSfixed64,
		KindUint32, KindFixed32, KindUint64, KindFixed64,
		KindString, KindBool, KindBytes, KindEnum:
		return true
	default:
		return false
	}
}

// MutationType represents an FDB atomic mutation operation in the language-neutral IR.
type MutationType int

const (
	MutationNone MutationType = 0
	MutationAdd  MutationType = 1
	MutationMax  MutationType = 2
	MutationMin  MutationType = 3
)

// EnumValueSpec describes a single value in a Protobuf enum.
type EnumValueSpec struct {
	ProtoName string // Original UPPER_SNAKE_CASE name in .proto
	Number    int32  // Enum numeric value
}

// EnumSpec describes a Protobuf enum definition.
type EnumSpec struct {
	Name   string          // Protobuf enum name (e.g., "Role")
	Values []EnumValueSpec // Enum values in declaration order
}

// FieldSpec describes a single Protobuf field in a language-neutral representation.
type FieldSpec struct {
	ProtoName      string       // Original snake_case name in .proto
	GoName         string       // UpperCamelCase identifier
	Number         int32        // Protobuf field tag number
	Kind           FieldKind
	EnumGoName     string       // Scoped enum type name if Kind == KindEnum
	MessageGoName  string       // Scoped message type name if Kind == KindMessage
	IsRepeated     bool
	IsUnsigned     bool         // uint32, fixed32, uint64, fixed64
	IsEnum         bool
	IsVersionstamp bool
	Mutation       MutationType
}

// IndexPrefixSpec describes a non-colliding leading prefix of a composite secondary index.
type IndexPrefixSpec struct {
	Fields []FieldSpec
}

// SecondaryIndexSpec describes a secondary index on a message.
type SecondaryIndexSpec struct {
	Fields      []FieldSpec
	IsFanOut    bool
	FanOutField FieldSpec
	IndexID     int64
	Prefixes    []IndexPrefixSpec
}

// MessageSpec describes an FDB-persisted Protobuf message, FIFO queue, or helper message.
type MessageSpec struct {
	Name             string // Protobuf message name (e.g., "User")
	Fields           []FieldSpec
	PrimaryKeyFields []FieldSpec
	SecondaryIndexes []SecondaryIndexSpec
	NestedMessages   []MessageSpec
	NestedEnums      []EnumSpec
	IsQueue          bool
	GoPackagePath    string
	GoPackageName    string
	FilePrefix       string
}

// HasRepository returns true if this message is annotated with a primary_key or is_queue.
func (m MessageSpec) HasRepository() bool {
	return len(m.PrimaryKeyFields) > 0 || m.IsQueue
}

// FileSpec describes a single parsed .proto file in the language-neutral IR.
type FileSpec struct {
	ProtoPath               string
	Package                 string
	GoImportPath            string
	GoPackageName           string
	GeneratedFilenamePrefix string
	Enums                   []EnumSpec
	Messages                []MessageSpec
}

// AnnotatedMessages returns only the top-level messages in the file that require repository generation.
func (f *FileSpec) AnnotatedMessages() []MessageSpec {
	if f == nil {
		return nil
	}
	var out []MessageSpec
	for _, m := range f.Messages {
		if m.HasRepository() {
			out = append(out, m)
		}
	}
	return out
}

// ParseFile extracts and validates a language-neutral FileSpec from a protogen.File.
func ParseFile(file *protogen.File) (*FileSpec, error) {
	if file == nil {
		return nil, fmt.Errorf("cannot parse nil protogen.File")
	}
	var protoPath, pkg string
	if file.Desc != nil {
		protoPath = file.Desc.Path()
		pkg = string(file.Desc.Package())
	}
	goImportPath := string(file.GoImportPath)
	goPackageName := string(file.GoPackageName)
	prefix := file.GeneratedFilenamePrefix

	spec := &FileSpec{
		ProtoPath:               protoPath,
		Package:                 pkg,
		GoImportPath:            goImportPath,
		GoPackageName:           goPackageName,
		GeneratedFilenamePrefix: prefix,
	}

	for _, enum := range file.Enums {
		spec.Enums = append(spec.Enums, ProcessEnum(enum))
	}

	processed := make(map[string]bool, len(file.Messages))
	for _, message := range file.Messages {
		msgName := message.GoIdent.GoName
		dedupKey := goImportPath + "." + msgName
		if message.Desc != nil {
			dedupKey = string(message.Desc.FullName())
		}
		if processed[dedupKey] {
			continue
		}
		processed[dedupKey] = true

		var opts proto.Message
		if message.Desc != nil {
			opts = message.Desc.Options()
		}
		msgSpec, err := ProcessMessageAll(message, opts)
		if err != nil {
			return nil, err
		}
		if msgSpec != nil {
			msgSpec.GoPackagePath = goImportPath
			msgSpec.GoPackageName = goPackageName
			msgSpec.FilePrefix = prefix
			spec.Messages = append(spec.Messages, *msgSpec)
		}
	}

	return spec, nil
}

// ParsePlugin extracts all annotated MessageSpecs from a protogen.Plugin.
func ParsePlugin(plugin *protogen.Plugin) ([]MessageSpec, error) {
	if plugin == nil {
		return nil, fmt.Errorf("cannot parse nil protogen.Plugin")
	}
	var messages []MessageSpec
	processedFiles := make(map[string]bool)

	for _, file := range plugin.Files {
		if file == nil || !file.Generate {
			continue
		}
		fileKey := file.GeneratedFilenamePrefix
		if file.Desc != nil {
			fileKey = file.Desc.Path()
		}
		if processedFiles[fileKey] {
			continue
		}
		processedFiles[fileKey] = true

		fileSpec, err := ParseFile(file)
		if err != nil {
			return nil, err
		}
		messages = append(messages, fileSpec.AnnotatedMessages()...)
	}
	return messages, nil
}

// ProcessMessage extracts and validates a language-neutral MessageSpec from a protogen.Message.
// Returns (nil, nil) if the message has no primary_key and is not a queue.
func ProcessMessage(message *protogen.Message, msgOptions proto.Message) (*MessageSpec, error) {
	spec, err := ProcessMessageAll(message, msgOptions)
	if err != nil || spec == nil {
		return nil, err
	}
	if !spec.HasRepository() {
		return nil, nil
	}
	return spec, nil
}

// ProcessMessageAll extracts a MessageSpec from a protogen.Message (including unannotated
// helper messages, nested messages, and nested enums).
func ProcessMessageAll(message *protogen.Message, msgOptions proto.Message) (*MessageSpec, error) {
	if message == nil {
		return nil, fmt.Errorf("cannot process nil protogen.Message")
	}
	msgName := message.GoIdent.GoName
	if message.Desc != nil {
		if _, isNested := message.Desc.Parent().(protoreflect.MessageDescriptor); isNested || msgName == "" {
			msgName = string(message.Desc.Name())
		}
	}

	fieldMap := make(map[string]FieldSpec, len(message.Fields))
	fields := make([]FieldSpec, 0, len(message.Fields))
	for _, field := range message.Fields {
		if field == nil || field.Desc == nil {
			continue
		}
		f, err := extractField(field, msgName)
		if err != nil {
			return nil, err
		}
		fieldMap[f.ProtoName] = f
		fields = append(fields, f)
	}

	var primaryKey []string
	if msgOptions != nil && proto.HasExtension(msgOptions, annotationspb.E_PrimaryKey) {
		pkValues := proto.GetExtension(msgOptions, annotationspb.E_PrimaryKey)
		if pkValues != nil {
			switch v := pkValues.(type) {
			case []interface{}:
				for _, val := range v {
					primaryKey = append(primaryKey, val.(string))
				}
			case []string:
				primaryKey = v
			case string:
				primaryKey = []string{v}
			default:
				return nil, fmt.Errorf("unknown type for primary_key in message %s: %T", msgName, v)
			}
		}
	}

	var isQueue bool
	if msgOptions != nil && proto.HasExtension(msgOptions, annotationspb.E_IsQueue) {
		val := proto.GetExtension(msgOptions, annotationspb.E_IsQueue)
		if val != nil {
			isQueue = val.(bool)
		}
	}

	var rawIndexes []*annotationspb.SecondaryIndex
	if msgOptions != nil && proto.HasExtension(msgOptions, annotationspb.E_SecondaryIndex) {
		siValues := proto.GetExtension(msgOptions, annotationspb.E_SecondaryIndex)
		if siValues != nil {
			switch v := siValues.(type) {
			case []*annotationspb.SecondaryIndex:
				rawIndexes = v
			case *annotationspb.SecondaryIndex:
				rawIndexes = []*annotationspb.SecondaryIndex{v}
			default:
				return nil, fmt.Errorf("unknown type for secondary_index in message %s: %T", msgName, v)
			}
		}
	}

	spec, err := buildMessageSpecInternal(msgName, fields, fieldMap, primaryKey, rawIndexes, isQueue, true)
	if err != nil {
		return nil, err
	}

	for _, nestedEnum := range message.Enums {
		spec.NestedEnums = append(spec.NestedEnums, ProcessEnum(nestedEnum))
	}
	for _, nestedMsg := range message.Messages {
		if nestedMsg == nil {
			continue
		}
		if nestedMsg.Desc != nil && nestedMsg.Desc.IsMapEntry() {
			continue
		}
		var nestedOpts proto.Message
		if nestedMsg.Desc != nil {
			nestedOpts = nestedMsg.Desc.Options()
		}
		nestedSpec, err := ProcessMessageAll(nestedMsg, nestedOpts)
		if err != nil {
			return nil, err
		}
		if nestedSpec != nil {
			spec.NestedMessages = append(spec.NestedMessages, *nestedSpec)
		}
	}

	return spec, nil
}

// ProcessEnum extracts a language-neutral EnumSpec from a protogen.Enum.
func ProcessEnum(enum *protogen.Enum) EnumSpec {
	if enum == nil || enum.Desc == nil {
		return EnumSpec{}
	}
	enumName := string(enum.Desc.Name())
	values := make([]EnumValueSpec, 0, len(enum.Values))
	for _, v := range enum.Values {
		if v == nil || v.Desc == nil {
			continue
		}
		values = append(values, EnumValueSpec{
			ProtoName: string(v.Desc.Name()),
			Number:    int32(v.Desc.Number()),
		})
	}
	return EnumSpec{
		Name:   enumName,
		Values: values,
	}
}

// BuildMessageSpec validates and constructs a MessageSpec from extracted field specs and option values.
// Returns (nil, nil) if the message has no primary_key and is not a queue.
func BuildMessageSpec(
	msgName string,
	fields []FieldSpec,
	fieldMap map[string]FieldSpec,
	primaryKey []string,
	rawIndexes []*annotationspb.SecondaryIndex,
	isQueue bool,
) (*MessageSpec, error) {
	return buildMessageSpecInternal(msgName, fields, fieldMap, primaryKey, rawIndexes, isQueue, false)
}

func buildMessageSpecInternal(
	msgName string,
	fields []FieldSpec,
	fieldMap map[string]FieldSpec,
	primaryKey []string,
	rawIndexes []*annotationspb.SecondaryIndex,
	isQueue bool,
	allowUnannotated bool,
) (*MessageSpec, error) {
	vsCount := 0
	for _, f := range fields {
		if err := ValidateFieldSpec(f, msgName); err != nil {
			return nil, err
		}
		if f.IsVersionstamp {
			vsCount++
			if vsCount > 1 {
				return nil, fmt.Errorf("message %s has multiple is_versionstamp=true fields", msgName)
			}
		}
	}

	pkSet := make(map[string]bool, len(primaryKey))
	primaryKeyFields := make([]FieldSpec, 0, len(primaryKey))
	for _, pkName := range primaryKey {
		if pkSet[pkName] {
			return nil, fmt.Errorf("duplicate primary key field %q in message %s", pkName, msgName)
		}
		f, ok := fieldMap[pkName]
		if !ok {
			return nil, fmt.Errorf("primary key field %q not found in message %s", pkName, msgName)
		}
		if f.IsRepeated {
			return nil, fmt.Errorf("primary key field %q in message %s cannot be repeated", pkName, msgName)
		}
		if f.Mutation != MutationNone {
			return nil, fmt.Errorf("primary key field %q in message %s cannot have an atomic mutation", pkName, msgName)
		}
		if !f.Kind.IsValidKeyKind() {
			return nil, fmt.Errorf("primary key field %q in message %s has unsupported kind %v", pkName, msgName, f.Kind)
		}
		pkSet[pkName] = true
		primaryKeyFields = append(primaryKeyFields, f)
	}

	for _, f := range fields {
		if f.IsVersionstamp && !pkSet[f.ProtoName] {
			return nil, fmt.Errorf("field %q in message %s is marked is_versionstamp=true but is not in primary_key", f.ProtoName, msgName)
		}
	}

	usedHashes := make(map[int64]string)
	secondaryIndexes := make([]SecondaryIndexSpec, 0, len(rawIndexes))
	for _, idx := range rawIndexes {
		si, err := BuildSecondaryIndex(idx, fieldMap, usedHashes, msgName)
		if err != nil {
			return nil, err
		}
		secondaryIndexes = append(secondaryIndexes, si)
	}
	secondaryIndexes = PopulateIndexPrefixes(secondaryIndexes)

	if !allowUnannotated && len(primaryKeyFields) == 0 && !isQueue {
		return nil, nil
	}

	if isQueue {
		if !MsgHasVersionstampPK(MessageSpec{PrimaryKeyFields: primaryKeyFields}) {
			return nil, fmt.Errorf("queue message %s must have an is_versionstamp=true primary key field", msgName)
		}
		if len(primaryKeyFields) < 2 {
			return nil, fmt.Errorf("queue message %s must have at least 2 primary key fields (queue partition key and versionstamp)", msgName)
		}
		primaryKeyFields = ReorderQueuePrimaryKeyFields(primaryKeyFields)
	}

	return &MessageSpec{
		Name:             msgName,
		Fields:           fields,
		PrimaryKeyFields: primaryKeyFields,
		SecondaryIndexes: secondaryIndexes,
		IsQueue:          isQueue,
	}, nil
}

// ValidateFieldSpec checks field-level constraints for versionstamp and atomic mutation annotations.
func ValidateFieldSpec(f FieldSpec, msgName string) error {
	if f.IsVersionstamp && f.Kind != KindBytes {
		return fmt.Errorf("field %q in message %s is marked is_versionstamp=true but is %v (must be bytes)", f.ProtoName, msgName, f.Kind)
	}
	if f.IsVersionstamp && f.IsRepeated {
		return fmt.Errorf("field %q in message %s is marked is_versionstamp=true but cannot be repeated", f.ProtoName, msgName)
	}
	if f.Mutation != MutationNone && f.IsRepeated {
		return fmt.Errorf("field %q in message %s has atomic mutation %v but cannot be repeated", f.ProtoName, msgName, f.Mutation)
	}
	if f.Mutation != MutationNone && !f.Kind.IsInteger() {
		return fmt.Errorf("field %q in message %s has atomic mutation %v but is not a 32-bit or 64-bit integer type", f.ProtoName, msgName, f.Mutation)
	}
	return nil
}

func extractField(field *protogen.Field, msgName string) (FieldSpec, error) {
	fieldOptions := field.Desc.Options()
	var mutation MutationType
	if fieldOptions != nil && proto.HasExtension(fieldOptions, annotationspb.E_Mutation) {
		m := proto.GetExtension(fieldOptions, annotationspb.E_Mutation)
		if m != nil {
			switch m.(annotationspb.MutationType) {
			case annotationspb.MutationType_MUTATION_ADD:
				mutation = MutationAdd
			case annotationspb.MutationType_MUTATION_MAX:
				mutation = MutationMax
			case annotationspb.MutationType_MUTATION_MIN:
				mutation = MutationMin
			default:
				mutation = MutationNone
			}
		}
	}

	var isVersionstamp bool
	if fieldOptions != nil && proto.HasExtension(fieldOptions, annotationspb.E_IsVersionstamp) {
		val := proto.GetExtension(fieldOptions, annotationspb.E_IsVersionstamp)
		if val != nil {
			isVersionstamp = val.(bool)
		}
	}

	protoKind := field.Desc.Kind()
	kind := MapProtoKind(protoKind)

	var enumGoName string
	if protoKind == protoreflect.EnumKind && field.Enum != nil {
		enumGoName = field.Enum.GoIdent.GoName
	}

	var msgGoName string
	if protoKind == protoreflect.MessageKind && field.Message != nil && !field.Desc.IsMap() {
		msgGoName = field.Message.GoIdent.GoName
	}

	protoName := string(field.Desc.Name())
	spec := FieldSpec{
		ProtoName:      protoName,
		GoName:         field.GoName,
		Number:         int32(field.Desc.Number()),
		Kind:           kind,
		EnumGoName:     enumGoName,
		MessageGoName:  msgGoName,
		IsRepeated:     field.Desc.IsList(),
		IsUnsigned:     kind == KindUint32 || kind == KindFixed32 || kind == KindUint64 || kind == KindFixed64,
		IsEnum:         kind == KindEnum,
		IsVersionstamp: isVersionstamp,
		Mutation:       mutation,
	}
	if err := ValidateFieldSpec(spec, msgName); err != nil {
		return FieldSpec{}, err
	}
	return spec, nil
}

func toPascalCase(s string) string {
	parts := strings.Split(s, "_")
	var out strings.Builder
	for _, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(strings.ToLower(part))
		runes[0] = unicode.ToUpper(runes[0])
		out.WriteString(string(runes))
	}
	if out.Len() == 0 {
		return s
	}
	return out.String()
}

func fieldsSignature(fields []FieldSpec) string {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		name := f.GoName
		if name == "" {
			name = toPascalCase(f.ProtoName)
		}
		names = append(names, strings.ToLower(name))
	}
	return strings.Join(names, ",")
}

// BuildSecondaryIndex validates and constructs a SecondaryIndexSpec.
func BuildSecondaryIndex(
	idx *annotationspb.SecondaryIndex,
	fieldMap map[string]FieldSpec,
	usedHashes map[int64]string,
	msgName string,
) (SecondaryIndexSpec, error) {
	if idx == nil || len(idx.GetFields()) == 0 {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s must specify at least one field", msgName)
	}
	seenFields := make(map[string]bool, len(idx.GetFields()))
	idxFields := make([]FieldSpec, 0, len(idx.GetFields()))
	for _, idxFieldName := range idx.GetFields() {
		if seenFields[idxFieldName] {
			return SecondaryIndexSpec{}, fmt.Errorf("duplicate field %q in secondary index of message %s", idxFieldName, msgName)
		}
		seenFields[idxFieldName] = true
		f, ok := fieldMap[idxFieldName]
		if !ok {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index field %q not found in message %s", idxFieldName, msgName)
		}
		if f.Mutation != MutationNone {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index field %q in message %s cannot have an atomic mutation", idxFieldName, msgName)
		}
		if f.IsVersionstamp {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index field %q in message %s cannot be a versionstamp field", idxFieldName, msgName)
		}
		if !f.Kind.IsValidKeyKind() {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index field %q in message %s has unsupported kind %v", idxFieldName, msgName, f.Kind)
		}
		idxFields = append(idxFields, f)
	}

	isFanOut := false
	var fanOutField FieldSpec
	for _, f := range idxFields {
		if f.IsRepeated {
			if isFanOut {
				return SecondaryIndexSpec{}, fmt.Errorf("multiple repeated fields in secondary index are not supported in message %s", msgName)
			}
			isFanOut = true
			fanOutField = f
		}
	}

	signature := fieldsSignature(idxFields)

	var indexID int64
	if idx.GetId() != 0 {
		indexID = int64(idx.GetId())
	} else {
		indexID = ComputeIndexID(signature)
	}

	if existingSig, ok := usedHashes[indexID]; ok {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index ID collision detected in message %s: indices with fields %q and %q share the same ID %d", msgName, signature, existingSig, indexID)
	}
	if usedHashes != nil {
		usedHashes[indexID] = signature
	}

	return SecondaryIndexSpec{
		Fields:      idxFields,
		IsFanOut:    isFanOut,
		FanOutField: fanOutField,
		IndexID:     indexID,
	}, nil
}

// PopulateIndexPrefixes populates non-colliding leading prefix specs (1 <= k < len(Fields))
// for each composite non-fan-out secondary index on a message.
func PopulateIndexPrefixes(indexes []SecondaryIndexSpec) []SecondaryIndexSpec {
	if len(indexes) == 0 {
		return indexes
	}
	out := make([]SecondaryIndexSpec, len(indexes))
	copy(out, indexes)
	for i := range out {
		out[i].Prefixes = EffectiveIndexPrefixes(out, i)
	}
	return out
}

// EffectiveIndexPrefixes returns the non-colliding leading prefixes for indexes[idxPos].
// If a prefix matches another full index on the same message or an earlier composite index's
// prefix, it is skipped to prevent duplicate method generation.
func EffectiveIndexPrefixes(indexes []SecondaryIndexSpec, idxPos int) []IndexPrefixSpec {
	if idxPos < 0 || idxPos >= len(indexes) {
		return nil
	}
	target := indexes[idxPos]
	if target.IsFanOut || len(target.Fields) <= 1 {
		return nil
	}
	if len(target.Prefixes) > 0 {
		return target.Prefixes
	}

	claimed := make(map[string]bool, len(indexes)*2)
	for _, idx := range indexes {
		claimed[fieldsSignature(idx.Fields)] = true
	}

	var prefixes []IndexPrefixSpec
	for i := 0; i <= idxPos; i++ {
		idx := indexes[i]
		if idx.IsFanOut || len(idx.Fields) <= 1 {
			continue
		}
		for k := 1; k < len(idx.Fields); k++ {
			prefixFields := idx.Fields[:k]
			sig := fieldsSignature(prefixFields)
			if claimed[sig] {
				continue
			}
			claimed[sig] = true
			if i == idxPos {
				copied := make([]FieldSpec, k)
				copy(copied, prefixFields)
				prefixes = append(prefixes, IndexPrefixSpec{Fields: copied})
			}
		}
	}
	return prefixes
}

// ComputeIndexID computes the 32-bit FNV-1a hash of the lowercase comma-joined field signature as int64.
func ComputeIndexID(signature string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(signature))
	return int64(h.Sum32())
}

// ReorderQueuePrimaryKeyFields reorders queue primary keys to:
// [first_non_versionstamp_pk (queue_name), versionstamp_fields..., remaining_tie_breaker_fields...].
func ReorderQueuePrimaryKeyFields(pkFields []FieldSpec) []FieldSpec {
	if len(pkFields) < 2 {
		return pkFields
	}
	firstNonVS := -1
	for i, f := range pkFields {
		if !f.IsVersionstamp {
			firstNonVS = i
			break
		}
	}
	if firstNonVS == -1 {
		return pkFields
	}
	reordered := make([]FieldSpec, 0, len(pkFields))
	reordered = append(reordered, pkFields[firstNonVS])
	for _, f := range pkFields {
		if f.IsVersionstamp {
			reordered = append(reordered, f)
		}
	}
	for i, f := range pkFields {
		if i != firstNonVS && !f.IsVersionstamp {
			reordered = append(reordered, f)
		}
	}
	return reordered
}

// MapProtoKind converts a protoreflect.Kind into a language-neutral FieldKind.
func MapProtoKind(kind protoreflect.Kind) FieldKind {
	switch kind {
	case protoreflect.Int32Kind:
		return KindInt32
	case protoreflect.Sint32Kind:
		return KindSint32
	case protoreflect.Sfixed32Kind:
		return KindSfixed32
	case protoreflect.Int64Kind:
		return KindInt64
	case protoreflect.Sint64Kind:
		return KindSint64
	case protoreflect.Sfixed64Kind:
		return KindSfixed64
	case protoreflect.Uint32Kind:
		return KindUint32
	case protoreflect.Fixed32Kind:
		return KindFixed32
	case protoreflect.Uint64Kind:
		return KindUint64
	case protoreflect.Fixed64Kind:
		return KindFixed64
	case protoreflect.FloatKind:
		return KindFloat
	case protoreflect.DoubleKind:
		return KindDouble
	case protoreflect.StringKind:
		return KindString
	case protoreflect.BoolKind:
		return KindBool
	case protoreflect.BytesKind:
		return KindBytes
	case protoreflect.EnumKind:
		return KindEnum
	case protoreflect.MessageKind:
		return KindMessage
	default:
		return KindUnknown
	}
}

// HasMutationFields returns true if the message has any atomic mutation fields.
func HasMutationFields(msg MessageSpec) bool {
	for _, f := range msg.Fields {
		if f.Mutation != MutationNone {
			return true
		}
	}
	return false
}

// MsgHasVersionstampPK returns true if any primary key field on the message is a versionstamp.
func MsgHasVersionstampPK(msg MessageSpec) bool {
	for _, f := range msg.PrimaryKeyFields {
		if f.IsVersionstamp {
			return true
		}
	}
	return false
}
