package ir

import (
	"fmt"
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
	MutationNone  MutationType = 0
	MutationAdd   MutationType = 1
	MutationMax   MutationType = 2
	MutationMin   MutationType = 3
	MutationCount MutationType = 4
)

// IndexAggregateType represents a secondary index aggregation mode in the language-neutral IR.
type IndexAggregateType int

const (
	AggregateNone  IndexAggregateType = 0
	AggregateCount IndexAggregateType = 1
	AggregateSum   IndexAggregateType = 2
	AggregateMin   IndexAggregateType = 3
	AggregateMax   IndexAggregateType = 4
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
	ProtoName      string // Original snake_case name in .proto
	GoName         string // UpperCamelCase identifier
	Number         int32  // Protobuf field tag number
	Kind           FieldKind
	EnumGoName     string // Scoped enum type name if Kind == KindEnum
	MessageGoName  string // Scoped message type name if Kind == KindMessage
	IsRepeated     bool
	IsUnsigned     bool // uint32, fixed32, uint64, fixed64
	IsEnum         bool
	IsVersionstamp bool
	Mutation       MutationType
}

// SecondaryIndexSpec describes a secondary index on a message.
type SecondaryIndexSpec struct {
	Fields         []FieldSpec
	IsFanOut       bool
	FanOutField    FieldSpec
	IndexID        int64
	Aggregate      IndexAggregateType
	AggregateField FieldSpec
	Mutation       MutationType
	Name           string // PascalCase optional name for mutation index constants
}

// IsAggregate returns true if the secondary index is an atomic aggregate index (COUNT, SUM, MIN, MAX).
func (s SecondaryIndexSpec) IsAggregate() bool {
	return s.Aggregate != AggregateNone
}

// IsMutation returns true if the secondary index is a manual atomic mutation index (ADD, MAX, MIN, COUNT).
func (s SecondaryIndexSpec) IsMutation() bool {
	return s.Mutation != MutationNone
}

// MessageSpec describes an FDB-persisted Protobuf message, FIFO queue, or helper message.
type MessageSpec struct {
	Name             string // Protobuf message name (e.g., "User")
	TypeID           int64  // Mandatory explicit non-zero compile-time FDB type prefix (annotations.type_id)
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

// ValidateUniqueTypeIDs checks that every annotated message has a non-zero TypeID and no two share the same TypeID.
func ValidateUniqueTypeIDs(messages []MessageSpec) error {
	usedTypeIDs := make(map[int64]string, len(messages))
	for _, m := range messages {
		if !m.HasRepository() {
			continue
		}
		if m.TypeID <= 0 {
			return fmt.Errorf("message %s: explicit non-zero option (annotations.type_id) is required", m.Name)
		}
		if existingMsg, ok := usedTypeIDs[m.TypeID]; ok {
			return fmt.Errorf("message type ID collision detected: messages %q and %q share the same type ID %d", m.Name, existingMsg, m.TypeID)
		}
		usedTypeIDs[m.TypeID] = m.Name
	}
	return nil
}

// ParseFile extracts and validates a language-neutral FileSpec from a protogen.File.
func ParseFile(file *protogen.File) (*FileSpec, error) {
	if file == nil {
		return nil, fmt.Errorf("cannot parse nil protogen.File")
	}
	goImportPath := string(file.GoImportPath)
	goPackageName := string(file.GoPackageName)
	prefix := file.GeneratedFilenamePrefix
	var protoPath, pkg string
	if file.Desc != nil {
		protoPath = file.Desc.Path()
		pkg = string(file.Desc.Package())
	}

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

	if err := ValidateUniqueTypeIDs(spec.Messages); err != nil {
		return nil, err
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
	if err := ValidateUniqueTypeIDs(messages); err != nil {
		return nil, err
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

	if msgOptions != nil {
		if proto.HasExtension(msgOptions, annotationspb.E_Interleave) {
			if val := proto.GetExtension(msgOptions, annotationspb.E_Interleave); val != nil {
				if spec, ok := val.(*annotationspb.InterleaveSpec); !ok || spec != nil {
					return nil, fmt.Errorf("message %s: option (annotations.interleave) is reserved and not implemented yet", msgName)
				}
			}
		}
		if proto.HasExtension(msgOptions, annotationspb.E_SplitLargeRecords) {
			if val, ok := proto.GetExtension(msgOptions, annotationspb.E_SplitLargeRecords).(bool); ok && val {
				return nil, fmt.Errorf("message %s: option (annotations.split_large_records) is reserved and not implemented yet", msgName)
			}
		}
		if proto.HasExtension(msgOptions, annotationspb.E_TtlField) {
			if val, ok := proto.GetExtension(msgOptions, annotationspb.E_TtlField).(string); ok && val != "" {
				return nil, fmt.Errorf("message %s: option (annotations.ttl_field) is reserved and not implemented yet", msgName)
			}
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
			case *annotationspb.PrimaryKey:
				if v != nil {
					primaryKey = v.GetFields()
				}
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

	var explicitTypeID uint32
	if msgOptions != nil && proto.HasExtension(msgOptions, annotationspb.E_TypeId) {
		val := proto.GetExtension(msgOptions, annotationspb.E_TypeId)
		if val != nil {
			switch v := val.(type) {
			case uint32:
				explicitTypeID = v
			default:
				return nil, fmt.Errorf("unknown type for type_id in message %s: %T", msgName, v)
			}
		}
	}

	spec, err := buildMessageSpecInternal(msgName, fields, fieldMap, primaryKey, rawIndexes, isQueue, explicitTypeID, true)
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

// BuildMessageSpecWithTypeID validates and constructs a MessageSpec with a mandatory explicit non-zero type ID.
// Returns (nil, nil) if the message has no primary_key, is not a queue, and has explicitTypeID == 0.
func BuildMessageSpecWithTypeID(
	msgName string,
	fields []FieldSpec,
	fieldMap map[string]FieldSpec,
	primaryKey []string,
	rawIndexes []*annotationspb.SecondaryIndex,
	isQueue bool,
	explicitTypeID uint32,
) (*MessageSpec, error) {
	return buildMessageSpecInternal(msgName, fields, fieldMap, primaryKey, rawIndexes, isQueue, explicitTypeID, false)
}

func buildMessageSpecInternal(
	msgName string,
	fields []FieldSpec,
	fieldMap map[string]FieldSpec,
	primaryKey []string,
	rawIndexes []*annotationspb.SecondaryIndex,
	isQueue bool,
	explicitTypeID uint32,
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

	if (explicitTypeID != 0 || len(rawIndexes) > 0) && len(primaryKeyFields) == 0 && !isQueue {
		return nil, fmt.Errorf("message %s: primary_key is required when type_id or secondary_index is specified", msgName)
	}
	if isQueue && len(rawIndexes) > 0 {
		return nil, fmt.Errorf("queue message %s cannot define secondary_index", msgName)
	}

	usedHashes := make(map[int64]string)
	seenSignatures := make(map[string]int64)
	seenConstNames := make(map[string]int64)
	secondaryIndexes := make([]SecondaryIndexSpec, 0, len(rawIndexes))
	for _, idx := range rawIndexes {
		si, err := BuildSecondaryIndex(idx, fieldMap, usedHashes, msgName)
		if err != nil {
			return nil, err
		}
		sig := indexSignature(si)
		if prevID, dup := seenSignatures[sig]; dup {
			return nil, fmt.Errorf("duplicate secondary index on [%s] in message %s (IDs %d and %d)", sig, msgName, prevID, si.IndexID)
		}
		seenSignatures[sig] = si.IndexID

		constName := msgName + IndexConstSuffix(si)
		if prevID, dup := seenConstNames[constName]; dup {
			return nil, fmt.Errorf("duplicate secondary index constant name %q in message %s (IDs %d and %d); specify a distinct 'name' on the mutation index", constName, msgName, prevID, si.IndexID)
		}
		seenConstNames[constName] = si.IndexID
		secondaryIndexes = append(secondaryIndexes, si)
	}

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

	var typeID int64
	if len(primaryKeyFields) > 0 || isQueue {
		if explicitTypeID == 0 {
			return nil, fmt.Errorf("message %s: explicit non-zero option (annotations.type_id) is required", msgName)
		}
		typeID = int64(explicitTypeID)
	}

	return &MessageSpec{
		Name:             msgName,
		TypeID:           typeID,
		Fields:           fields,
		PrimaryKeyFields: primaryKeyFields,
		SecondaryIndexes: secondaryIndexes,
		IsQueue:          isQueue,
	}, nil
}

// ValidateFieldSpec checks field-level constraints for versionstamp and rejects unimplemented mutation options.
func ValidateFieldSpec(f FieldSpec, msgName string) error {
	if f.Mutation != MutationNone {
		return fmt.Errorf("field %q in message %s: option (annotations.mutation) is reserved and not implemented yet", f.ProtoName, msgName)
	}
	if f.IsVersionstamp && f.Kind != KindBytes {
		return fmt.Errorf("field %q in message %s is marked is_versionstamp=true but is %v (must be bytes)", f.ProtoName, msgName, f.Kind)
	}
	if f.IsVersionstamp && f.IsRepeated {
		return fmt.Errorf("field %q in message %s is marked is_versionstamp=true but cannot be repeated", f.ProtoName, msgName)
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
			case annotationspb.MutationType_MUTATION_COUNT:
				mutation = MutationCount
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

func isValidIndexName(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if i == 0 {
			if !unicode.IsLetter(r) {
				return false
			}
		} else {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
				return false
			}
		}
	}
	return true
}

// IndexConstSuffix returns the deterministic suffix for an index's Go constant name
// (everything after the message name prefix, e.g. "IndexByEmail", "CountByDepartment",
// "SumAmountByTenantId", "MaxLatencyById", "AddLikesById").
func IndexConstSuffix(si SecondaryIndexSpec) string {
	var fieldNames strings.Builder
	for _, f := range si.Fields {
		name := f.GoName
		if name == "" {
			name = toPascalCase(f.ProtoName)
		}
		fieldNames.WriteString(name)
	}
	byFields := fieldNames.String()

	if si.IsMutation() {
		var opPrefix string
		switch si.Mutation {
		case MutationCount:
			opPrefix = "Count"
		case MutationAdd:
			opPrefix = "Add"
		case MutationMin:
			opPrefix = "Min"
		case MutationMax:
			opPrefix = "Max"
		}
		custom := toPascalCase(si.Name)
		var base string
		if custom != "" {
			if strings.HasPrefix(custom, opPrefix) {
				base = custom
			} else {
				base = opPrefix + custom
			}
		} else {
			base = opPrefix
		}
		if byFields == "" {
			return base
		}
		return base + "By" + byFields
	}

	switch si.Aggregate {
	case AggregateCount:
		if byFields == "" {
			return "Count"
		}
		return "CountBy" + byFields
	case AggregateSum, AggregateMin, AggregateMax:
		var opPrefix string
		switch si.Aggregate {
		case AggregateSum:
			opPrefix = "Sum"
		case AggregateMin:
			opPrefix = "Min"
		case AggregateMax:
			opPrefix = "Max"
		}
		aggFieldGoName := si.AggregateField.GoName
		if aggFieldGoName == "" {
			aggFieldGoName = toPascalCase(si.AggregateField.ProtoName)
		}
		if byFields == "" {
			return opPrefix + aggFieldGoName
		}
		return opPrefix + aggFieldGoName + "By" + byFields
	default:
		return "By" + byFields
	}
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

func indexSignature(si SecondaryIndexSpec) string {
	base := fieldsSignature(si.Fields)
	if si.IsMutation() {
		var mutOp string
		switch si.Mutation {
		case MutationCount:
			mutOp = "mutation_count"
		case MutationAdd:
			mutOp = "mutation_add"
		case MutationMin:
			mutOp = "mutation_min"
		case MutationMax:
			mutOp = "mutation_max"
		}
		namePart := strings.ToLower(toPascalCase(si.Name))
		return mutOp + "(" + namePart + "|by:" + base + ")"
	}
	switch si.Aggregate {
	case AggregateCount:
		return "count(" + base + ")"
	case AggregateSum:
		return "sum(" + strings.ToLower(si.AggregateField.ProtoName) + "|by:" + base + ")"
	case AggregateMin:
		return "min(" + strings.ToLower(si.AggregateField.ProtoName) + "|by:" + base + ")"
	case AggregateMax:
		return "max(" + strings.ToLower(si.AggregateField.ProtoName) + "|by:" + base + ")"
	default:
		return base
	}
}

// BuildSecondaryIndex validates and constructs a SecondaryIndexSpec.
func BuildSecondaryIndex(
	idx *annotationspb.SecondaryIndex,
	fieldMap map[string]FieldSpec,
	usedHashes map[int64]string,
	msgName string,
) (SecondaryIndexSpec, error) {
	if idx == nil {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s must specify at least one field", msgName)
	}
	if idx.GetUnique() {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s: option 'unique' is reserved and not implemented yet", msgName)
	}
	if len(idx.GetStoring()) > 0 {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s: option 'storing' is reserved and not implemented yet", msgName)
	}
	if idx.GetSparse() {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s: option 'sparse' is reserved and not implemented yet", msgName)
	}

	var aggType IndexAggregateType
	switch idx.GetAggregate() {
	case annotationspb.IndexAggregateType_AGGREGATE_NONE:
		aggType = AggregateNone
	case annotationspb.IndexAggregateType_AGGREGATE_COUNT:
		aggType = AggregateCount
	case annotationspb.IndexAggregateType_AGGREGATE_SUM:
		aggType = AggregateSum
	case annotationspb.IndexAggregateType_AGGREGATE_MIN:
		aggType = AggregateMin
	case annotationspb.IndexAggregateType_AGGREGATE_MAX:
		aggType = AggregateMax
	default:
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s has unknown aggregate type %v", msgName, idx.GetAggregate())
	}

	var mutType MutationType
	switch idx.GetMutation() {
	case annotationspb.MutationType_MUTATION_NONE:
		mutType = MutationNone
	case annotationspb.MutationType_MUTATION_ADD:
		mutType = MutationAdd
	case annotationspb.MutationType_MUTATION_MAX:
		mutType = MutationMax
	case annotationspb.MutationType_MUTATION_MIN:
		mutType = MutationMin
	case annotationspb.MutationType_MUTATION_COUNT:
		mutType = MutationCount
	default:
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s has unknown mutation type %v", msgName, idx.GetMutation())
	}

	if aggType != AggregateNone && mutType != MutationNone {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s cannot specify both aggregate and mutation", msgName)
	}

	idxName := idx.GetName()
	if idxName != "" {
		if mutType == MutationNone {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s: option 'name' is only supported on mutation indexes", msgName)
		}
		if !isValidIndexName(idxName) {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s: invalid name %q (must start with a letter and contain only letters, digits, or underscores)", msgName, idxName)
		}
	}

	if aggType == AggregateNone && mutType == MutationNone && len(idx.GetFields()) == 0 {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s must specify at least one field", msgName)
	}

	var aggField FieldSpec
	switch aggType {
	case AggregateNone, AggregateCount:
		if idx.GetAggregateField() != "" {
			if mutType != MutationNone {
				return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s: aggregate_field cannot be set on a mutation index", msgName)
			}
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s: aggregate_field cannot be set for %v", msgName, idx.GetAggregate())
		}
	case AggregateSum, AggregateMin, AggregateMax:
		aggFieldName := idx.GetAggregateField()
		if aggFieldName == "" {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s: aggregate_field is required for %v", msgName, idx.GetAggregate())
		}
		f, ok := fieldMap[aggFieldName]
		if !ok {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index aggregate_field %q not found in message %s", aggFieldName, msgName)
		}
		if f.IsRepeated {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index aggregate_field %q in message %s cannot be repeated", aggFieldName, msgName)
		}
		if !f.Kind.IsInteger() {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index aggregate_field %q in message %s must be an integer type, got %v", aggFieldName, msgName, f.Kind)
		}
		aggField = f
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
		if f.IsVersionstamp {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index field %q in message %s cannot be a versionstamp field", idxFieldName, msgName)
		}
		if !f.Kind.IsValidKeyKind() {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index field %q in message %s has unsupported kind %v", idxFieldName, msgName, f.Kind)
		}
		if mutType != MutationNone && f.IsRepeated {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index field %q in message %s cannot be repeated on a mutation index", idxFieldName, msgName)
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

	spec := SecondaryIndexSpec{
		Fields:         idxFields,
		IsFanOut:       isFanOut,
		FanOutField:    fanOutField,
		Aggregate:      aggType,
		AggregateField: aggField,
		Mutation:       mutType,
		Name:           idxName,
	}
	signature := indexSignature(spec)
	if idx.GetId() == 0 {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index on [%s] in message %s requires an explicit non-zero id", signature, msgName)
	}
	indexID := int64(idx.GetId())
	spec.IndexID = indexID

	if existingSig, ok := usedHashes[indexID]; ok {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index ID collision detected in message %s: indices with fields %q and %q share the same ID %d", msgName, signature, existingSig, indexID)
	}
	if usedHashes != nil {
		usedHashes[indexID] = signature
	}

	return spec, nil
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

// MsgHasVersionstampPK returns true if any primary key field on the message is a versionstamp.
func MsgHasVersionstampPK(msg MessageSpec) bool {
	for _, f := range msg.PrimaryKeyFields {
		if f.IsVersionstamp {
			return true
		}
	}
	return false
}
