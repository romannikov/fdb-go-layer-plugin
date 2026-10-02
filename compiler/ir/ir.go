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
	RustName  string // PascalCase variant name for prost::Enumeration
	Number    int32  // Enum numeric value
}

// EnumSpec describes a Protobuf enum definition.
type EnumSpec struct {
	Name   string          // Protobuf enum name (e.g., "Role")
	Values []EnumValueSpec // Enum values in declaration order
}

// FieldSpec describes a single Protobuf field in a language-neutral representation.
type FieldSpec struct {
	ProtoName       string       // Original snake_case name in .proto
	GoName          string       // CamelCase name from protogen (for Go backend)
	RustName        string       // snake_case / r#escaped name (for Rust backend)
	Number          int32        // Protobuf field tag number
	Kind            FieldKind
	EnumGoName      string       // If Kind == KindEnum
	EnumRustName    string       // If Kind == KindEnum (Rust path from file root, e.g. "Role" or "user::Status")
	MessageGoName   string       // If Kind == KindMessage
	MessageRustName string       // If Kind == KindMessage (Rust path from file root, e.g. "Address" or "user::Profile")
	IsRepeated      bool
	IsUnsigned      bool         // uint32, fixed32, uint64, fixed64
	IsEnum          bool
	IsVersionstamp  bool
	Mutation        MutationType
}

// SecondaryIndexSpec describes a secondary index on a message.
type SecondaryIndexSpec struct {
	Fields      []FieldSpec
	IsFanOut    bool
	FanOutField FieldSpec
	IndexID     int64
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

// ParsePlugin extracts all annotated MessageSpecs from a protogen.Plugin.
func ParsePlugin(plugin *protogen.Plugin) ([]MessageSpec, error) {
	var messages []MessageSpec
	processedMessages := make(map[string]bool)

	for _, file := range plugin.Files {
		if !file.Generate {
			continue
		}
		goPackagePath := string(file.GoImportPath)
		for _, message := range file.Messages {
			msgName := message.GoIdent.GoName
			if processedMessages[msgName] {
				continue
			}
			processedMessages[msgName] = true

			spec, err := ProcessMessage(message, message.Desc.Options())
			if err != nil {
				return nil, err
			}
			if spec != nil {
				spec.GoPackagePath = goPackagePath
				spec.GoPackageName = string(file.GoPackageName)
				spec.FilePrefix = file.GeneratedFilenamePrefix
				messages = append(messages, *spec)
			}
		}
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
	msgName := message.GoIdent.GoName
	if message.Desc != nil {
		if _, isNested := message.Desc.Parent().(protoreflect.MessageDescriptor); isNested || msgName == "" {
			msgName = string(message.Desc.Name())
		}
	}

	fieldMap := make(map[string]FieldSpec, len(message.Fields))
	fields := make([]FieldSpec, 0, len(message.Fields))
	for _, field := range message.Fields {
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
	enumName := string(enum.Desc.Name())
	prefix := strings.ToUpper(toSnakeCase(enumName)) + "_"

	rawNames := make([]string, len(enum.Values))
	numbers := make([]int32, len(enum.Values))
	strippedNames := make([]string, len(enum.Values))
	seen := make(map[string]int)

	for i, v := range enum.Values {
		raw := string(v.Desc.Name())
		rawNames[i] = raw
		numbers[i] = int32(v.Desc.Number())

		candidate := raw
		if strings.HasPrefix(raw, prefix) {
			rem := strings.TrimPrefix(raw, prefix)
			if len(rem) > 0 && unicode.IsLetter(rune(rem[0])) {
				candidate = rem
			}
		}
		pascal := EscapeRustIdent(toPascalCase(candidate))
		strippedNames[i] = pascal
		seen[pascal]++
	}

	values := make([]EnumValueSpec, len(enum.Values))
	for i := range enum.Values {
		rustName := strippedNames[i]
		if seen[rustName] > 1 {
			rustName = EscapeRustIdent(toPascalCase(rawNames[i]))
		}
		values[i] = EnumValueSpec{
			ProtoName: rawNames[i],
			RustName:  rustName,
			Number:    numbers[i],
		}
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
	for _, f := range fields {
		if err := ValidateFieldSpec(f, msgName); err != nil {
			return nil, err
		}
	}

	primaryKeyFields := make([]FieldSpec, 0, len(primaryKey))
	for _, pkName := range primaryKey {
		f, ok := fieldMap[pkName]
		if !ok {
			return nil, fmt.Errorf("primary key field %q not found in message %s", pkName, msgName)
		}
		primaryKeyFields = append(primaryKeyFields, f)
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

	if !allowUnannotated && len(primaryKeyFields) == 0 && !isQueue {
		return nil, nil
	}

	if isQueue {
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

	var enumGoName, enumRustName string
	if protoKind == protoreflect.EnumKind && field.Enum != nil {
		enumGoName = field.Enum.GoIdent.GoName
		enumRustName = rustPathFromFileRoot(field.Enum.Desc)
	}

	var msgGoName, msgRustName string
	if protoKind == protoreflect.MessageKind && field.Message != nil && !field.Desc.IsMap() {
		msgGoName = field.Message.GoIdent.GoName
		msgRustName = rustPathFromFileRoot(field.Message.Desc)
	}

	protoName := string(field.Desc.Name())
	spec := FieldSpec{
		ProtoName:       protoName,
		GoName:          field.GoName,
		RustName:        EscapeRustIdent(protoName),
		Number:          int32(field.Desc.Number()),
		Kind:            kind,
		EnumGoName:      enumGoName,
		EnumRustName:    enumRustName,
		MessageGoName:   msgGoName,
		MessageRustName: msgRustName,
		IsRepeated:      field.Desc.IsList(),
		IsUnsigned:      kind == KindUint32 || kind == KindFixed32 || kind == KindUint64 || kind == KindFixed64,
		IsEnum:          kind == KindEnum,
		IsVersionstamp:  isVersionstamp,
		Mutation:        mutation,
	}
	if err := ValidateFieldSpec(spec, msgName); err != nil {
		return FieldSpec{}, err
	}
	return spec, nil
}

func rustPathFromFileRoot(desc protoreflect.Descriptor) string {
	if desc == nil {
		return ""
	}
	parts := []string{string(desc.Name())}
	for p := desc.Parent(); p != nil; p = p.Parent() {
		if msgDesc, ok := p.(protoreflect.MessageDescriptor); ok {
			parts = append([]string{EscapeRustIdent(toSnakeCase(string(msgDesc.Name())))}, parts...)
		} else {
			break
		}
	}
	return strings.Join(parts, "::")
}

func toSnakeCase(s string) string {
	if s == "" {
		return ""
	}
	var out strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 && runes[i-1] != '_' && (unicode.IsLower(runes[i-1]) || (i+1 < len(runes) && unicode.IsLower(runes[i+1]))) {
				out.WriteByte('_')
			}
			out.WriteRune(unicode.ToLower(r))
		} else {
			out.WriteRune(r)
		}
	}
	return out.String()
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

// BuildSecondaryIndex validates and constructs a SecondaryIndexSpec.
func BuildSecondaryIndex(
	idx *annotationspb.SecondaryIndex,
	fieldMap map[string]FieldSpec,
	usedHashes map[int64]string,
	msgName string,
) (SecondaryIndexSpec, error) {
	idxFields := make([]FieldSpec, 0, len(idx.GetFields()))
	for _, idxFieldName := range idx.GetFields() {
		f, ok := fieldMap[idxFieldName]
		if !ok {
			return SecondaryIndexSpec{}, fmt.Errorf("secondary index field %q not found in message %s", idxFieldName, msgName)
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

	fieldNames := make([]string, 0, len(idxFields))
	for _, f := range idxFields {
		name := f.GoName
		if name == "" {
			name = f.ProtoName
		}
		fieldNames = append(fieldNames, strings.ToLower(name))
	}
	signature := strings.Join(fieldNames, ",")

	var indexID int64
	if idx.GetId() != 0 {
		indexID = int64(idx.GetId())
	} else {
		indexID = ComputeIndexID(signature)
	}

	if existingSig, ok := usedHashes[indexID]; ok {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index ID collision detected in message %s: indices with fields %q and %q share the same ID %d", msgName, signature, existingSig, indexID)
	}
	usedHashes[indexID] = signature

	return SecondaryIndexSpec{
		Fields:      idxFields,
		IsFanOut:    isFanOut,
		FanOutField: fanOutField,
		IndexID:     indexID,
	}, nil
}

// ComputeIndexID computes the 32-bit FNV-1a hash of the lowercase comma-joined field signature as int64.
func ComputeIndexID(signature string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(signature))
	return int64(h.Sum32())
}

// ReorderQueuePrimaryKeyFields reorders queue primary keys when len > 2 to:
// [pk[0] (queue_name), versionstamp_fields..., remaining_tie_breaker_fields...].
func ReorderQueuePrimaryKeyFields(pkFields []FieldSpec) []FieldSpec {
	if len(pkFields) <= 2 {
		return pkFields
	}
	reordered := make([]FieldSpec, 0, len(pkFields))
	reordered = append(reordered, pkFields[0])
	for _, f := range pkFields[1:] {
		if f.IsVersionstamp {
			reordered = append(reordered, f)
		}
	}
	for _, f := range pkFields[1:] {
		if !f.IsVersionstamp {
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

var rustKeywords = map[string]bool{
	"as": true, "break": true, "const": true, "continue": true, "crate": true,
	"else": true, "enum": true, "extern": true, "false": true, "fn": true,
	"for": true, "if": true, "impl": true, "in": true, "let": true,
	"loop": true, "match": true, "mod": true, "move": true, "mut": true,
	"pub": true, "ref": true, "return": true, "self": true, "Self": true,
	"static": true, "struct": true, "super": true, "trait": true, "true": true,
	"type": true, "unsafe": true, "use": true, "where": true, "while": true,
	"async": true, "await": true, "dyn": true,
}

// EscapeRustIdent escapes Rust reserved keywords with r# matching prost's field naming.
func EscapeRustIdent(ident string) string {
	if rustKeywords[ident] {
		return "r#" + ident
	}
	return ident
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
