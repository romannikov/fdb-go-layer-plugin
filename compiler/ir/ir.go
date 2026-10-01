package ir

import (
	"fmt"
	"hash/fnv"
	"strings"

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

// FieldSpec describes a single Protobuf field in a language-neutral representation.
type FieldSpec struct {
	ProtoName      string       // Original snake_case name in .proto
	GoName         string       // CamelCase name from protogen (for Go backend)
	RustName       string       // snake_case / r#escaped name (for Rust backend)
	Number         int32        // Protobuf field tag number
	Kind           FieldKind
	EnumGoName     string       // If Kind == KindEnum
	EnumRustName   string       // If Kind == KindEnum
	MessageGoName  string       // If Kind == KindMessage
	IsRepeated     bool
	IsUnsigned     bool         // uint32, fixed32, uint64, fixed64
	IsEnum         bool
	IsVersionstamp bool
	Mutation       MutationType
}

// SecondaryIndexSpec describes a secondary index on a message.
type SecondaryIndexSpec struct {
	Fields      []FieldSpec
	IsFanOut    bool
	FanOutField FieldSpec
	IndexID     int64
}

// MessageSpec describes an FDB-persisted Protobuf message or FIFO queue.
type MessageSpec struct {
	Name             string // Protobuf message name (e.g., "User")
	Fields           []FieldSpec
	PrimaryKeyFields []FieldSpec
	SecondaryIndexes []SecondaryIndexSpec
	IsQueue          bool
	GoPackagePath    string
	GoPackageName    string
	FilePrefix       string
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
	msgName := message.GoIdent.GoName

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

	return BuildMessageSpec(msgName, fields, fieldMap, primaryKey, rawIndexes, isQueue)
}

// BuildMessageSpec validates and constructs a MessageSpec from extracted field specs and option values.
func BuildMessageSpec(
	msgName string,
	fields []FieldSpec,
	fieldMap map[string]FieldSpec,
	primaryKey []string,
	rawIndexes []*annotationspb.SecondaryIndex,
	isQueue bool,
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

	if len(primaryKeyFields) == 0 && !isQueue {
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
		enumRustName = string(field.Enum.Desc.Name())
	}

	var msgGoName string
	if protoKind == protoreflect.MessageKind && field.Message != nil && !field.Desc.IsMap() {
		msgGoName = field.Message.GoIdent.GoName
	}

	protoName := string(field.Desc.Name())
	spec := FieldSpec{
		ProtoName:      protoName,
		GoName:         field.GoName,
		RustName:       EscapeRustIdent(protoName),
		Number:         int32(field.Desc.Number()),
		Kind:           kind,
		EnumGoName:     enumGoName,
		EnumRustName:   enumRustName,
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
