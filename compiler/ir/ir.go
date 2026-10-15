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

// IndexAggregateType represents a pre-aggregated secondary index type in AggregateNamespace.
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

// IndexPrefixSpec describes a non-colliding leading prefix of a composite key or secondary index.
type IndexPrefixSpec struct {
	Fields []FieldSpec
}

// IndexRangeScanSpec describes a range scan over a key or secondary index where
// EqualityFields are matched by equality and RangeField is bounded by KeyRange[K].
type IndexRangeScanSpec struct {
	EqualityFields []FieldSpec
	RangeField     FieldSpec
}

// AllFields returns EqualityFields followed by RangeField.
func (r IndexRangeScanSpec) AllFields() []FieldSpec {
	out := make([]FieldSpec, 0, len(r.EqualityFields)+1)
	out = append(out, r.EqualityFields...)
	out = append(out, r.RangeField)
	return out
}

// SecondaryIndexSpec describes a secondary or aggregate index on a message.
type SecondaryIndexSpec struct {
	Fields            []FieldSpec
	IsFanOut          bool
	FanOutField       FieldSpec
	IndexID           int64
	IsUnique          bool
	IsSparse          bool
	StoringFields     []FieldSpec
	Aggregate         IndexAggregateType
	AggregateField    *FieldSpec
	Prefixes          []IndexPrefixSpec
	RangeScans        []IndexRangeScanSpec
	CoveredPrefixes   []IndexPrefixSpec
	CoveredRangeScans []IndexRangeScanSpec
}

// IsCovering returns true if the index stores projected non-key fields in its value.
func (s SecondaryIndexSpec) IsCovering() bool {
	return len(s.StoringFields) > 0
}

// InterleaveSpec describes how a child message is physically interleaved under a parent message's primary key.
type InterleaveSpec struct {
	ParentName          string
	ParentTypeID        int64
	OnDeleteCascade     bool
	ParentPKFields      []FieldSpec
	ChildPKSuffixFields []FieldSpec
}

// ChildSpec describes a child message interleaved under this parent message.
type ChildSpec struct {
	Name                string
	TypeID              int64
	OnDeleteCascade     bool
	PrimaryKeyFields    []FieldSpec
	ChildPKSuffixFields []FieldSpec
	SecondaryIndexes    []SecondaryIndexSpec
	HasMutationFields   bool
}

// MessageSpec describes an FDB-persisted Protobuf message, FIFO queue, or helper message.
type MessageSpec struct {
	Name                 string // Protobuf message name (e.g., "User")
	TypeID               int64  // Explicit compile-time FDB type prefix (> 0 for annotated messages)
	Fields               []FieldSpec
	PrimaryKeyFields     []FieldSpec
	PrimaryKeyPrefixes   []IndexPrefixSpec
	PrimaryKeyRangeScans []IndexRangeScanSpec
	SecondaryIndexes     []SecondaryIndexSpec
	Interleave           *InterleaveSpec
	Children             []ChildSpec
	NestedMessages       []MessageSpec
	NestedEnums          []EnumSpec
	IsQueue              bool
	GoPackagePath        string
	GoPackageName        string
	FilePrefix           string
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
			return fmt.Errorf("message %s must specify a non-zero (annotations.type_id)", m.Name)
		}
		if existingMsg, ok := usedTypeIDs[m.TypeID]; ok {
			return fmt.Errorf("message type ID collision detected: messages %q and %q share the same type ID %d", m.Name, existingMsg, m.TypeID)
		}
		usedTypeIDs[m.TypeID] = m.Name
	}
	return nil
}

// ResolveInterleave validates and links parent-child interleave relationships across a set of messages.
func ResolveInterleave(messages []MessageSpec) ([]MessageSpec, error) {
	if len(messages) == 0 {
		return messages, nil
	}
	out := make([]MessageSpec, len(messages))
	copy(out, messages)

	byName := make(map[string]int, len(out))
	for i, m := range out {
		if m.HasRepository() {
			byName[m.Name] = i
		}
		out[i].Children = nil
	}

	for i := range out {
		child := &out[i]
		if child.Interleave == nil {
			continue
		}
		if child.IsQueue {
			return nil, fmt.Errorf("queue message %s cannot be interleaved", child.Name)
		}
		if MsgHasVersionstampPK(*child) {
			return nil, fmt.Errorf("interleaved message %s cannot have a versionstamp primary key", child.Name)
		}
		parentIdx, ok := byName[child.Interleave.ParentName]
		if !ok {
			return nil, fmt.Errorf("interleave parent %q for message %s not found", child.Interleave.ParentName, child.Name)
		}
		if parentIdx == i {
			return nil, fmt.Errorf("message %s cannot interleave under itself", child.Name)
		}
		parent := &out[parentIdx]
		if parent.IsQueue || len(parent.PrimaryKeyFields) == 0 {
			return nil, fmt.Errorf("interleave parent %q for message %s must be an entity message with a primary_key", parent.Name, child.Name)
		}
		if MsgHasVersionstampPK(*parent) {
			return nil, fmt.Errorf("interleave parent %q for message %s cannot have a versionstamp primary key", parent.Name, child.Name)
		}
		if parent.Interleave != nil {
			return nil, fmt.Errorf("multi-level interleave is not supported: parent %q of message %s is itself interleaved", parent.Name, child.Name)
		}
		if len(child.PrimaryKeyFields) <= len(parent.PrimaryKeyFields) {
			return nil, fmt.Errorf("interleaved child message %s must have all %d primary key fields of parent %s plus at least one child primary key field", child.Name, len(parent.PrimaryKeyFields), parent.Name)
		}
		for j, pField := range parent.PrimaryKeyFields {
			cField := child.PrimaryKeyFields[j]
			if cField.ProtoName != pField.ProtoName || cField.Kind != pField.Kind {
				return nil, fmt.Errorf("interleaved child message %s primary key field %d (%s: %v) must match parent %s primary key field (%s: %v)", child.Name, j, cField.ProtoName, cField.Kind, parent.Name, pField.ProtoName, pField.Kind)
			}
		}

		parentPK := make([]FieldSpec, len(parent.PrimaryKeyFields))
		copy(parentPK, child.PrimaryKeyFields[:len(parent.PrimaryKeyFields)])
		suffixPK := make([]FieldSpec, len(child.PrimaryKeyFields)-len(parent.PrimaryKeyFields))
		copy(suffixPK, child.PrimaryKeyFields[len(parent.PrimaryKeyFields):])

		interleaveCopy := *child.Interleave
		interleaveCopy.ParentTypeID = parent.TypeID
		interleaveCopy.ParentPKFields = parentPK
		interleaveCopy.ChildPKSuffixFields = suffixPK
		child.Interleave = &interleaveCopy

		parent.Children = append(parent.Children, ChildSpec{
			Name:                child.Name,
			TypeID:              child.TypeID,
			OnDeleteCascade:     interleaveCopy.OnDeleteCascade,
			PrimaryKeyFields:    child.PrimaryKeyFields,
			ChildPKSuffixFields: suffixPK,
			SecondaryIndexes:    child.SecondaryIndexes,
			HasMutationFields:   HasMutationFields(*child),
		})
	}

	return out, nil
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

	if err := ValidateUniqueTypeIDs(spec.Messages); err != nil {
		return nil, err
	}
	resolved, err := ResolveInterleave(spec.Messages)
	if err != nil {
		return nil, err
	}
	spec.Messages = resolved

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
	resolved, err := ResolveInterleave(messages)
	if err != nil {
		return nil, err
	}
	return resolved, nil
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

	if msgOptions != nil && proto.HasExtension(msgOptions, annotationspb.E_SplitLargeRecords) {
		if val, ok := proto.GetExtension(msgOptions, annotationspb.E_SplitLargeRecords).(bool); ok && val {
			return nil, fmt.Errorf("split_large_records in message %s is reserved and not implemented yet", msgName)
		}
	}
	if msgOptions != nil && proto.HasExtension(msgOptions, annotationspb.E_TtlField) {
		if val, ok := proto.GetExtension(msgOptions, annotationspb.E_TtlField).(string); ok && val != "" {
			return nil, fmt.Errorf("ttl_field in message %s is reserved and not implemented yet", msgName)
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

	var rawInterleave *annotationspb.InterleaveSpec
	if msgOptions != nil && proto.HasExtension(msgOptions, annotationspb.E_Interleave) {
		val := proto.GetExtension(msgOptions, annotationspb.E_Interleave)
		if val != nil {
			switch v := val.(type) {
			case *annotationspb.InterleaveSpec:
				if v != nil && v.GetParent() != "" {
					rawInterleave = v
				}
			default:
				return nil, fmt.Errorf("unknown type for interleave in message %s: %T", msgName, v)
			}
		}
	}

	spec, err := buildMessageSpecInternal(msgName, fields, fieldMap, primaryKey, rawIndexes, isQueue, explicitTypeID, rawInterleave, true)
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

// BuildMessageSpec validates and constructs a MessageSpec without an explicit type ID.
// Because a non-zero type_id is required on all annotated messages, this returns an error
// for annotated messages unless BuildMessageSpecWithTypeID is used with explicitTypeID > 0.
func BuildMessageSpec(
	msgName string,
	fields []FieldSpec,
	fieldMap map[string]FieldSpec,
	primaryKey []string,
	rawIndexes []*annotationspb.SecondaryIndex,
	isQueue bool,
) (*MessageSpec, error) {
	return buildMessageSpecInternal(msgName, fields, fieldMap, primaryKey, rawIndexes, isQueue, 0, nil, false)
}

// BuildMessageSpecWithTypeID validates and constructs a MessageSpec with an explicit non-zero type ID.
func BuildMessageSpecWithTypeID(
	msgName string,
	fields []FieldSpec,
	fieldMap map[string]FieldSpec,
	primaryKey []string,
	rawIndexes []*annotationspb.SecondaryIndex,
	isQueue bool,
	explicitTypeID uint32,
) (*MessageSpec, error) {
	return buildMessageSpecInternal(msgName, fields, fieldMap, primaryKey, rawIndexes, isQueue, explicitTypeID, nil, false)
}

// BuildMessageSpecFull validates and constructs a MessageSpec with an explicit type ID and optional interleave spec.
func BuildMessageSpecFull(
	msgName string,
	fields []FieldSpec,
	fieldMap map[string]FieldSpec,
	primaryKey []string,
	rawIndexes []*annotationspb.SecondaryIndex,
	isQueue bool,
	explicitTypeID uint32,
	rawInterleave *annotationspb.InterleaveSpec,
) (*MessageSpec, error) {
	return buildMessageSpecInternal(msgName, fields, fieldMap, primaryKey, rawIndexes, isQueue, explicitTypeID, rawInterleave, false)
}

func buildMessageSpecInternal(
	msgName string,
	fields []FieldSpec,
	fieldMap map[string]FieldSpec,
	primaryKey []string,
	rawIndexes []*annotationspb.SecondaryIndex,
	isQueue bool,
	explicitTypeID uint32,
	rawInterleave *annotationspb.InterleaveSpec,
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

	hasVS := MsgHasVersionstampPK(MessageSpec{PrimaryKeyFields: primaryKeyFields})

	usedHashes := make(map[int64]string)
	secondaryIndexes := make([]SecondaryIndexSpec, 0, len(rawIndexes))
	for _, idx := range rawIndexes {
		si, err := BuildSecondaryIndexWithPK(idx, fieldMap, pkSet, usedHashes, msgName)
		if err != nil {
			return nil, err
		}
		if hasVS && si.IsUnique {
			return nil, fmt.Errorf("unique secondary index is not supported on message %s with versionstamp primary key", msgName)
		}
		if hasVS && si.Aggregate != AggregateNone {
			return nil, fmt.Errorf("aggregate secondary index is not supported on message %s with versionstamp primary key", msgName)
		}
		secondaryIndexes = append(secondaryIndexes, si)
	}

	if !allowUnannotated && len(primaryKeyFields) == 0 && !isQueue {
		return nil, nil
	}

	if isQueue {
		if !hasVS {
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
			return nil, fmt.Errorf("message %s must specify a non-zero (annotations.type_id)", msgName)
		}
		typeID = int64(explicitTypeID)
	}

	var interleave *InterleaveSpec
	if rawInterleave != nil && rawInterleave.GetParent() != "" {
		interleave = &InterleaveSpec{
			ParentName:      rawInterleave.GetParent(),
			OnDeleteCascade: rawInterleave.GetOnDeleteCascade(),
		}
	}

	pkPrefixes, pkRangeScans := PopulatePrimaryKeyScans(primaryKeyFields, isQueue, hasVS)
	secondaryIndexes = PopulateIndexPrefixesWithPK(secondaryIndexes, primaryKeyFields, isQueue, hasVS)

	return &MessageSpec{
		Name:                 msgName,
		TypeID:               typeID,
		Fields:               fields,
		PrimaryKeyFields:     primaryKeyFields,
		PrimaryKeyPrefixes:   pkPrefixes,
		PrimaryKeyRangeScans: pkRangeScans,
		SecondaryIndexes:     secondaryIndexes,
		Interleave:           interleave,
		IsQueue:              isQueue,
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
	return BuildSecondaryIndexWithPK(idx, fieldMap, nil, usedHashes, msgName)
}

// BuildSecondaryIndexWithPK validates and constructs a SecondaryIndexSpec with primary key awareness for storing fields.
func BuildSecondaryIndexWithPK(
	idx *annotationspb.SecondaryIndex,
	fieldMap map[string]FieldSpec,
	pkSet map[string]bool,
	usedHashes map[int64]string,
	msgName string,
) (SecondaryIndexSpec, error) {
	if idx == nil || len(idx.GetFields()) == 0 {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s must specify at least one field", msgName)
	}
	if idx.GetId() == 0 {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s must specify a non-zero id", msgName)
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

	isUnique := idx.GetUnique()
	if isUnique && isFanOut {
		return SecondaryIndexSpec{}, fmt.Errorf("unique secondary index in message %s cannot index repeated field %q", msgName, fanOutField.ProtoName)
	}
	isSparse := idx.GetSparse()

	var aggregate IndexAggregateType
	switch idx.GetAggregate() {
	case annotationspb.IndexAggregateType_AGGREGATE_COUNT:
		aggregate = AggregateCount
	case annotationspb.IndexAggregateType_AGGREGATE_SUM:
		aggregate = AggregateSum
	case annotationspb.IndexAggregateType_AGGREGATE_MIN:
		aggregate = AggregateMin
	case annotationspb.IndexAggregateType_AGGREGATE_MAX:
		aggregate = AggregateMax
	default:
		aggregate = AggregateNone
	}

	if aggregate == AggregateNone && idx.GetAggregateField() != "" {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index in message %s specifies aggregate_field %q without an aggregate type", msgName, idx.GetAggregateField())
	}

	var aggregateField *FieldSpec
	if aggregate != AggregateNone {
		if isUnique {
			return SecondaryIndexSpec{}, fmt.Errorf("aggregate secondary index in message %s cannot be unique", msgName)
		}
		if isFanOut {
			return SecondaryIndexSpec{}, fmt.Errorf("aggregate secondary index in message %s cannot index repeated fields", msgName)
		}
		if len(idx.GetStoring()) > 0 {
			return SecondaryIndexSpec{}, fmt.Errorf("aggregate secondary index in message %s cannot specify storing fields", msgName)
		}
		if aggregate == AggregateCount {
			if idx.GetAggregateField() != "" {
				return SecondaryIndexSpec{}, fmt.Errorf("COUNT aggregate index in message %s must not specify aggregate_field", msgName)
			}
		} else {
			aggName := idx.GetAggregateField()
			if aggName == "" {
				return SecondaryIndexSpec{}, fmt.Errorf("aggregate index in message %s must specify aggregate_field", msgName)
			}
			af, ok := fieldMap[aggName]
			if !ok {
				return SecondaryIndexSpec{}, fmt.Errorf("aggregate_field %q not found in message %s", aggName, msgName)
			}
			if af.IsRepeated {
				return SecondaryIndexSpec{}, fmt.Errorf("aggregate_field %q in message %s cannot be repeated", aggName, msgName)
			}
			if !af.Kind.IsInteger() {
				return SecondaryIndexSpec{}, fmt.Errorf("aggregate_field %q in message %s must be an integer type", aggName, msgName)
			}
			if af.Mutation != MutationNone {
				return SecondaryIndexSpec{}, fmt.Errorf("aggregate_field %q in message %s cannot have an atomic mutation", aggName, msgName)
			}
			afCopy := af
			aggregateField = &afCopy
		}
	}

	var storingFields []FieldSpec
	if len(idx.GetStoring()) > 0 {
		seenStoring := make(map[string]bool, len(idx.GetStoring()))
		for _, storingName := range idx.GetStoring() {
			if seenStoring[storingName] {
				return SecondaryIndexSpec{}, fmt.Errorf("duplicate storing field %q in secondary index of message %s", storingName, msgName)
			}
			seenStoring[storingName] = true
			if seenFields[storingName] || (pkSet != nil && pkSet[storingName]) {
				return SecondaryIndexSpec{}, fmt.Errorf("storing field %q in message %s is already part of the index key or primary key", storingName, msgName)
			}
			sf, ok := fieldMap[storingName]
			if !ok {
				return SecondaryIndexSpec{}, fmt.Errorf("storing field %q not found in message %s", storingName, msgName)
			}
			if sf.Mutation != MutationNone {
				return SecondaryIndexSpec{}, fmt.Errorf("storing field %q in message %s cannot have an atomic mutation", storingName, msgName)
			}
			storingFields = append(storingFields, sf)
		}
	}

	signature := fieldsSignature(idxFields)
	indexID := int64(idx.GetId())

	if existingSig, ok := usedHashes[indexID]; ok {
		return SecondaryIndexSpec{}, fmt.Errorf("secondary index ID collision detected in message %s: indices with fields %q and %q share the same ID %d", msgName, signature, existingSig, indexID)
	}
	if usedHashes != nil {
		usedHashes[indexID] = signature
	}

	return SecondaryIndexSpec{
		Fields:         idxFields,
		IsFanOut:       isFanOut,
		FanOutField:    fanOutField,
		IndexID:        indexID,
		IsUnique:       isUnique,
		IsSparse:       isSparse,
		StoringFields:  storingFields,
		Aggregate:      aggregate,
		AggregateField: aggregateField,
	}, nil
}

// PopulatePrimaryKeyScans returns leading prefix specs (1 <= k < len(pkFields)) for ListBy*
// and trailing-field range scan specs (1 <= k <= len(pkFields)) for ScanBy* on primary keys.
func PopulatePrimaryKeyScans(pkFields []FieldSpec, isQueue bool, hasVS bool) ([]IndexPrefixSpec, []IndexRangeScanSpec) {
	if isQueue || hasVS || len(pkFields) == 0 {
		return nil, nil
	}
	var prefixes []IndexPrefixSpec
	for k := 1; k < len(pkFields); k++ {
		copied := make([]FieldSpec, k)
		copy(copied, pkFields[:k])
		prefixes = append(prefixes, IndexPrefixSpec{Fields: copied})
	}
	rangeScans := make([]IndexRangeScanSpec, 0, len(pkFields))
	for k := 0; k < len(pkFields); k++ {
		var eq []FieldSpec
		if k > 0 {
			eq = make([]FieldSpec, k)
			copy(eq, pkFields[:k])
		}
		rangeScans = append(rangeScans, IndexRangeScanSpec{
			EqualityFields: eq,
			RangeField:     pkFields[k],
		})
	}
	return prefixes, rangeScans
}

// PopulateIndexPrefixes populates non-colliding leading prefix specs and range scan specs
// for each non-fan-out, non-aggregate secondary index on a message.
func PopulateIndexPrefixes(indexes []SecondaryIndexSpec) []SecondaryIndexSpec {
	return PopulateIndexPrefixesWithPK(indexes, nil, false, false)
}

// PopulateIndexPrefixesWithPK populates prefixes, range scans, and covering scan specs while avoiding
// method signature collisions with primary key range scans or other secondary indexes.
func PopulateIndexPrefixesWithPK(indexes []SecondaryIndexSpec, pkFields []FieldSpec, isQueue bool, hasVS bool) []SecondaryIndexSpec {
	if len(indexes) == 0 {
		return indexes
	}
	out := make([]SecondaryIndexSpec, len(indexes))
	copy(out, indexes)

	claimedGetBy := make(map[string]bool, len(out)*2)
	claimedCoveredGetBy := make(map[string]bool, len(out)*2)
	for _, idx := range out {
		if idx.Aggregate != AggregateNone {
			continue
		}
		sig := fieldsSignature(idx.Fields)
		claimedGetBy[sig] = true
		if idx.IsCovering() {
			claimedCoveredGetBy[sig] = true
		}
	}

	claimedScanBy := make(map[string]bool, len(out)*2+len(pkFields))
	if !isQueue && !hasVS {
		for k := 1; k <= len(pkFields); k++ {
			claimedScanBy[fieldsSignature(pkFields[:k])] = true
		}
	}
	claimedCoveredScanBy := make(map[string]bool, len(out)*2)

	for i := range out {
		idx := &out[i]
		idx.Prefixes = nil
		idx.RangeScans = nil
		idx.CoveredPrefixes = nil
		idx.CoveredRangeScans = nil
		if idx.Aggregate != AggregateNone || idx.IsFanOut {
			continue
		}
		for k := 1; k < len(idx.Fields); k++ {
			prefixFields := idx.Fields[:k]
			sig := fieldsSignature(prefixFields)
			if !claimedGetBy[sig] {
				claimedGetBy[sig] = true
				copied := make([]FieldSpec, k)
				copy(copied, prefixFields)
				idx.Prefixes = append(idx.Prefixes, IndexPrefixSpec{Fields: copied})
			}
			if idx.IsCovering() && !claimedCoveredGetBy[sig] {
				claimedCoveredGetBy[sig] = true
				copied := make([]FieldSpec, k)
				copy(copied, prefixFields)
				idx.CoveredPrefixes = append(idx.CoveredPrefixes, IndexPrefixSpec{Fields: copied})
			}
		}
		for k := 0; k < len(idx.Fields); k++ {
			scanFields := idx.Fields[:k+1]
			sig := fieldsSignature(scanFields)
			var eq []FieldSpec
			if k > 0 {
				eq = make([]FieldSpec, k)
				copy(eq, idx.Fields[:k])
			}
			spec := IndexRangeScanSpec{
				EqualityFields: eq,
				RangeField:     idx.Fields[k],
			}
			if !claimedScanBy[sig] {
				claimedScanBy[sig] = true
				idx.RangeScans = append(idx.RangeScans, spec)
			}
			if idx.IsCovering() && !claimedCoveredScanBy[sig] {
				claimedCoveredScanBy[sig] = true
				idx.CoveredRangeScans = append(idx.CoveredRangeScans, spec)
			}
		}
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
	if target.IsFanOut || target.Aggregate != AggregateNone || len(target.Fields) <= 1 {
		return nil
	}
	if len(target.Prefixes) > 0 {
		return target.Prefixes
	}
	populated := PopulateIndexPrefixes(indexes)
	return populated[idxPos].Prefixes
}

// EffectiveIndexRangeScans returns the non-colliding range scan specs for indexes[idxPos].
func EffectiveIndexRangeScans(msg MessageSpec, idxPos int) []IndexRangeScanSpec {
	if idxPos < 0 || idxPos >= len(msg.SecondaryIndexes) {
		return nil
	}
	target := msg.SecondaryIndexes[idxPos]
	if target.IsFanOut || target.Aggregate != AggregateNone {
		return nil
	}
	if len(target.RangeScans) > 0 {
		return target.RangeScans
	}
	populated := PopulateIndexPrefixesWithPK(msg.SecondaryIndexes, msg.PrimaryKeyFields, msg.IsQueue, MsgHasVersionstampPK(msg))
	return populated[idxPos].RangeScans
}

// ComputeTypeID computes the 32-bit FNV-1a hash of msgName as int64.
func ComputeTypeID(msgName string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(msgName))
	return int64(h.Sum32())
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

// HasStandardOrUniqueIndexes returns true if the message has any non-aggregate secondary index.
func HasStandardOrUniqueIndexes(msg MessageSpec) bool {
	for _, idx := range msg.SecondaryIndexes {
		if idx.Aggregate == AggregateNone {
			return true
		}
	}
	return false
}

// HasAggregateIndexes returns true if the message has any aggregate secondary index.
func HasAggregateIndexes(msg MessageSpec) bool {
	for _, idx := range msg.SecondaryIndexes {
		if idx.Aggregate != AggregateNone {
			return true
		}
	}
	return false
}

// NeedsOldRecordOnSet returns true if Set/Upsert/Delete needs to read the existing record
// (i.e. when the message has any secondary or aggregate index).
func NeedsOldRecordOnSet(msg MessageSpec) bool {
	return len(msg.SecondaryIndexes) > 0
}
