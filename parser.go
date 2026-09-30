package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	annotationspb "github.com/romannikov/fdb-go-layer-plugin/fdb-layer"
)

type Field struct {
	Name            string
	Type            string
	IsRepeated      bool
	Mutation        annotationspb.MutationType
	MutationFDBType string
	MutationValue   int
	Number          int32
	IsVersionstamp  bool
	IsUnsigned      bool
	IsEnum          bool
}

type SecondaryIndex struct {
	Fields      []Field
	IsFanOut    bool
	FanOutField Field
	IndexID     int64
}

type Message struct {
	Name             string
	Fields           []Field
	PrimaryKeyFields []Field
	SecondaryIndexes []SecondaryIndex
	GoPackagePath    string
	GoPackageName    string
	FilePrefix       string
	IsQueue          bool
}

func ProcessMessage(message *protogen.Message, msgOptions proto.Message) *Message {
	primaryKeyFields := []Field{}
	secondaryIndexes := []SecondaryIndex{}

	msgName := message.GoIdent.GoName

	fieldMap := make(map[string]Field, len(message.Fields))
	fields := make([]Field, 0, len(message.Fields))
	for _, field := range message.Fields {
		fieldName := string(field.Desc.Name())
		f := extractField(field)
		fieldMap[fieldName] = f
		fields = append(fields, f)
	}

	var primaryKey []string
	if proto.HasExtension(msgOptions, annotationspb.E_PrimaryKey) {
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
				log.Fatalf("Unknown type for primary_key: %T", v)
			}
		}
	}

	for _, pkName := range primaryKey {
		f, ok := fieldMap[pkName]
		if !ok {
			log.Fatalf("Primary key field %s not found in message %s", pkName, msgName)
		}
		primaryKeyFields = append(primaryKeyFields, f)
	}

	usedHashes := make(map[int64]string)

	if proto.HasExtension(msgOptions, annotationspb.E_SecondaryIndex) {
		siValues := proto.GetExtension(msgOptions, annotationspb.E_SecondaryIndex)
		if siValues != nil {
			var rawIndexes []*annotationspb.SecondaryIndex
			switch v := siValues.(type) {
			case []*annotationspb.SecondaryIndex:
				rawIndexes = v
			case *annotationspb.SecondaryIndex:
				rawIndexes = []*annotationspb.SecondaryIndex{v}
			default:
				log.Fatalf("Unknown type for secondary_index: %T", v)
			}

			for _, idx := range rawIndexes {
				secondaryIndexes = append(secondaryIndexes, buildSecondaryIndex(idx, fieldMap, usedHashes, msgName))
			}
		}
	}

	var isQueue bool
	if proto.HasExtension(msgOptions, annotationspb.E_IsQueue) {
		val := proto.GetExtension(msgOptions, annotationspb.E_IsQueue)
		if val != nil {
			isQueue = val.(bool)
		}
	}

	if len(primaryKeyFields) == 0 && !isQueue {
		return nil
	}

	if isQueue {
		primaryKeyFields = ReorderQueuePrimaryKeyFields(primaryKeyFields)
	}

	return &Message{
		Name:             msgName,
		Fields:           fields,
		PrimaryKeyFields: primaryKeyFields,
		SecondaryIndexes: secondaryIndexes,
		IsQueue:          isQueue,
	}
}

func extractField(field *protogen.Field) Field {
	fieldOptions := field.Desc.Options()
	var mutation annotationspb.MutationType
	var mutationFDBType string
	var mutationValue int
	if proto.HasExtension(fieldOptions, annotationspb.E_Mutation) {
		m := proto.GetExtension(fieldOptions, annotationspb.E_Mutation)
		if m != nil {
			mutation = m.(annotationspb.MutationType)
			switch mutation {
			case annotationspb.MutationType_MUTATION_ADD:
				mutationFDBType = "fdb.MutationTypeAdd"
				mutationValue = 2
			case annotationspb.MutationType_MUTATION_MAX:
				mutationFDBType = "fdb.MutationTypeMax"
				mutationValue = 12
			case annotationspb.MutationType_MUTATION_MIN:
				mutationFDBType = "fdb.MutationTypeMin"
				mutationValue = 13
			}
		}
	}

	var isVersionstamp bool
	if proto.HasExtension(fieldOptions, annotationspb.E_IsVersionstamp) {
		val := proto.GetExtension(fieldOptions, annotationspb.E_IsVersionstamp)
		if val != nil {
			isVersionstamp = val.(bool)
		}
	}

	kind := field.Desc.Kind()
	return Field{
		Name:            field.GoName,
		Type:            FieldGoType(field),
		IsRepeated:      field.Desc.IsList(),
		Mutation:        mutation,
		MutationFDBType: mutationFDBType,
		MutationValue:   mutationValue,
		Number:          int32(field.Desc.Number()),
		IsVersionstamp:  isVersionstamp,
		IsUnsigned:      kind == protoreflect.Uint32Kind || kind == protoreflect.Uint64Kind || kind == protoreflect.Fixed32Kind || kind == protoreflect.Fixed64Kind,
		IsEnum:          kind == protoreflect.EnumKind,
	}
}

func buildSecondaryIndex(idx *annotationspb.SecondaryIndex, fieldMap map[string]Field, usedHashes map[int64]string, msgName string) SecondaryIndex {
	idxFields := make([]Field, 0, len(idx.Fields))
	for _, idxFieldName := range idx.Fields {
		f, ok := fieldMap[idxFieldName]
		if !ok {
			log.Fatalf("Secondary index field %s not found in message %s", idxFieldName, msgName)
		}
		idxFields = append(idxFields, f)
	}

	isFanOut := false
	var fanOutField Field
	for _, f := range idxFields {
		if f.IsRepeated {
			if isFanOut {
				log.Fatalf("Multiple repeated fields in secondary index are not supported in message %s", msgName)
			}
			isFanOut = true
			fanOutField = f
		}
	}

	h := fnv.New32a()
	fieldNames := make([]string, 0, len(idxFields))
	for _, f := range idxFields {
		fieldNames = append(fieldNames, strings.ToLower(f.Name))
	}
	signature := strings.Join(fieldNames, ",")
	h.Write([]byte(signature))
	indexID := int64(h.Sum32())

	if existingSig, ok := usedHashes[indexID]; ok {
		log.Fatalf("Secondary index ID collision detected in message %s: indices with fields %s and %s share the same hash %d", msgName, signature, existingSig, indexID)
	}
	usedHashes[indexID] = signature

	return SecondaryIndex{
		Fields:      idxFields,
		IsFanOut:    isFanOut,
		FanOutField: fanOutField,
		IndexID:     indexID,
	}
}

func ReorderQueuePrimaryKeyFields(pkFields []Field) []Field {
	if len(pkFields) <= 2 {
		return pkFields
	}
	reordered := make([]Field, 0, len(pkFields))
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

func FieldGoType(field *protogen.Field) string {
	if field.Desc.Kind() == protoreflect.EnumKind && field.Enum != nil {
		return field.Enum.GoIdent.GoName
	}
	if field.Desc.Kind() == protoreflect.MessageKind && field.Message != nil && !field.Desc.IsMap() {
		return "*" + field.Message.GoIdent.GoName
	}
	return GoType(field.Desc.Kind())
}

func GoType(kind protoreflect.Kind) string {
	switch kind {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return "int32"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "uint32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return "int64"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "uint64"
	case protoreflect.FloatKind:
		return "float32"
	case protoreflect.DoubleKind:
		return "float64"
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.BytesKind:
		return "[]byte"
	default:
		return "interface{}"
	}
}

func JoinFieldNames(fields []Field) string {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.Name)
	}
	return strings.Join(names, "And")
}

func HasMutationFields(msg Message) bool {
	for _, f := range msg.Fields {
		if f.MutationValue > 0 {
			return true
		}
	}
	return false
}

func HasBinaryImports(msg Message) bool {
	return HasMutationFields(msg)
}

func HasBytesImports(msg Message) bool {
	for _, idx := range msg.SecondaryIndexes {
		for _, f := range idx.Fields {
			if f.Type == "[]byte" && !f.IsRepeated {
				return true
			}
		}
	}
	return false
}

func PackField(name string, f Field) string {
	if f.IsUnsigned || f.Type == "uint32" || f.Type == "uint64" {
		return "uint64(" + name + ")"
	}
	if f.IsEnum || f.Type == "int32" || f.Type == "int64" {
		return "int64(" + name + ")"
	}
	return name
}

func FieldNotEqual(lhs, rhs string, f Field) string {
	if f.Type == "[]byte" {
		return fmt.Sprintf("!bytes.Equal(%s, %s)", lhs, rhs)
	}
	return fmt.Sprintf("%s != %s", lhs, rhs)
}

func FieldEqual(lhs, rhs string, f Field) string {
	if f.Type == "[]byte" {
		return fmt.Sprintf("bytes.Equal(%s, %s)", lhs, rhs)
	}
	return fmt.Sprintf("%s == %s", lhs, rhs)
}

func HasNonRepeatedFields(fields []Field) bool {
	for _, f := range fields {
		if !f.IsRepeated {
			return true
		}
	}
	return false
}

func NonRepeatedFieldsEqual(fields []Field) string {
	var parts []string
	for _, f := range fields {
		if !f.IsRepeated {
			parts = append(parts, FieldEqual("old."+f.Name, "entity."+f.Name, f))
		}
	}
	return strings.Join(parts, " && ")
}

func MapKeyType(f Field) string {
	if f.Type == "[]byte" {
		return "string"
	}
	return f.Type
}

func MapKeyExpr(expr string, f Field) string {
	if f.Type == "[]byte" {
		return "string(" + expr + ")"
	}
	return expr
}

func MsgHasVersionstampPK(msg Message) bool {
	for _, f := range msg.PrimaryKeyFields {
		if f.IsVersionstamp {
			return true
		}
	}
	return false
}
