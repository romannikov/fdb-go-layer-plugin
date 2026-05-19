package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strings"
	"text/template"

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

func main() {
	protogen.Options{}.Run(func(plugin *protogen.Plugin) error {
		messages := []Message{}
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

				msgOptions := message.Desc.Options()
				processedMessage := ProcessMessage(message, msgOptions)
				if processedMessage != nil {
					processedMessage.GoPackagePath = goPackagePath
					processedMessage.GoPackageName = string(file.GoPackageName)
					processedMessage.FilePrefix = file.GeneratedFilenamePrefix
					messages = append(messages, *processedMessage)
				}
			}
		}

		tmpl := template.Must(template.New("fdb").Funcs(template.FuncMap{
			"joinFieldNames":      JoinFieldNames,
			"lower":               strings.ToLower,
			"hasMutationFields":   HasMutationFields,
			"hasBinaryImports":    HasBinaryImports,
			"msgHasVersionstampPK": MsgHasVersionstampPK,
			"packField":           PackField,
		}).Parse(fdbTemplate))

		for _, msg := range messages {
			fileName := msg.FilePrefix + "_" + strings.ToLower(msg.Name) + ".go"
			genFile := plugin.NewGeneratedFile(fileName, "")

			err := tmpl.Execute(genFile, msg)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Generated %s\n", fileName)
		}

		return nil
	})
}

func ProcessMessage(message *protogen.Message, msgOptions proto.Message) *Message {
	primaryKeyFields := []Field{}
	secondaryIndexes := []SecondaryIndex{}

	msgName := message.GoIdent.GoName

	fieldMap := make(map[string]*protogen.Field)
	fields := []Field{}
	for _, field := range message.Fields {
		fieldName := field.Desc.Name()
		fieldMap[string(fieldName)] = field

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

		fields = append(fields, Field{
			Name:            field.GoName,
			Type:            GoType(field.Desc.Kind()),
			IsRepeated:      field.Desc.IsList(),
			Mutation:        mutation,
			MutationFDBType: mutationFDBType,
			MutationValue:   mutationValue,
			Number:          int32(field.Desc.Number()),
			IsVersionstamp:  isVersionstamp,
			IsUnsigned:      field.Desc.Kind() == protoreflect.Uint32Kind || field.Desc.Kind() == protoreflect.Uint64Kind || field.Desc.Kind() == protoreflect.Fixed32Kind || field.Desc.Kind() == protoreflect.Fixed64Kind,
		})
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
		if field, ok := fieldMap[pkName]; ok {
			fieldOptions := field.Desc.Options()
			var isVersionstamp bool
			if proto.HasExtension(fieldOptions, annotationspb.E_IsVersionstamp) {
				val := proto.GetExtension(fieldOptions, annotationspb.E_IsVersionstamp)
				if val != nil {
					isVersionstamp = val.(bool)
				}
			}

			primaryKeyFields = append(primaryKeyFields, Field{
				Name:           field.GoName,
				Type:           GoType(field.Desc.Kind()),
				IsVersionstamp: isVersionstamp,
				IsUnsigned:     field.Desc.Kind() == protoreflect.Uint32Kind || field.Desc.Kind() == protoreflect.Uint64Kind || field.Desc.Kind() == protoreflect.Fixed32Kind || field.Desc.Kind() == protoreflect.Fixed64Kind,
			})
		} else {
			log.Fatalf("Primary key field %s not found in message %s", pkName, msgName)
		}
	}

	usedHashes := make(map[int64]string)

	if proto.HasExtension(msgOptions, annotationspb.E_SecondaryIndex) {
		siValues := proto.GetExtension(msgOptions, annotationspb.E_SecondaryIndex)
		if siValues != nil {
			switch v := siValues.(type) {
			case []*annotationspb.SecondaryIndex:
				for _, idx := range v {
					idxFields := []Field{}
					for _, idxFieldName := range idx.Fields {
						if field, ok := fieldMap[idxFieldName]; ok {
							idxFields = append(idxFields, Field{
								Name:       field.GoName,
								Type:       GoType(field.Desc.Kind()),
								IsRepeated: field.Desc.IsList(),
								IsUnsigned: field.Desc.Kind() == protoreflect.Uint32Kind || field.Desc.Kind() == protoreflect.Uint64Kind || field.Desc.Kind() == protoreflect.Fixed32Kind || field.Desc.Kind() == protoreflect.Fixed64Kind,
							})
						} else {
							log.Fatalf("Secondary index field %s not found in message %s", idxFieldName, msgName)
						}
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
					var fieldNames []string
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

					secondaryIndexes = append(secondaryIndexes, SecondaryIndex{
						Fields:      idxFields,
						IsFanOut:    isFanOut,
						FanOutField: fanOutField,
						IndexID:     indexID,
					})
				}
			case *annotationspb.SecondaryIndex:
				idxFields := []Field{}
				for _, idxFieldName := range v.Fields {
					if field, ok := fieldMap[idxFieldName]; ok {
						idxFields = append(idxFields, Field{
							Name:       field.GoName,
							Type:       GoType(field.Desc.Kind()),
							IsRepeated: field.Desc.IsList(),
							IsUnsigned: field.Desc.Kind() == protoreflect.Uint32Kind || field.Desc.Kind() == protoreflect.Uint64Kind || field.Desc.Kind() == protoreflect.Fixed32Kind || field.Desc.Kind() == protoreflect.Fixed64Kind,
						})
					} else {
						log.Fatalf("Secondary index field %s not found in message %s", idxFieldName, msgName)
					}
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
				var fieldNames []string
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

				secondaryIndexes = append(secondaryIndexes, SecondaryIndex{
					Fields:      idxFields,
					IsFanOut:    isFanOut,
					FanOutField: fanOutField,
					IndexID:     indexID,
				})
			default:
				log.Fatalf("Unknown type for secondary_index: %T", v)
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

	return &Message{
		Name:             msgName,
		Fields:           fields,
		PrimaryKeyFields: primaryKeyFields,
		SecondaryIndexes: secondaryIndexes,
		IsQueue:          isQueue,
	}
}

func GoType(kind protoreflect.Kind) string {
	switch kind {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		return "int32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		return "int64"
	case protoreflect.FloatKind:
		return "float32"
	case protoreflect.DoubleKind:
		return "float64"
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BoolKind:
		return "bool"
	default:
		return "interface{}"
	}
}

func JoinFieldNames(fields []Field) string {
	names := []string{}
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

func PackField(name string, f Field) string {
	if f.IsUnsigned {
		return "uint64(" + name + ")"
	}
	if f.Type == "int32" || f.Type == "int64" {
		return "int64(" + name + ")"
	}
	return name
}

func MsgHasVersionstampPK(msg Message) bool {
	for _, f := range msg.PrimaryKeyFields {
		if f.IsVersionstamp {
			return true
		}
	}
	return false
}

const fdbTemplate = `// Code generated by fdb-go-layer-plugin. DO NOT EDIT.
// Source: {{.GoPackagePath}}

package {{.GoPackageName}}

import (
	"context"
	{{if hasBinaryImports .}}"encoding/binary"{{end}}
	{{if not .IsQueue}}"fmt"{{end}}

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	fdblayer "github.com/romannikov/fdb-go-layer-plugin/fdb-layer"
	"google.golang.org/protobuf/proto"
)

{{if .IsQueue}}
// {{.Name}}Repository defines the repository interface for the {{.Name}} queue.
type {{.Name}}Repository interface {
	Enqueue(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, msg *{{.Name}}) error
	Dequeue(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, {{(index .PrimaryKeyFields 0).Name | lower}} {{(index .PrimaryKeyFields 0).Type}}) (*{{.Name}}, error)
}

type {{.Name | lower}}Repository struct {
	store *fdblayer.RecordStore
}

// New{{.Name}}Repository creates a new {{.Name}}Repository instance.
func New{{.Name}}Repository(store *fdblayer.RecordStore) {{.Name}}Repository {
	return &{{.Name | lower}}Repository{store: store}
}

// Enqueue adds a {{.Name}} to the queue.
func (r *{{.Name | lower}}Repository) Enqueue(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, entity *{{.Name}}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	typeID, err := r.store.GetTypeID("{{.Name}}")
	if err != nil {
		return err
	}

	{{if msgHasVersionstampPK .}}
	key, err := dir.PackWithVersionstamp(tuple.Tuple{typeID, fdblayer.DataNamespace, {{range .PrimaryKeyFields}}{{if .IsVersionstamp}}tuple.IncompleteVersionstamp(0){{else}}{{packField (printf "entity.%s" .Name) .}}{{end}}, {{end}}})
	if err != nil {
		return err
	}
	{{else}}
	key := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, {{range .PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}}})
	{{end}}

	value, err := proto.Marshal(entity)
	if err != nil {
		return err
	}

	{{if msgHasVersionstampPK .}}
	tr.SetVersionstampedKey(key, value)
	{{else}}
	tr.Set(key, value)
	{{end}}

	return nil
}

// Dequeue reads and removes the single oldest message from the queue.
func (r *{{.Name | lower}}Repository) Dequeue(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, {{(index .PrimaryKeyFields 0).Name | lower}} {{(index .PrimaryKeyFields 0).Type}}) (*{{.Name}}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	typeID, err := r.store.GetTypeID("{{.Name}}")
	if err != nil {
		return nil, err
	}

	prefixTuple := tuple.Tuple{typeID, fdblayer.DataNamespace, {{(index .PrimaryKeyFields 0).Name | lower}}}
	prefixRange, err := fdb.PrefixRange(dir.Pack(prefixTuple))
	if err != nil {
		return nil, err
	}

	options := fdb.RangeOptions{Limit: 1}
	rows := tr.GetRange(prefixRange, options).GetSliceOrPanic()

	if len(rows) == 0 {
		return nil, nil
	}

	firstItem := rows[0]
	keyToDelete := firstItem.Key
	payload := firstItem.Value

	tr.Clear(keyToDelete)

	entity := &{{.Name}}{}
	err = proto.Unmarshal(payload, entity)
	if err != nil {
		return nil, err
	}

	return entity, nil
}
{{else}}
// {{.Name}}PaginationOptions represents options for paginated queries
type {{.Name}}PaginationOptions struct {
	Begin tuple.Tuple
	Limit int
}

// {{.Name}}PaginatedResult represents a paginated result set
type {{.Name}}PaginatedResult struct {
	Items      []*{{.Name}}
	NextKey    tuple.Tuple
	HasMore    bool
}

{{if gt (len .PrimaryKeyFields) 1}}
// {{.Name}}PrimaryKey represents the compound primary key for {{.Name}}.
type {{.Name}}PrimaryKey struct {
	{{range .PrimaryKeyFields}}
	{{.Name}} {{.Type}}
	{{end}}
}
{{end}}

// {{.Name}}Repository defines the repository interface for {{.Name}}.
type {{.Name}}Repository interface {
	fdblayer.GenericRepository[*{{.Name}}, {{if eq (len .PrimaryKeyFields) 1}}{{(index .PrimaryKeyFields 0).Type}}{{else if gt (len .PrimaryKeyFields) 1}}{{.Name}}PrimaryKey{{else}}struct{}{{end}}]

	BatchGet{{.Name}}(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, ids []tuple.Tuple) (map[string]*{{.Name}}, error)
	List{{.Name}}(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, opts {{.Name}}PaginationOptions) (*{{.Name}}PaginatedResult, error)
	{{range $idxIndex, $idx := .SecondaryIndexes}}
	Get{{$.Name}}By{{joinFieldNames $idx.Fields}}(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, {{range $i, $f := $idx.Fields}}{{if $i}}, {{end}}{{$f.Name}} {{$f.Type}}{{end}}) ([]*{{$.Name}}, error)
	{{end}}
	{{range .Fields}}
	{{if .MutationValue}}
	{{if eq .MutationValue 2}}Add{{else if eq .MutationValue 12}}Max{{else if eq .MutationValue 13}}Min{{end}}{{$.Name}}{{.Name}}(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, {{range $.PrimaryKeyFields}}{{.Name}} {{.Type}}, {{end}} val {{.Type}}) error
	{{end}}
	{{end}}
}

type {{.Name | lower}}Repository struct {
	store *fdblayer.RecordStore
}

// New{{.Name}}Repository creates a new {{.Name}}Repository instance.
func New{{.Name}}Repository(store *fdblayer.RecordStore) {{.Name}}Repository {
	return &{{.Name | lower}}Repository{store: store}
}

// Create creates a new {{.Name}} entity in the database.
func (r *{{.Name | lower}}Repository) Create(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, entity *{{.Name}}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	typeID, err := r.store.GetTypeID("{{.Name}}")
	if err != nil {
		return err
	}

	{{if msgHasVersionstampPK .}}
	key, err := dir.PackWithVersionstamp(tuple.Tuple{typeID, fdblayer.DataNamespace, {{range .PrimaryKeyFields}}{{if .IsVersionstamp}}tuple.IncompleteVersionstamp(0){{else}}{{packField (printf "entity.%s" .Name) .}}{{end}}, {{end}}})
	if err != nil {
		return err
	}
	{{else}}
	key := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, {{range .PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}}})
	{{end}}

	// Save atomic fields and zero them out for marshaling
	{{range .Fields}}
	{{if .Mutation}}
	atomic_{{.Name}} := entity.{{.Name}}
	entity.{{.Name}} = 0
	{{end}}
	{{end}}

	value, err := proto.Marshal(entity)
	if err != nil {
		return err
	}

	// Restore atomic fields
	{{range .Fields}}
	{{if .Mutation}}
	entity.{{.Name}} = atomic_{{.Name}}
	{{end}}
	{{end}}

	{{if msgHasVersionstampPK .}}
	tr.SetVersionstampedKey(key, value)
	{{else}}
	tr.Set(key, value)
	{{end}}

	// Store atomic fields in separate keys
	{{range .Fields}}
	{{if .Mutation}}
	{
		fieldKey := dir.Pack(tuple.Tuple{typeID, fdblayer.FieldNamespace, {{range $.PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}} {{.Number}}})
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(atomic_{{.Name}}))
		tr.Set(fieldKey, buf)
	}
	{{end}}
	{{end}}

	{{range $idxIndex, $idx := .SecondaryIndexes}}
	{
		{{if $idx.IsFanOut}}
		// Fan-out index
		{{range $i, $f := $idx.Fields}}
		{{if $f.IsRepeated}}
		for _, item := range entity.{{$f.Name}} {
			indexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, {{$idx.IndexID}}, 
				{{range $j, $sf := $idx.Fields}}
				{{if eq $j $i}} item, {{else}} {{packField (printf "entity.%s" $sf.Name) $sf}}, {{end}}
				{{end}}
				{{range $.PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}}
			})
			tr.Set(indexKey, []byte{})
		}
		{{end}}
		{{end}}
		{{else}}
		// Standard index
		indexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, {{$idx.IndexID}}, 
			{{range $i, $f := $idx.Fields}} {{packField (printf "entity.%s" $f.Name) $f}}, {{end}}
			{{range $.PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}}
		})
		tr.Set(indexKey, []byte{})
		{{end}}
	}
	{{end}}

	return nil
}

// Get retrieves a {{.Name}} entity by its primary key.
func (r *{{.Name | lower}}Repository) Get(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, pk {{if eq (len .PrimaryKeyFields) 1}}{{(index .PrimaryKeyFields 0).Type}}{{else if gt (len .PrimaryKeyFields) 1}}{{.Name}}PrimaryKey{{else}}struct{}{{end}}) (*{{.Name}}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	typeID, err := r.store.GetTypeID("{{.Name}}")
	if err != nil {
		return nil, err
	}

	key := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, {{if eq (len .PrimaryKeyFields) 1}}{{packField "pk" (index .PrimaryKeyFields 0)}}{{else if gt (len .PrimaryKeyFields) 1}}{{range $i, $f := .PrimaryKeyFields}}{{if $i}}, {{end}}{{packField (printf "pk.%s" $f.Name) $f}}{{end}}{{end}}})
	value := tr.Get(key).MustGet()
	if value == nil {
		return nil, fmt.Errorf("{{.Name | lower}} not found")
	}
	entity := &{{.Name}}{}
	err = proto.Unmarshal(value, entity)
	if err != nil {
		return nil, err
	}

	// Read atomic fields
	{{range .Fields}}
	{{if .Mutation}}
	{
		fieldKey := dir.Pack(tuple.Tuple{typeID, fdblayer.FieldNamespace, {{if eq (len $.PrimaryKeyFields) 1}}{{packField "pk" (index $.PrimaryKeyFields 0)}}{{else if gt (len $.PrimaryKeyFields) 1}}{{range $i, $f := $.PrimaryKeyFields}}{{if $i}}, {{end}}{{packField (printf "pk.%s" $f.Name) $f}}{{end}}{{end}}, {{.Number}}})
		fieldVal := tr.Get(fieldKey).MustGet()
		if fieldVal != nil {
			entity.{{.Name}} = {{.Type}}(binary.LittleEndian.Uint64(fieldVal))
		}
	}
	{{end}}
	{{end}}
	return entity, nil
}

// Set updates an existing {{.Name}} entity in the database.
func (r *{{.Name | lower}}Repository) Set(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, entity *{{.Name}}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	typeID, err := r.store.GetTypeID("{{.Name}}")
	if err != nil {
		return err
	}

	{{if msgHasVersionstampPK .}}
	// If the entity already has a versionstamp, delete the old versionstamped record.
	// (Since the primary key contains the versionstamp, an update changes the key itself).
	hasOldVS := false
	for _, b := range entity.{{range .PrimaryKeyFields}}{{if .IsVersionstamp}}{{.Name}}{{end}}{{end}} {
		if b != 0 {
			hasOldVS = true
			break
		}
	}
	if hasOldVS {
		oldKey := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, {{range .PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}}})
		tr.Clear(oldKey)
	}

	key, err := dir.PackWithVersionstamp(tuple.Tuple{typeID, fdblayer.DataNamespace, {{range .PrimaryKeyFields}}{{if .IsVersionstamp}}tuple.IncompleteVersionstamp(0){{else}}{{packField (printf "entity.%s" .Name) .}}{{end}}, {{end}}})
	if err != nil {
		return err
	}
	{{else}}
	key := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, {{range .PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}}})

	// Clear stale index entries from the old version of the entity
	oldValue := tr.Get(key).MustGet()
	if oldValue != nil {
		old := &{{.Name}}{}
		if unmarshalErr := proto.Unmarshal(oldValue, old); unmarshalErr == nil {
			{{range $idxIndex, $idx := .SecondaryIndexes}}
			{
				{{if $idx.IsFanOut}}
				// Fan-out index
				{{range $i, $f := $idx.Fields}}
				{{if $f.IsRepeated}}
				for _, item := range old.{{$f.Name}} {
					oldIndexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, {{$idx.IndexID}}, 
						{{range $j, $sf := $idx.Fields}}
						{{if eq $j $i}} item, {{else}} {{packField (printf "old.%s" $sf.Name) $sf}}, {{end}}
						{{end}}
						{{range $.PrimaryKeyFields}} {{packField (printf "old.%s" .Name) .}}, {{end}}
					})
					tr.Clear(oldIndexKey)
				}
				{{end}}
				{{end}}
				{{else}}
				// Standard index
				oldIndexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, {{$idx.IndexID}},
					{{range $i, $f := $idx.Fields}} {{packField (printf "old.%s" $f.Name) $f}}, {{end}}
					{{range $.PrimaryKeyFields}} {{packField (printf "old.%s" .Name) .}}, {{end}}
				})
				tr.Clear(oldIndexKey)
				{{end}}
			}
			{{end}}
		}
	}
	{{end}}

	// Save atomic fields and zero them out for marshaling
	{{range .Fields}}
	{{if .Mutation}}
	atomic_{{.Name}} := entity.{{.Name}}
	entity.{{.Name}} = 0
	{{end}}
	{{end}}

	value, err := proto.Marshal(entity)
	if err != nil {
		return err
	}

	// Restore atomic fields
	{{range .Fields}}
	{{if .Mutation}}
	entity.{{.Name}} = atomic_{{.Name}}
	{{end}}
	{{end}}

	{{if msgHasVersionstampPK .}}
	tr.SetVersionstampedKey(key, value)
	{{else}}
	tr.Set(key, value)
	{{end}}

	// Store atomic fields in separate keys
	{{range .Fields}}
	{{if .Mutation}}
	{
		fieldKey := dir.Pack(tuple.Tuple{typeID, fdblayer.FieldNamespace, {{range $.PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}} {{.Number}}})
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(atomic_{{.Name}}))
		tr.Set(fieldKey, buf)
	}
	{{end}}
	{{end}}

	{{range $idxIndex, $idx := .SecondaryIndexes}}
	{
		{{if $idx.IsFanOut}}
		// Fan-out index
		{{range $i, $f := $idx.Fields}}
		{{if $f.IsRepeated}}
		for _, item := range entity.{{$f.Name}} {
			indexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, {{$idx.IndexID}}, 
				{{range $j, $sf := $idx.Fields}}
				{{if eq $j $i}} item, {{else}} {{packField (printf "entity.%s" $sf.Name) $sf}}, {{end}}
				{{end}}
				{{range $.PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}}
			})
			tr.Set(indexKey, []byte{})
		}
		{{end}}
		{{end}}
		{{else}}
		// Standard index
		indexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, {{$idx.IndexID}}, 
			{{range $i, $f := $idx.Fields}} {{packField (printf "entity.%s" $f.Name) $f}}, {{end}}
			{{range $.PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}}
		})
		tr.Set(indexKey, []byte{})
		{{end}}
	}
	{{end}}

	return nil
}

// Delete removes a {{.Name}} entity from the database.
func (r *{{.Name | lower}}Repository) Delete(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, pk {{if eq (len .PrimaryKeyFields) 1}}{{(index .PrimaryKeyFields 0).Type}}{{else if gt (len .PrimaryKeyFields) 1}}{{.Name}}PrimaryKey{{else}}struct{}{{end}}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	typeID, err := r.store.GetTypeID("{{.Name}}")
	if err != nil {
		return err
	}

	key := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace, {{if eq (len .PrimaryKeyFields) 1}}{{packField "pk" (index .PrimaryKeyFields 0)}}{{else if gt (len .PrimaryKeyFields) 1}}{{range $i, $f := .PrimaryKeyFields}}{{if $i}}, {{end}}{{packField (printf "pk.%s" $f.Name) $f}}{{end}}{{end}}})
	value := tr.Get(key).MustGet()
	if value != nil {
		entity := &{{.Name}}{}
		err := proto.Unmarshal(value, entity)
		if err == nil {
			{{range $idxIndex, $idx := .SecondaryIndexes}}
			{
				{{if $idx.IsFanOut}}
				// Fan-out index
				{{range $i, $f := $idx.Fields}}
				{{if $f.IsRepeated}}
				for _, item := range entity.{{$f.Name}} {
					indexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, {{$idx.IndexID}}, 
						{{range $j, $sf := $idx.Fields}}
						{{if eq $j $i}} item, {{else}} {{packField (printf "entity.%s" $sf.Name) $sf}}, {{end}}
						{{end}}
						{{range $.PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}}
					})
					tr.Clear(indexKey)
				}
				{{end}}
				{{end}}
				{{else}}
				// Standard index
				indexKey := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, {{$idx.IndexID}}, 
					{{range $i, $f := $idx.Fields}} {{packField (printf "entity.%s" $f.Name) $f}}, {{end}}
					{{range $.PrimaryKeyFields}} {{packField (printf "entity.%s" .Name) .}}, {{end}}
				})
				tr.Clear(indexKey)
				{{end}}
			}
			{{end}}
		}
	}
	tr.Clear(key)
	// Clear atomic fields
	{{range .Fields}}
	{{if .Mutation}}
	{
		fieldKey := dir.Pack(tuple.Tuple{typeID, fdblayer.FieldNamespace, {{if eq (len $.PrimaryKeyFields) 1}}{{packField "pk" (index $.PrimaryKeyFields 0)}}{{else if gt (len $.PrimaryKeyFields) 1}}{{range $i, $f := $.PrimaryKeyFields}}{{if $i}}, {{end}}{{packField (printf "pk.%s" $f.Name) $f}}{{end}}{{end}}, {{.Number}}})
		tr.Clear(fieldKey)
	}
	{{end}}
	{{end}}
	return nil
}

// BatchGet{{.Name}} retrieves multiple {{.Name}} entities by their primary keys.
func (r *{{.Name | lower}}Repository) BatchGet{{.Name}}(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, ids []tuple.Tuple) (map[string]*{{.Name}}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	typeID, err := r.store.GetTypeID("{{.Name}}")
	if err != nil {
		return nil, err
	}

	result := make(map[string]*{{.Name}})
	futures := make([]fdb.FutureByteSlice, len(ids))

	for i, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keyTpl := make(tuple.Tuple, 2+len(id))
		keyTpl[0] = typeID
		keyTpl[1] = fdblayer.DataNamespace
		copy(keyTpl[2:], id)
		key := dir.Pack(keyTpl)
		futures[i] = tr.Get(key)
	}

	for i, future := range futures {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value := future.MustGet()
		if value == nil {
			continue
		}
		entity := &{{.Name}}{}
		err := proto.Unmarshal(value, entity)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal entity at index %d: %w", i, err)
		}
		result[ids[i].String()] = entity
	}

	return result, nil
}

// List{{.Name}} retrieves a list of {{.Name}} entities starting from the given key.
func (r *{{.Name | lower}}Repository) List{{.Name}}(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, opts {{.Name}}PaginationOptions) (*{{.Name}}PaginatedResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	typeID, err := r.store.GetTypeID("{{.Name}}")
	if err != nil {
		return nil, err
	}

	result := &{{.Name}}PaginatedResult{
		Items: make([]*{{.Name}}, 0),
	}

	beginTpl := make(tuple.Tuple, 2+len(opts.Begin))
	beginTpl[0] = typeID
	beginTpl[1] = fdblayer.DataNamespace
	copy(beginTpl[2:], opts.Begin)
	begin := dir.Pack(beginTpl)

	// Scan only data keys under typeID namespace.
	dataPrefix := dir.Pack(tuple.Tuple{typeID, fdblayer.DataNamespace})
	dataPrefixRange, err := fdb.PrefixRange(dataPrefix)
	if err != nil {
		return nil, err
	}

	iter := tr.GetRange(fdb.KeyRange{
		Begin: begin,
		End:   dataPrefixRange.End,
	}, fdb.RangeOptions{
		Reverse: false,
	}).Iterator()

	var nextKey fdb.Key
	for iter.Advance() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		kv := iter.MustGet()

		entity := &{{.Name}}{}
		err = proto.Unmarshal(kv.Value, entity)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal entity: %w", err)
		}
		result.Items = append(result.Items, entity)
		nextKey = kv.Key

		// Stop once we have enough items for pagination check
		if len(result.Items) > opts.Limit {
			break
		}
	}

	result.HasMore = len(result.Items) > opts.Limit
	if result.HasMore {
		tpl, err := dir.Unpack(nextKey)
		if err != nil {
			return nil, fmt.Errorf("failed to unpack next key: %w", err)
		}
		// Remove typeID and DataNamespace to return just the PK tuple
		result.NextKey = tpl[2:]
		result.Items = result.Items[:opts.Limit]
	}

	return result, nil
}

{{range $idxIndex, $idx := .SecondaryIndexes}}
// Get{{$.Name}}By{{joinFieldNames $idx.Fields}} retrieves {{$.Name}} entities by their {{joinFieldNames $idx.Fields}} index.
func (r *{{$.Name | lower}}Repository) Get{{$.Name}}By{{joinFieldNames $idx.Fields}}(ctx context.Context, tr fdb.ReadTransaction, dir directory.DirectorySubspace, {{range $i, $f := $idx.Fields}}{{if $i}}, {{end}}{{$f.Name}} {{$f.Type}}{{end}}) ([]*{{$.Name}}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	typeID, err := r.store.GetTypeID("{{$.Name}}")
	if err != nil {
		return nil, err
	}

	entities := []*{{$.Name}}{}
	indexKeyPrefix := dir.Pack(tuple.Tuple{typeID, fdblayer.IndexNamespace, {{$idx.IndexID}}, {{range $i, $f := $idx.Fields}} {{packField $f.Name $f}}, {{end}}})
	indexRange, err := fdb.PrefixRange(indexKeyPrefix)
	if err != nil {
		return nil, err
	}
	kvs := tr.GetRange(indexRange, fdb.RangeOptions{}).GetSliceOrPanic()
	for _, kv := range kvs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tpl, err := dir.Unpack(kv.Key)
		if err != nil {
			return nil, err
		}
		pkIndexStart := 3 + {{len $idx.Fields}}
		pkTuple := tpl[pkIndexStart:]
		keyTpl := make(tuple.Tuple, 2+len(pkTuple))
		keyTpl[0] = typeID
		keyTpl[1] = fdblayer.DataNamespace
		copy(keyTpl[2:], pkTuple)
		key := dir.Pack(keyTpl)
		value := tr.Get(key).MustGet()
		if value == nil {
			continue
		}
		entity := &{{$.Name}}{}
		err = proto.Unmarshal(value, entity)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}
{{end}}

{{range .Fields}}
{{if .MutationValue}}
// {{if eq .MutationValue 2}}Add{{else if eq .MutationValue 12}}Max{{else if eq .MutationValue 13}}Min{{end}}{{$.Name}}{{.Name}} applies an atomic mutation to the {{.Name}} field of {{$.Name}}.
func (r *{{$.Name | lower}}Repository) {{if eq .MutationValue 2}}Add{{else if eq .MutationValue 12}}Max{{else if eq .MutationValue 13}}Min{{end}}{{$.Name}}{{.Name}}(ctx context.Context, tr fdblayer.Transaction, dir directory.DirectorySubspace, {{range $.PrimaryKeyFields}}{{.Name}} {{.Type}}, {{end}} val {{.Type}}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	typeID, err := r.store.GetTypeID("{{$.Name}}")
	if err != nil {
		return err
	}
	key := dir.Pack(tuple.Tuple{typeID, fdblayer.FieldNamespace, {{range $.PrimaryKeyFields}} {{packField .Name .}}, {{end}} {{.Number}}})
	
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(val))
	
	{{if eq .MutationValue 2}}tr.Add(key, buf){{else if eq .MutationValue 12}}tr.Max(key, buf){{else if eq .MutationValue 13}}tr.Min(key, buf){{end}}
	return nil
}
{{end}}
{{end}}
{{end}}
`
