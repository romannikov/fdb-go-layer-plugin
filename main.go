package main

import (
	"fmt"
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
	Name string
	Type string
}

type SecondaryIndex struct {
	Fields []Field
}

type Message struct {
	Name             string
	Fields           []Field
	PrimaryKeyFields []Field
	SecondaryIndexes []SecondaryIndex
	GoPackagePath    string
	GoPackageName    string
	FilePrefix       string
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
			"joinFieldNames": JoinFieldNames,
			"lower":          strings.ToLower,
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

		if len(messages) > 0 {
			var metaFileName string
			for _, f := range plugin.Files {
				if f.Generate {
					metaFileName = f.GeneratedFilenamePrefix + "_metadata.go"
					break
				}
			}
			metaFile := plugin.NewGeneratedFile(metaFileName, "")
			metaTmpl := template.Must(template.New("meta").Parse(metadataTemplate))
			metaData := struct {
				GoPackagePath string
				GoPackageName string
				Messages      []Message
			}{
				GoPackagePath: messages[0].GoPackagePath,
				GoPackageName: messages[0].GoPackageName,
				Messages:      messages,
			}
			err := metaTmpl.Execute(metaFile, metaData)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Generated metadata.go\n")
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
		fields = append(fields, Field{
			Name: field.GoName,
			Type: GoType(field.Desc.Kind()),
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
			primaryKeyFields = append(primaryKeyFields, Field{
				Name: field.GoName,
				Type: GoType(field.Desc.Kind()),
			})
		} else {
			log.Fatalf("Primary key field %s not found in message %s", pkName, msgName)
		}
	}

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
								Name: field.GoName,
								Type: GoType(field.Desc.Kind()),
							})
						} else {
							log.Fatalf("Secondary index field %s not found in message %s", idxFieldName, msgName)
						}
					}
					secondaryIndexes = append(secondaryIndexes, SecondaryIndex{
						Fields: idxFields,
					})
				}
			case *annotationspb.SecondaryIndex:
				idxFields := []Field{}
				for _, idxFieldName := range v.Fields {
					if field, ok := fieldMap[idxFieldName]; ok {
						idxFields = append(idxFields, Field{
							Name: field.GoName,
							Type: GoType(field.Desc.Kind()),
						})
					} else {
						log.Fatalf("Secondary index field %s not found in message %s", idxFieldName, msgName)
					}
				}
				secondaryIndexes = append(secondaryIndexes, SecondaryIndex{
					Fields: idxFields,
				})
			default:
				log.Fatalf("Unknown type for secondary_index: %T", v)
			}
		}
	}

	return &Message{
		Name:             msgName,
		Fields:           fields,
		PrimaryKeyFields: primaryKeyFields,
		SecondaryIndexes: secondaryIndexes,
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

const metadataTemplate = `// Code generated by fdb-go-layer-plugin. DO NOT EDIT.
// Source: {{.GoPackagePath}}

package {{.GoPackageName}}

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Transaction is a mockable interface that abstracts fdb.Transaction
type Transaction interface {
	fdb.ReadTransaction
	Set(key fdb.KeyConvertible, value []byte)
	Clear(key fdb.KeyConvertible)
}

// RecordStore holds metadata mapping between message names and their integer type IDs.
type RecordStore struct {
	metadata map[string]int64
}

// NewRecordStore creates a new RecordStore instance.
func NewRecordStore() *RecordStore {
	return &RecordStore{
		metadata: make(map[string]int64),
	}
}

// Metadata returns a read-only copy of the metadata mapping.
func (s *RecordStore) Metadata() map[string]int64 {
	copy := make(map[string]int64, len(s.metadata))
	for k, v := range s.metadata {
		copy[k] = v
	}
	return copy
}

// SyncMetadata reads the existing metadata from FDB and assigns new IDs to any unmapped messages.
func (s *RecordStore) SyncMetadata(tr Transaction, metaDir directory.DirectorySubspace) error {
	kvs := tr.GetRange(metaDir, fdb.RangeOptions{}).GetSliceOrPanic()
	
	maxID := int64(0)
	for _, kv := range kvs {
		tpl, err := metaDir.Unpack(kv.Key)
		if err != nil {
			return err
		}
		valTpl, err := tuple.Unpack(kv.Value)
		if err != nil {
			return err
		}
		msgName := tpl[0].(string)
		id := valTpl[0].(int64)
		s.metadata[msgName] = id
		if id > maxID {
			maxID = id
		}
	}

	messages := []string{
		{{range .Messages}} "{{.Name}}",
		{{end}}
	}

	for _, msg := range messages {
		if _, exists := s.metadata[msg]; !exists {
			maxID++
			s.metadata[msg] = maxID
			key := metaDir.Pack(tuple.Tuple{msg})
			val := tuple.Tuple{int64(maxID)}.Pack()
			tr.Set(key, val)
		}
	}
	return nil
}
`

const fdbTemplate = `// Code generated by fdb-go-layer-plugin. DO NOT EDIT.
// Source: {{.GoPackagePath}}

package {{.GoPackageName}}

import (
	"fmt"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"google.golang.org/protobuf/proto"
)

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

func (s *RecordStore) get{{.Name}}TypeID() (int64, error) {
	if s.metadata == nil {
		return 0, fmt.Errorf("metadata not initialized, call SyncMetadata first")
	}
	typeID, ok := s.metadata["{{.Name}}"]
	if !ok {
		return 0, fmt.Errorf("type {{.Name}} not found in metadata")
	}
	return typeID, nil
}

// Create{{.Name}} creates a new {{.Name}} entity in the database.
func (s *RecordStore) Create{{.Name}}(tr Transaction, dir directory.DirectorySubspace, entity *{{.Name}}) error {
	typeID, err := s.get{{.Name}}TypeID()
	if err != nil {
		return err
	}

	key := dir.Pack(tuple.Tuple{typeID, {{range .PrimaryKeyFields}} entity.{{.Name}}, {{end}}})
	value, err := proto.Marshal(entity)
	if err != nil {
		return err
	}
	tr.Set(key, value)

	{{range $idxIndex, $idx := .SecondaryIndexes}}
	{{if eq $idxIndex 0}}
	indexKey := dir.Pack(tuple.Tuple{typeID, "index", "{{joinFieldNames $idx.Fields}}", 
	{{else}}
	indexKey = dir.Pack(tuple.Tuple{typeID, "index", "{{joinFieldNames $idx.Fields}}", 
	{{end}}
		{{range $i, $f := $idx.Fields}} entity.{{ $f.Name }}, {{end}}
		{{range $.PrimaryKeyFields}} entity.{{.Name}}, {{end}}
	})
	tr.Set(indexKey, []byte{})
	{{end}}

	return nil
}

// Get{{.Name}} retrieves a {{.Name}} entity by its primary key.
func (s *RecordStore) Get{{.Name}}(tr fdb.ReadTransaction, dir directory.DirectorySubspace, {{range $index, $element := .PrimaryKeyFields}}{{if $index}}, {{end}}{{.Name}} {{.Type}}{{end}}) (*{{.Name}}, error) {
	typeID, err := s.get{{.Name}}TypeID()
	if err != nil {
		return nil, err
	}

	key := dir.Pack(tuple.Tuple{typeID, {{range .PrimaryKeyFields}} {{.Name}}, {{end}}})
	value := tr.Get(key).MustGet()
	if value == nil {
		return nil, fmt.Errorf("{{.Name | lower}} not found")
	}
	entity := &{{.Name}}{}
	err = proto.Unmarshal(value, entity)
	if err != nil {
		return nil, err
	}
	return entity, nil
}

// Set{{.Name}} updates an existing {{.Name}} entity in the database.
func (s *RecordStore) Set{{.Name}}(tr Transaction, dir directory.DirectorySubspace, entity *{{.Name}}) error {
	typeID, err := s.get{{.Name}}TypeID()
	if err != nil {
		return err
	}

	key := dir.Pack(tuple.Tuple{typeID, {{range .PrimaryKeyFields}} entity.{{.Name}}, {{end}}})

	// Clear stale index entries from the old version of the entity
	oldValue := tr.Get(key).MustGet()
	if oldValue != nil {
		old := &{{.Name}}{}
		if unmarshalErr := proto.Unmarshal(oldValue, old); unmarshalErr == nil {
			{{range $idxIndex, $idx := .SecondaryIndexes}}
			{{if eq $idxIndex 0}}
			oldIndexKey := dir.Pack(tuple.Tuple{typeID, "index", "{{joinFieldNames $idx.Fields}}",
			{{else}}
			oldIndexKey = dir.Pack(tuple.Tuple{typeID, "index", "{{joinFieldNames $idx.Fields}}",
			{{end}}
				{{range $i, $f := $idx.Fields}} old.{{ $f.Name }}, {{end}}
				{{range $.PrimaryKeyFields}} old.{{.Name}}, {{end}}
			})
			tr.Clear(oldIndexKey)
			{{end}}
		}
	}

	value, err := proto.Marshal(entity)
	if err != nil {
		return err
	}
	tr.Set(key, value)

	{{range $idxIndex, $idx := .SecondaryIndexes}}
	{{if eq $idxIndex 0}}
	indexKey := dir.Pack(tuple.Tuple{typeID, "index", "{{joinFieldNames $idx.Fields}}", 
	{{else}}
	indexKey = dir.Pack(tuple.Tuple{typeID, "index", "{{joinFieldNames $idx.Fields}}", 
	{{end}}
		{{range $i, $f := $idx.Fields}} entity.{{ $f.Name }}, {{end}}
		{{range $.PrimaryKeyFields}} entity.{{.Name}}, {{end}}
	})
	tr.Set(indexKey, []byte{})
	{{end}}

	return nil
}

// Delete{{.Name}} removes a {{.Name}} entity from the database.
func (s *RecordStore) Delete{{.Name}}(tr Transaction, dir directory.DirectorySubspace, {{range $index, $element := .PrimaryKeyFields}}{{if $index}}, {{end}}{{.Name}} {{.Type}}{{end}}) error {
	typeID, err := s.get{{.Name}}TypeID()
	if err != nil {
		return err
	}

	key := dir.Pack(tuple.Tuple{typeID, {{range .PrimaryKeyFields}} {{.Name}}, {{end}}})
	value := tr.Get(key).MustGet()
	if value != nil {
		entity := &{{.Name}}{}
		err := proto.Unmarshal(value, entity)
		if err == nil {
			{{range $idxIndex, $idx := .SecondaryIndexes}}
			{{if eq $idxIndex 0}}
			indexKey := dir.Pack(tuple.Tuple{typeID, "index", "{{joinFieldNames $idx.Fields}}", 
			{{else}}
			indexKey = dir.Pack(tuple.Tuple{typeID, "index", "{{joinFieldNames $idx.Fields}}", 
			{{end}}
				{{range $i, $f := $idx.Fields}} entity.{{ $f.Name }}, {{end}}
				{{range $.PrimaryKeyFields}} entity.{{.Name}}, {{end}}
			})
			tr.Clear(indexKey)
			{{end}}
		}
	}
	tr.Clear(key)
	return nil
}

{{range $idxIndex, $idx := .SecondaryIndexes}}
// Get{{$.Name}}By{{joinFieldNames $idx.Fields}} retrieves {{$.Name}} entities by their {{joinFieldNames $idx.Fields}} index.
func (s *RecordStore) Get{{$.Name}}By{{joinFieldNames $idx.Fields}}(tr fdb.ReadTransaction, dir directory.DirectorySubspace, {{range $i, $f := $idx.Fields}}{{if $i}}, {{end}}{{$f.Name}} {{$f.Type}}{{end}}) ([]*{{$.Name}}, error) {
	typeID, err := s.get{{$.Name}}TypeID()
	if err != nil {
		return nil, err
	}

	entities := []*{{$.Name}}{}
	indexKeyPrefix := dir.Pack(tuple.Tuple{typeID, "index", "{{joinFieldNames $idx.Fields}}", {{range $i, $f := $idx.Fields}} {{$f.Name}}, {{end}}})
	indexRange, err := fdb.PrefixRange(indexKeyPrefix)
	if err != nil {
		return nil, err
	}
	kvs := tr.GetRange(indexRange, fdb.RangeOptions{}).GetSliceOrPanic()
	for _, kv := range kvs {
		tpl, err := dir.Unpack(kv.Key)
		if err != nil {
			return nil, err
		}
		// tpl is: {typeID, "index", "index_name", ...idx_fields, ...pk_fields}
		// pk starts at index 3 + len(idx.Fields)
		pkIndexStart := 3 + {{len $idx.Fields}}
		pkTuple := tpl[pkIndexStart:]
		keyTpl := append(tuple.Tuple{typeID}, pkTuple...)
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

// BatchGet{{.Name}} retrieves multiple {{.Name}} entities by their primary keys.
func (s *RecordStore) BatchGet{{.Name}}(tr fdb.ReadTransaction, dir directory.DirectorySubspace, ids []tuple.Tuple) (map[string]*{{.Name}}, error) {
	typeID, err := s.get{{.Name}}TypeID()
	if err != nil {
		return nil, err
	}

	result := make(map[string]*{{.Name}})
	futures := make([]fdb.FutureByteSlice, len(ids))

	for i, id := range ids {
		keyTpl := append(tuple.Tuple{typeID}, id...)
		key := dir.Pack(keyTpl)
		futures[i] = tr.Get(key)
	}

	for i, future := range futures {
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
func (s *RecordStore) List{{.Name}}(tr fdb.ReadTransaction, dir directory.DirectorySubspace, opts {{.Name}}PaginationOptions) (*{{.Name}}PaginatedResult, error) {
	typeID, err := s.get{{.Name}}TypeID()
	if err != nil {
		return nil, err
	}

	result := &{{.Name}}PaginatedResult{
		Items: make([]*{{.Name}}, 0),
	}

	beginTpl := append(tuple.Tuple{typeID}, opts.Begin...)
	begin := dir.Pack(beginTpl)
	
	endTpl := append(tuple.Tuple{typeID}, math.MaxInt64)
	end := dir.Pack(endTpl)

	iter := tr.GetRange(fdb.KeyRange{
		Begin: begin,
		End:   end,
	}, fdb.RangeOptions{
		Limit:   opts.Limit + 1,
		Reverse: false,
	}).Iterator()

	var nextKey fdb.Key
	for iter.Advance() {
		kv := iter.MustGet()
		entity := &{{.Name}}{}
		err := proto.Unmarshal(kv.Value, entity)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal entity: %w", err)
		}
		result.Items = append(result.Items, entity)
		nextKey = kv.Key
	}

	result.HasMore = len(result.Items) > opts.Limit
	if result.HasMore {
		tpl, err := dir.Unpack(nextKey)
		if err != nil {
			return nil, fmt.Errorf("failed to unpack next key: %w", err)
		}
		// Remove typeID to return just the PK tuple
		result.NextKey = tpl[1:]
		result.Items = result.Items[:opts.Limit]
	}

	return result, nil
}
`
