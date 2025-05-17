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
}

func main() {
	protogen.Options{}.Run(func(plugin *protogen.Plugin) error {
		messages := []Message{}
		processedMessages := make(map[string]bool) // To track processed messages

		for _, file := range plugin.Files {
			if !file.Generate {
				continue
			}

			goPackagePath := string(file.GoImportPath)

			for _, message := range file.Messages {
				msgName := message.GoIdent.GoName

				// Skip if already processed
				if processedMessages[msgName] {
					continue
				}
				processedMessages[msgName] = true

				msgOptions := message.Desc.Options()
				processedMessage := processMessage(message, msgOptions)
				if processedMessage != nil {
					processedMessage.GoPackagePath = goPackagePath
					messages = append(messages, *processedMessage)
				}
			}
		}

		// Generate code for each message
		tmpl := template.Must(template.New("fdb").Funcs(template.FuncMap{
			"joinFieldNames": joinFieldNames,
		}).Parse(fdbTemplate))

		for _, msg := range messages {
			// Create a new generated file
			fileName := fmt.Sprintf("%s.go", strings.ToLower(msg.Name))
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

func processMessage(message *protogen.Message, msgOptions proto.Message) *Message {
	primaryKeyFields := []Field{}
	secondaryIndexes := []SecondaryIndex{}

	msgName := message.GoIdent.GoName

	// Build a map of fields for easy lookup
	fieldMap := make(map[string]*protogen.Field)
	fields := []Field{}
	for _, field := range message.Fields {
		fieldName := field.Desc.Name()
		fieldMap[string(fieldName)] = field
		fields = append(fields, Field{
			Name: field.GoName,
			Type: goType(field.Desc.Kind()),
		})
	}

	// Collect primary key fields
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
				Type: goType(field.Desc.Kind()),
			})
		} else {
			log.Fatalf("Primary key field %s not found in message %s", pkName, msgName)
		}
	}

	// Collect secondary indexes
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
								Type: goType(field.Desc.Kind()),
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
							Type: goType(field.Desc.Kind()),
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

func goType(kind protoreflect.Kind) string {
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

func joinFieldNames(fields []Field) string {
	names := []string{}
	for _, f := range fields {
		names = append(names, f.Name)
	}
	return strings.Join(names, "And")
}

const fdbTemplate = `// Code generated by fdb-go-layer-plugin. DO NOT EDIT.
// Source: {{.GoPackagePath}}

package db

import (
    "fmt"

    "github.com/apple/foundationdb/bindings/go/src/fdb"
    "github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
    "github.com/apple/foundationdb/bindings/go/src/fdb/directory"
    "google.golang.org/protobuf/proto"
    pb "{{.GoPackagePath}}"
)

// Create{{.Name}} creates a new {{.Name}} entity in the database.
// Parameters:
//   - tr: FoundationDB transaction
//   - dir: directory subspace for the entity
//   - entity: the {{.Name}} entity to create
func Create{{.Name}}(tr fdb.Transaction, dir directory.DirectorySubspace, entity *pb.{{.Name}}) error {
    key := dir.Sub("{{.Name}}").Pack(tuple.Tuple{ {{range .PrimaryKeyFields}} entity.{{.Name}}, {{end}} })
    value, err := proto.Marshal(entity)
    if err != nil {
        return err
    }
    tr.Set(key, value)

    {{range $idxIndex, $idx := .SecondaryIndexes}}
    {{if eq $idxIndex 0}}
    indexKey := dir.Sub("{{$.Name}}").Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{
    {{else}}
    indexKey = dir.Sub("{{$.Name}}").Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{
    {{end}}
        {{range $i, $f := $idx.Fields}} entity.{{ $f.Name }}, {{end}}
        {{range $.PrimaryKeyFields}} entity.{{.Name}}, {{end}}
    })
    tr.Set(indexKey, []byte{})
    {{end}}

    return nil
}

// Get{{.Name}} retrieves a {{.Name}} entity by its primary key.
// Parameters:
//   - tr: FoundationDB read transaction
//   - dir: directory subspace for the entity
//   {{range .PrimaryKeyFields}}//   - {{.Name}}: primary key field {{.Name}} of type {{.Type}}
//   {{end}}
func Get{{.Name}}(tr fdb.ReadTransaction, dir directory.DirectorySubspace, {{range $index, $element := .PrimaryKeyFields}}{{if $index}}, {{end}}{{.Name}} {{.Type}}{{end}}) (*pb.{{.Name}}, error) {
    key := dir.Sub("{{.Name}}").Pack(tuple.Tuple{ {{range .PrimaryKeyFields}} {{.Name}}, {{end}} })
    value := tr.Get(key).MustGet()
    if value == nil {
        return nil, fmt.Errorf("{{.Name}} not found")
    }
    entity := &pb.{{.Name}}{}
    err := proto.Unmarshal(value, entity)
    if err != nil {
        return nil, err
    }
    return entity, nil
}

// Set{{.Name}} updates an existing {{.Name}} entity in the database.
// Parameters:
//   - tr: FoundationDB transaction
//   - dir: directory subspace for the entity
//   - entity: the {{.Name}} entity to update
func Set{{.Name}}(tr fdb.Transaction, dir directory.DirectorySubspace, entity *pb.{{.Name}}) error {
    key := dir.Sub("{{.Name}}").Pack(tuple.Tuple{ {{range .PrimaryKeyFields}} entity.{{.Name}}, {{end}} })
    value, err := proto.Marshal(entity)
    if err != nil {
        return err
    }
    tr.Set(key, value)

    {{range $idxIndex, $idx := .SecondaryIndexes}}
    {{if eq $idxIndex 0}}
    indexKey := dir.Sub("{{$.Name}}").Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{
    {{else}}
    indexKey = dir.Sub("{{$.Name}}").Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{
    {{end}}
        {{range $i, $f := $idx.Fields}} entity.{{ $f.Name }}, {{end}}
        {{range $.PrimaryKeyFields}} entity.{{.Name}}, {{end}}
    })
    tr.Set(indexKey, []byte{})
    {{end}}

    return nil
}

// Delete{{.Name}} removes a {{.Name}} entity from the database.
// Parameters:
//   - tr: FoundationDB transaction
//   - dir: directory subspace for the entity
//   {{range .PrimaryKeyFields}}//   - {{.Name}}: primary key field {{.Name}} of type {{.Type}}
//   {{end}}
func Delete{{.Name}}(tr fdb.Transaction, dir directory.DirectorySubspace, {{range $index, $element := .PrimaryKeyFields}}{{if $index}}, {{end}}{{.Name}} {{.Type}}{{end}}) error {
    key := dir.Sub("{{.Name}}").Pack(tuple.Tuple{ {{range .PrimaryKeyFields}} {{.Name}}, {{end}} })
    value := tr.Get(key).MustGet()
    if value != nil {
        entity := &pb.{{.Name}}{}
        err := proto.Unmarshal(value, entity)
        if err == nil {
            {{range $idxIndex, $idx := .SecondaryIndexes}}
            {{if eq $idxIndex 0}}
            indexKey := dir.Sub("{{$.Name}}").Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{
            {{else}}
            indexKey = dir.Sub("{{$.Name}}").Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{
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
// Parameters:
//   - tr: FoundationDB read transaction
//   - dir: directory subspace for the entity
//   {{range $i, $f := $idx.Fields}}//   - {{$f.Name}}: index field {{$f.Name}} of type {{$f.Type}}
//   {{end}}
func Get{{$.Name}}By{{joinFieldNames $idx.Fields}}(tr fdb.ReadTransaction, dir directory.DirectorySubspace, {{range $i, $f := $idx.Fields}}{{if $i}}, {{end}}{{$f.Name}} {{$f.Type}}{{end}}) ([]*pb.{{$.Name}}, error) {
    entities := []*pb.{{$.Name}}{}

    indexKeyPrefix := dir.Sub("{{$.Name}}").Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{ {{range $i, $f := $idx.Fields}} {{$f.Name}}, {{end}} })
    indexRange, err := fdb.PrefixRange(indexKeyPrefix)
    if err != nil {
        return nil, err
    }
    kvs := tr.GetRange(indexRange, fdb.RangeOptions{}).GetSliceOrPanic()
    for _, kv := range kvs {
        tpl, err := dir.Sub("{{$.Name}}").Sub("{{joinFieldNames $idx.Fields}}_index").Unpack(kv.Key)
        if err != nil {
            return nil, err
        }
        pkTuple := tpl[{{len $idx.Fields}}:]
        key := dir.Sub("{{$.Name}}").Pack(pkTuple)
        value := tr.Get(key).MustGet()
        if value == nil {
            continue
        }
        entity := &pb.{{$.Name}}{}
        err = proto.Unmarshal(value, entity)
        if err != nil {
            return nil, err
        }
        entities = append(entities, entity)
    }
    return entities, nil
}
{{end}}
`
