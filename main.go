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
			fileName := fmt.Sprintf("%s_repository.go", strings.ToLower(msg.Name))
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

const fdbTemplate = `package repositories

import (
    "context"
    "fmt"

    "github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
    "github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"google.golang.org/protobuf/proto"
    pb "{{.GoPackagePath}}"
)

type {{.Name}}Repository struct {
    db  fdb.Database
    dir directory.DirectorySubspace
}

func New{{.Name}}Repository(db fdb.Database) (*{{.Name}}Repository, error) {
    dir, err := directory.CreateOrOpen(db, []string{"{{.Name}}"}, nil)
    if err != nil {
        return nil, err
    }
    return &{{.Name}}Repository{db: db, dir: dir}, nil
}

func (repo *{{.Name}}Repository) Get(ctx context.Context, tr fdb.ReadTransaction, {{range $index, $element := .PrimaryKeyFields}}{{if $index}}, {{end}}{{.Name}} {{.Type}}{{end}}) (*pb.{{.Name}}, error) {
    var entity *pb.{{.Name}}

    key := repo.dir.Pack(tuple.Tuple{ {{range .PrimaryKeyFields}} {{.Name}}, {{end}} })
    value := tr.Get(key).MustGet()
    if value == nil {
        return nil, fmt.Errorf("{{.Name}} not found")
    }
    entity = &pb.{{.Name}}{}
    err := proto.Unmarshal(value, entity)
    if err != nil {
        return nil, err
    }
    return entity, nil
}

func (repo *{{.Name}}Repository) Set(ctx context.Context, tr fdb.Transaction, entity *pb.{{.Name}}) error {
    key := repo.dir.Pack(tuple.Tuple{ {{range .PrimaryKeyFields}} entity.{{.Name}}, {{end}} })
    value, err := proto.Marshal(entity)
    if err != nil {
        return err
    }
    tr.Set(key, value)

    // Handle secondary indexes
    {{range $idxIndex, $idx := .SecondaryIndexes}}
    indexKey := repo.dir.Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{
        {{range $i, $f := $idx.Fields}} entity.{{ $f.Name }}, {{end}}
        {{range $.PrimaryKeyFields}} entity.{{.Name}}, {{end}}
    })
    tr.Set(indexKey, []byte{})
    {{end}}

    return nil
}

func (repo *{{.Name}}Repository) Delete(ctx context.Context, tr fdb.Transaction, {{range $index, $element := .PrimaryKeyFields}}{{if $index}}, {{end}}{{.Name}} {{.Type}}{{end}}) error {
    key := repo.dir.Pack(tuple.Tuple{ {{range .PrimaryKeyFields}} {{.Name}}, {{end}} })
    value := tr.Get(key).MustGet()
    if value != nil {
        entity := &pb.{{.Name}}{}
        err := proto.Unmarshal(value, entity)
        if err == nil {
            // Handle index cleanup
            {{range $idxIndex, $idx := .SecondaryIndexes}}
            indexKey := repo.dir.Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{
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

{{/* Generate GetBy methods for secondary indexes */}}
{{range $idxIndex, $idx := .SecondaryIndexes}}
func (repo *{{$.Name}}Repository) GetBy{{joinFieldNames $idx.Fields}}(ctx context.Context, tr fdb.ReadTransaction, {{range $i, $f := $idx.Fields}}{{if $i}}, {{end}}{{$f.Name}} {{$f.Type}}{{end}}) ([]*pb.{{$.Name}}, error) {
    entities := []*pb.{{$.Name}}{}

    indexKeyPrefix := repo.dir.Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{ {{range $i, $f := $idx.Fields}} {{$f.Name}}, {{end}} })
    indexRange, err := fdb.PrefixRange(indexKeyPrefix)
	if err != nil {
		return nil, err
	}
    kvs := tr.GetRange(indexRange, fdb.RangeOptions{}).GetSliceOrPanic()
    for _, kv := range kvs {
        tpl, err := repo.dir.Sub("{{joinFieldNames $idx.Fields}}_index").Unpack(kv.Key)
        if err != nil {
            return nil, err
        }
        // The primary key fields are after the index fields
        pkTuple := tpl[{{len $idx.Fields}}:] // Skip the index fields
        key := repo.dir.Pack(pkTuple)
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
