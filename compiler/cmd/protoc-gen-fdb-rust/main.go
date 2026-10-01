package main

import (
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/romannikov/fdb-layer/compiler/backend/rust"
	"github.com/romannikov/fdb-layer/compiler/ir"
)

func main() {
	var flags flag.FlagSet
	generateMessages := flags.Bool("generate_messages", false, "Emit #[derive(::prost::Message)] struct definitions alongside repositories")

	opts := protogen.Options{
		ParamFunc: flags.Set,
	}

	opts.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)

		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}

			var annotated []ir.MessageSpec
			for _, msg := range f.Messages {
				spec, err := ir.ProcessMessage(msg, msg.Desc.Options())
				if err != nil {
					return fmt.Errorf("failed to parse message %s: %w", msg.Desc.Name(), err)
				}
				if spec != nil && len(spec.PrimaryKeyFields) > 0 {
					annotated = append(annotated, *spec)
				}
			}

			if len(annotated) == 0 {
				continue
			}

			protoPath := f.Desc.Path()
			ext := filepath.Ext(protoPath)
			stem := strings.TrimSuffix(protoPath, ext)
			filename := stem + ".fdb.rs"

			g := gen.NewGeneratedFile(filename, f.GoImportPath)
			rust.GenerateFileWithOptions(g, protoPath, annotated, rust.GenerateOptions{
				GenerateMessages: *generateMessages,
			})
		}

		return nil
	})
}
