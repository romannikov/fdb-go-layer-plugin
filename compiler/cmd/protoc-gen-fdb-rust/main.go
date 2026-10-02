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

			var messages []ir.MessageSpec
			hasRepo := false
			for _, msg := range f.Messages {
				spec, err := ir.ProcessMessageAll(msg, msg.Desc.Options())
				if err != nil {
					return fmt.Errorf("failed to parse message %s: %w", msg.Desc.Name(), err)
				}
				if spec == nil {
					continue
				}
				if spec.HasRepository() {
					hasRepo = true
				}
				if *generateMessages || spec.HasRepository() {
					messages = append(messages, *spec)
				}
			}

			if !hasRepo {
				continue
			}

			var topEnums []ir.EnumSpec
			if *generateMessages {
				for _, e := range f.Enums {
					topEnums = append(topEnums, ir.ProcessEnum(e))
				}
			}

			protoPath := f.Desc.Path()
			ext := filepath.Ext(protoPath)
			stem := strings.TrimSuffix(protoPath, ext)
			filename := stem + ".fdb.rs"

			g := gen.NewGeneratedFile(filename, f.GoImportPath)
			rust.GenerateFileWithOptions(g, protoPath, messages, rust.GenerateOptions{
				GenerateMessages: *generateMessages,
				Enums:            topEnums,
			})
		}

		return nil
	})
}
