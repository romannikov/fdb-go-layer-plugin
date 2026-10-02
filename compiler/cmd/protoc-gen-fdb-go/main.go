package main

import (
	"fmt"
	"os"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"

	"github.com/romannikov/fdb-layer/compiler/backend/golang"
	"github.com/romannikov/fdb-layer/compiler/ir"
)

func main() {
	protogen.Options{}.Run(func(plugin *protogen.Plugin) error {
		messages, err := ir.ParsePlugin(plugin)
		if err != nil {
			return err
		}

		for _, msg := range messages {
			fileName := msg.FilePrefix + "_" + strings.ToLower(msg.Name) + ".fdb.go"
			genFile := plugin.NewGeneratedFile(fileName, "")
			golang.GenerateMessage(genFile, msg)
			fmt.Fprintf(os.Stderr, "Generated %s\n", fileName)
		}

		return nil
	})
}
