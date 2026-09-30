package main

import (
	"fmt"
	"os"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
)

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

		for _, msg := range messages {
			fileName := msg.FilePrefix + "_" + strings.ToLower(msg.Name) + ".go"
			genFile := plugin.NewGeneratedFile(fileName, "")
			GenerateMessage(genFile, msg)
			fmt.Fprintf(os.Stderr, "Generated %s\n", fileName)
		}

		return nil
	})
}
