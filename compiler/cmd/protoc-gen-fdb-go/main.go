package main

import (
	"google.golang.org/protobuf/compiler/protogen"

	"github.com/romannikov/fdb-layer/compiler/backend"
	"github.com/romannikov/fdb-layer/compiler/backend/golang"
)

func main() {
	protogen.Options{}.Run(func(plugin *protogen.Plugin) error {
		return backend.RunProtogen(plugin, golang.NewBackend(), backend.Options{})
	})
}
