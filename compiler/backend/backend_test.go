package backend

import (
	"reflect"
	"strings"
	"testing"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

type stubBackend struct {
	lang string
}

func (s *stubBackend) Language() string {
	return s.lang
}

func (s *stubBackend) GenerateFile(file *ir.FileSpec, opts Options) ([]GeneratedFile, error) {
	if file == nil || len(file.AnnotatedMessages()) == 0 {
		return nil, nil
	}
	var p BufferPrinter
	p.P("// language: ", s.lang)
	p.P("package ", file.GoPackageName)
	return []GeneratedFile{
		{
			Filename:     file.GeneratedFilenamePrefix + ".fdb." + s.lang,
			GoImportPath: file.GoImportPath,
			Content:      p.Bytes(),
		},
	}, nil
}

func TestRegistry_RegisterAndLookup(t *testing.T) {
	reg := NewRegistry()

	if err := reg.Register(nil); err == nil {
		t.Fatal("expected error registering nil generator")
	}
	if err := reg.Register(&stubBackend{lang: "  "}); err == nil {
		t.Fatal("expected error registering empty language generator")
	}

	goGen := &stubBackend{lang: "go"}
	if err := reg.Register(goGen); err != nil {
		t.Fatalf("unexpected error registering go backend: %v", err)
	}
	if err := reg.Register(&stubBackend{lang: "GO"}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("expected duplicate registration error, got %v", err)
	}

	got, ok := reg.Get("Go")
	if !ok || got != goGen {
		t.Fatalf("expected to resolve goGen, got %v (ok=%v)", got, ok)
	}
	if _, ok := reg.Get("unknown"); ok {
		t.Fatal("expected false for unknown language")
	}

	langs := reg.Languages()
	if !reflect.DeepEqual(langs, []string{"go"}) {
		t.Fatalf("expected languages [go], got %v", langs)
	}
}

func TestParseParams(t *testing.T) {
	opts := ParseParams("paths=source_relative, custom_flag = val , bare_flag")
	if opts.Params["paths"] != "source_relative" {
		t.Fatalf("expected paths=source_relative, got %q", opts.Params["paths"])
	}
	if opts.Params["custom_flag"] != "val" {
		t.Fatalf("expected custom_flag=val, got %q", opts.Params["custom_flag"])
	}
	if opts.Params["bare_flag"] != "true" {
		t.Fatalf("expected bare_flag=true, got %q", opts.Params["bare_flag"])
	}
}

func TestStubBackend_GenerateFile(t *testing.T) {
	gen := &stubBackend{lang: "go"}

	// Unannotated file returns nil, nil
	unannotated := &ir.FileSpec{
		GoPackageName:           "store",
		GeneratedFilenamePrefix: "store",
		Messages: []ir.MessageSpec{
			{Name: "Address"},
		},
	}
	out, err := gen.GenerateFile(unannotated, Options{})
	if err != nil || len(out) != 0 {
		t.Fatalf("expected no output for unannotated file, got %v, err=%v", out, err)
	}

	// Annotated file returns generated file
	annotated := &ir.FileSpec{
		GoPackageName:           "store",
		GoImportPath:            "example.com/store",
		GeneratedFilenamePrefix: "store",
		Messages: []ir.MessageSpec{
			{
				Name: "User",
				PrimaryKeyFields: []ir.FieldSpec{
					{ProtoName: "id", GoName: "Id", Kind: ir.KindString},
				},
			},
		},
	}
	out, err = gen.GenerateFile(annotated, Options{})
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1 generated file, got %v, err=%v", out, err)
	}
	if out[0].Filename != "store.fdb.go" {
		t.Fatalf("expected filename store.fdb.go, got %q", out[0].Filename)
	}
	if !strings.Contains(string(out[0].Content), "package store") {
		t.Fatalf("unexpected content: %s", string(out[0].Content))
	}
}
