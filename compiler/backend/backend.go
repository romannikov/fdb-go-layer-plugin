package backend

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/compiler/protogen"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

// Printer is the minimal line-oriented code emission interface shared by
// *protogen.GeneratedFile and in-memory code generation buffers.
type Printer interface {
	P(v ...any)
}

// BufferPrinter is an in-memory implementation of Printer used by language backends
// and unit tests.
type BufferPrinter struct {
	buf bytes.Buffer
}

// P writes a line of output by concatenating all arguments with fmt.Sprint and appending a newline.
func (b *BufferPrinter) P(v ...any) {
	for _, x := range v {
		fmt.Fprint(&b.buf, x)
	}
	b.buf.WriteByte('\n')
}

// Bytes returns the accumulated output bytes.
func (b *BufferPrinter) Bytes() []byte {
	return b.buf.Bytes()
}

// String returns the accumulated output as a string.
func (b *BufferPrinter) String() string {
	return b.buf.String()
}

// Options carries target-specific flags parsed from --fdb-<lang>_opt=...
type Options struct {
	Params map[string]string
}

// ParseParams parses a comma-separated protoc plugin parameter string (e.g. "k1=v1,k2=v2")
// into an Options struct.
func ParseParams(raw string) Options {
	params := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if k, v, ok := strings.Cut(part, "="); ok {
			params[strings.TrimSpace(k)] = strings.TrimSpace(v)
		} else {
			params[part] = "true"
		}
	}
	return Options{Params: params}
}

// GeneratedFile represents a single output source file produced by a language backend.
type GeneratedFile struct {
	// Filename is the relative output path for protoc (e.g., "path/to/store.fdb.go").
	Filename string
	// GoImportPath is the Go import path when applicable (empty for non-Go targets).
	GoImportPath string
	// Content is the UTF-8 encoded source code of the generated file.
	Content []byte
}

// Generator is the common interface implemented by every target language backend
// (e.g., golang, and future targets such as rust, python, or typescript).
type Generator interface {
	// Language returns the canonical target language identifier (e.g., "go").
	Language() string

	// GenerateFile transforms a validated language-neutral IR FileSpec into zero or more
	// target-language source files. If a file contains no FDB-annotated messages and the
	// backend does not emit standalone types, GenerateFile returns (nil, nil).
	GenerateFile(file *ir.FileSpec, opts Options) ([]GeneratedFile, error)
}

// Registry maintains a thread-safe mapping of language identifiers to Generator implementations.
type Registry struct {
	mu       sync.RWMutex
	backends map[string]Generator
}

// NewRegistry creates an empty backend Registry.
func NewRegistry() *Registry {
	return &Registry{
		backends: make(map[string]Generator),
	}
}

// Register adds a Generator to the registry. It returns an error if gen is nil,
// has an empty Language(), or if a backend for that language is already registered.
func (r *Registry) Register(gen Generator) error {
	if gen == nil {
		return fmt.Errorf("cannot register nil backend generator")
	}
	lang := strings.TrimSpace(strings.ToLower(gen.Language()))
	if lang == "" {
		return fmt.Errorf("cannot register backend generator with empty language identifier")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.backends[lang]; exists {
		return fmt.Errorf("backend generator for language %q is already registered", lang)
	}
	r.backends[lang] = gen
	return nil
}

// Get looks up a registered Generator by language identifier (case-insensitive).
func (r *Registry) Get(lang string) (Generator, bool) {
	key := strings.TrimSpace(strings.ToLower(lang))
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.backends[key]
	return g, ok
}

// Languages returns a sorted list of all registered language identifiers.
func (r *Registry) Languages() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	langs := make([]string, 0, len(r.backends))
	for k := range r.backends {
		langs = append(langs, k)
	}
	sort.Strings(langs)
	return langs
}

// RunProtogen executes a Generator inside a standard protoc-gen-fdb-<lang> plugin entrypoint.
func RunProtogen(plugin *protogen.Plugin, gen Generator, opts Options) error {
	if plugin == nil {
		return fmt.Errorf("protogen.Plugin cannot be nil")
	}
	if gen == nil {
		return fmt.Errorf("backend.Generator cannot be nil")
	}
	if opts.Params == nil && plugin.Request != nil {
		opts = ParseParams(plugin.Request.GetParameter())
	}

	processedFiles := make(map[string]bool, len(plugin.Files))
	for _, f := range plugin.Files {
		if !f.Generate {
			continue
		}
		fileKey := f.GeneratedFilenamePrefix
		if f.Desc != nil {
			fileKey = f.Desc.Path()
		}
		if processedFiles[fileKey] {
			continue
		}
		processedFiles[fileKey] = true

		fileSpec, err := ir.ParseFile(f)
		if err != nil {
			return err
		}
		outputs, err := gen.GenerateFile(fileSpec, opts)
		if err != nil {
			return err
		}
		for _, out := range outputs {
			gf := plugin.NewGeneratedFile(out.Filename, protogen.GoImportPath(out.GoImportPath))
			if _, err := gf.Write(out.Content); err != nil {
				return fmt.Errorf("writing generated file %q: %w", out.Filename, err)
			}
		}
	}
	return nil
}
