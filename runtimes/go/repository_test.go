package fdblayer

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type testDirSubspace struct {
	subspace.Subspace
}

func newTestDir() testDirSubspace {
	return testDirSubspace{Subspace: subspace.FromBytes([]byte("test"))}
}

func (d testDirSubspace) GetPath() []string { return []string{"test"} }
func (d testDirSubspace) GetLayer() []byte  { return nil }
func (d testDirSubspace) CreateOrOpen(fdb.Transactor, []string, []byte) (directory.DirectorySubspace, error) {
	return d, nil
}
func (d testDirSubspace) Open(fdb.ReadTransactor, []string, []byte) (directory.DirectorySubspace, error) {
	return d, nil
}
func (d testDirSubspace) Create(fdb.Transactor, []string, []byte) (directory.DirectorySubspace, error) {
	return d, nil
}
func (d testDirSubspace) CreatePrefix(fdb.Transactor, []string, []byte, []byte) (directory.DirectorySubspace, error) {
	return d, nil
}
func (d testDirSubspace) Move(fdb.Transactor, []string, []string) (directory.DirectorySubspace, error) {
	return d, nil
}
func (d testDirSubspace) MoveTo(fdb.Transactor, []string) (directory.DirectorySubspace, error) {
	return d, nil
}
func (d testDirSubspace) Remove(fdb.Transactor, []string) (bool, error) { return true, nil }
func (d testDirSubspace) Exists(fdb.ReadTransactor, []string) (bool, error) {
	return true, nil
}
func (d testDirSubspace) List(fdb.ReadTransactor, []string) ([]string, error) {
	return nil, nil
}

type customSignedEnum int32
type customUnsignedID uint32

func TestEncodeAndDecodeAggregateValue_ExtremesAndErrors(t *testing.T) {
	// 1. Delta encoding (COUNT / SUM / MUTATION_ADD / MUTATION_COUNT)
	if got, err := DecodeAggregateValue(nil, AggregateEncodingDelta, "item"); err != nil || got != 0 {
		t.Fatalf("expected (0, nil) for nil delta value, got (%d, %v)", got, err)
	}
	for _, v := range []int64{math.MinInt64, -1000000, -1, 0, 1, 1000000, math.MaxInt64} {
		enc := EncodeDeltaLE(v)
		got, err := DecodeAggregateValue(enc, AggregateEncodingDelta, "item")
		if err != nil || got != v {
			t.Fatalf("EncodeDeltaLE/DecodeAggregateValue(%d) = (%d, %v)", v, got, err)
		}
	}

	// 2. Signed order encoding (AGGREGATE_MIN / AGGREGATE_MAX / MUTATION_MIN / MUTATION_MAX)
	if _, err := DecodeAggregateValue(nil, AggregateEncodingSignedMinMax, "item"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for nil signed min/max value, got %v", err)
	}
	orderedSigned := []int64{math.MinInt64, -999999, -1, 0, 1, 999999, math.MaxInt64}
	for i, v := range orderedSigned {
		enc := EncodeSignedOrderLE(v)
		got, err := DecodeAggregateValue(enc, AggregateEncodingSignedMinMax, "item")
		if err != nil || got != v {
			t.Fatalf("EncodeSignedOrderLE/DecodeAggregateValue(%d) = (%d, %v)", v, got, err)
		}
		if i > 0 {
			prevEnc := EncodeSignedOrderLE(orderedSigned[i-1])
			// Little-endian unsigned comparison (from most-significant byte 7 down to 0) must preserve signed order
			cmp := 0
			for b := 7; b >= 0; b-- {
				if prevEnc[b] != enc[b] {
					if prevEnc[b] < enc[b] {
						cmp = -1
					} else {
						cmp = 1
					}
					break
				}
			}
			if cmp >= 0 {
				t.Fatalf("expected EncodeSignedOrderLE(%d) < EncodeSignedOrderLE(%d) in unsigned LE order", orderedSigned[i-1], v)
			}
		}
	}

	// 3. Unsigned encoding (uint32 / uint64 AGGREGATE_MIN / AGGREGATE_MAX)
	if _, err := DecodeAggregateValue(nil, AggregateEncodingUintMinMax, "item"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for nil unsigned min/max value, got %v", err)
	}
	for _, u := range []uint64{0, 1, math.MaxUint32, math.MaxInt64, math.MaxUint64} {
		enc := EncodeUnsignedLE(u)
		got, err := DecodeAggregateValue(enc, AggregateEncodingUintMinMax, "item")
		if err != nil || uint64(got) != u {
			t.Fatalf("EncodeUnsignedLE/DecodeAggregateValue(%d) = (%d, %v)", u, uint64(got), err)
		}
	}

	// 4. Error branches: corrupted length != 8 and unknown AggregateEncoding
	if _, err := DecodeAggregateValue([]byte{1, 2, 3}, AggregateEncodingDelta, "item"); err == nil {
		t.Fatal("expected error for non-8-byte aggregate value")
	}
	if _, err := DecodeAggregateValue(make([]byte, 8), AggregateEncoding(99), "item"); err == nil {
		t.Fatal("expected error for unknown AggregateEncoding")
	}
}

func TestNormalizeElements_AllScalarAndCompositeKinds(t *testing.T) {
	if NormalizeTupleElement(nil) != nil {
		t.Fatal("expected nil for NormalizeTupleElement(nil)")
	}
	if got := NormalizeTupleElement(int(7)); got != int64(7) {
		t.Fatalf("expected int64(7), got %T(%v)", got, got)
	}
	if got := NormalizeTupleElement(int8(-8)); got != int64(-8) {
		t.Fatalf("expected int64(-8), got %T(%v)", got, got)
	}
	if got := NormalizeTupleElement(int16(-16)); got != int64(-16) {
		t.Fatalf("expected int64(-16), got %T(%v)", got, got)
	}
	if got := NormalizeTupleElement(int32(-32)); got != int64(-32) {
		t.Fatalf("expected int64(-32), got %T(%v)", got, got)
	}
	if got := NormalizeTupleElement(uint(7)); got != uint64(7) {
		t.Fatalf("expected uint64(7), got %T(%v)", got, got)
	}
	if got := NormalizeTupleElement(uint8(8)); got != uint64(8) {
		t.Fatalf("expected uint64(8), got %T(%v)", got, got)
	}
	if got := NormalizeTupleElement(uint16(16)); got != uint64(16) {
		t.Fatalf("expected uint64(16), got %T(%v)", got, got)
	}
	if got := NormalizeTupleElement(uint32(32)); got != uint64(32) {
		t.Fatalf("expected uint64(32), got %T(%v)", got, got)
	}
	if got := NormalizeTupleElement(customSignedEnum(3)); got != int64(3) {
		t.Fatalf("expected int64(3), got %T(%v)", got, got)
	}
	if got := NormalizeTupleElement(customUnsignedID(9)); got != uint64(9) {
		t.Fatalf("expected uint64(9), got %T(%v)", got, got)
	}
	nested := NormalizeTupleElement(tuple.Tuple{int32(1), uint16(2)}).(tuple.Tuple)
	if nested[0] != int64(1) || nested[1] != uint64(2) {
		t.Fatalf("unexpected normalized nested tuple: %+v", nested)
	}

	// normalizeElementWithKind coverage for all FieldKinds
	for _, in := range []any{int(-1), int8(-2), int16(-3), int32(-4), int64(-5), customSignedEnum(6)} {
		if _, err := normalizeElementWithKind(in, FieldKindInt64); err != nil {
			t.Fatalf("normalizeElementWithKind(%T, FieldKindInt64) failed: %v", in, err)
		}
	}
	if _, err := normalizeElementWithKind("bad", FieldKindInt64); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for string on FieldKindInt64, got %v", err)
	}

	for _, in := range []any{uint(1), uint8(2), uint16(3), uint32(4), uint64(5), int(6), int64(7), customUnsignedID(8)} {
		if _, err := normalizeElementWithKind(in, FieldKindUint64); err != nil {
			t.Fatalf("normalizeElementWithKind(%T, FieldKindUint64) failed: %v", in, err)
		}
	}
	for _, bad := range []any{int(-1), int64(-5), "bad"} {
		if _, err := normalizeElementWithKind(bad, FieldKindUint64); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("expected ErrInvalidKey for %v on FieldKindUint64, got %v", bad, err)
		}
	}

	if got, err := normalizeElementWithKind(float32(1.5), FieldKindFloat32); err != nil || got != float32(1.5) {
		t.Fatalf("FieldKindFloat32 failed: %v, %v", got, err)
	}
	if _, err := normalizeElementWithKind(float64(1.5), FieldKindFloat32); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for float64 on FieldKindFloat32, got %v", err)
	}

	if got, err := normalizeElementWithKind(float64(2.5), FieldKindFloat64); err != nil || got != float64(2.5) {
		t.Fatalf("FieldKindFloat64(float64) failed: %v, %v", got, err)
	}
	if got, err := normalizeElementWithKind(float32(2.5), FieldKindFloat64); err != nil || got != float64(2.5) {
		t.Fatalf("FieldKindFloat64(float32) failed: %v, %v", got, err)
	}
	if _, err := normalizeElementWithKind("bad", FieldKindFloat64); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for string on FieldKindFloat64, got %v", err)
	}

	if got, err := normalizeElementWithKind(true, FieldKindBool); err != nil || got != true {
		t.Fatalf("FieldKindBool failed: %v, %v", got, err)
	}
	if _, err := normalizeElementWithKind(1, FieldKindBool); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for int on FieldKindBool, got %v", err)
	}

	if _, err := normalizeElementWithKind(1, FieldKindString); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for int on FieldKindString, got %v", err)
	}
	if _, err := normalizeElementWithKind("not-bytes", FieldKindBytes); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for string on FieldKindBytes, got %v", err)
	}

	// Default fallback to normalizeAndValidateElement
	if got, err := normalizeElementWithKind(nil, FieldKind(0)); err != nil || got != nil {
		t.Fatalf("expected (nil, nil), got (%v, %v)", got, err)
	}
	if _, err := normalizeElementWithKind(tuple.Tuple{"a", int32(1)}, FieldKind(0)); err != nil {
		t.Fatalf("unexpected error for valid nested tuple: %v", err)
	}
	if _, err := normalizeElementWithKind(tuple.IncompleteVersionstamp(0), FieldKind(0)); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for incomplete versionstamp, got %v", err)
	}
	if _, err := normalizeElementWithKind(struct{}{}, FieldKind(0)); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for unsupported struct, got %v", err)
	}
}

func TestVersionstampAndCursorHelpers(t *testing.T) {
	completeVS := tuple.Versionstamp{
		TransactionVersion: [10]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		UserVersion:        42,
	}
	gotVS, err := PackVersionstampElement(completeVS)
	if err != nil || gotVS != completeVS {
		t.Fatalf("PackVersionstampElement(completeVS) failed: %v, %v", gotVS, err)
	}
	if _, err := PackVersionstampElement(tuple.IncompleteVersionstamp(0)); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for incomplete tuple.Versionstamp, got %v", err)
	}
	if _, err := PackVersionstampElement(12345); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for int in PackVersionstampElement, got %v", err)
	}

	if got := ExtractCursorKey(tuple.Tuple{"a", "b"}, 2); got != nil {
		t.Fatalf("expected nil when len(tpl) <= startIdx, got %+v", got)
	}
	cur := ExtractCursorKey(tuple.Tuple{"prefix", completeVS, "tail"}, 1)
	if len(cur) != 2 || !bytes.Equal(cur[0].([]byte), completeVS.Bytes()) || cur[1] != "tail" {
		t.Fatalf("unexpected ExtractCursorKey result: %+v", cur)
	}
}

func TestResolveScanBoundsAndPackSubspaceTuple_ErrorPaths(t *testing.T) {
	dir := newTestDir()
	baseTpl := tuple.Tuple{int64(1), DataNamespace}
	schema := []FieldKind{FieldKindString, FieldKindInt64}

	// PackSubspaceTuple length overflow
	if _, err := PackSubspaceTuple(dir, baseTpl, Key{"a", int64(1), "extra"}, schema); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for PackSubspaceTuple length overflow, got %v", err)
	}

	// ResolveScanBounds validation errors
	invalidRanges := []struct {
		name string
		kr   KeyRange
		opts []ScanOption
	}{
		{
			name: "both_prefix_and_begin",
			kr:   KeyRange{Prefix: Key{"a"}, Begin: Key{"a"}},
		},
		{
			name: "prefix_too_long",
			kr:   Prefix("a", int64(1), "extra"),
		},
		{
			name: "begin_too_long",
			kr:   Range(Key{"a", int64(1), "extra"}, nil),
		},
		{
			name: "end_too_long",
			kr:   Range(nil, Key{"a", int64(1), "extra"}),
		},
		{
			name: "cursor_too_long",
			kr:   AllKeys(),
			opts: []ScanOption{WithCursor(Key{"a", int64(1), "extra"})},
		},
		{
			name: "invalid_prefix_type",
			kr:   Prefix(123),
		},
		{
			name: "invalid_begin_type",
			kr:   Range(Key{123}, nil),
		},
		{
			name: "invalid_end_type",
			kr:   Range(nil, Key{123}),
		},
		{
			name: "invalid_cursor_type",
			kr:   AllKeys(),
			opts: []ScanOption{WithCursor(Key{123})},
		},
	}

	for _, tc := range invalidRanges {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := ResolveScanBounds(dir, baseTpl, tc.kr, tc.opts, schema); !errors.Is(err, ErrInvalidKey) {
				t.Fatalf("expected ErrInvalidKey for %s, got %v", tc.name, err)
			}
		})
	}
}

