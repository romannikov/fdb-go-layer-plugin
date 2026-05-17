package store

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// MockKV – in-memory sorted key-value store backing all mock FDB operations.
type MockKV struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMockKV() *MockKV {
	return &MockKV{data: make(map[string][]byte)}
}

func (m *MockKV) set(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := make([]byte, len(value))
	copy(v, value)
	m.data[string(key)] = v
}

func (m *MockKV) get(key []byte) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[string(key)]
	if !ok {
		return nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

func (m *MockKV) clear(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
}

// rangeSlice returns all KVs whose keys are in [begin, end).
func (m *MockKV) rangeSlice(begin, end []byte, opts fdb.RangeOptions) []fdb.KeyValue {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect matching keys
	type kv struct {
		key   string
		value []byte
	}
	var pairs []kv
	for k, v := range m.data {
		kb := []byte(k)
		if bytes.Compare(kb, begin) >= 0 && bytes.Compare(kb, end) < 0 {
			pairs = append(pairs, kv{k, v})
		}
	}

	// Sort by key
	if opts.Reverse {
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].key > pairs[j].key })
	} else {
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].key < pairs[j].key })
	}

	// Apply limit
	if opts.Limit > 0 && len(pairs) > opts.Limit {
		pairs = pairs[:opts.Limit]
	}

	result := make([]fdb.KeyValue, len(pairs))
	for i, p := range pairs {
		result[i] = fdb.KeyValue{
			Key:   fdb.Key(p.key),
			Value: p.value,
		}
	}
	return result
}

func (m *MockKV) HasKey(key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[string(key)]
	return ok
}

func (m *MockKV) KeyCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

// Snapshot returns a copy of all data for debugging.
func (m *MockKV) Snapshot() map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string][]byte, len(m.data))
	for k, v := range m.data {
		out[k] = v
	}
	return out
}

// MockFutureByteSlice – satisfies fdb.FutureByteSlice
type MockFutureByteSlice struct {
	value []byte
}

func (f *MockFutureByteSlice) Get() ([]byte, error) { return f.value, nil }
func (f *MockFutureByteSlice) MustGet() []byte      { return f.value }
func (f *MockFutureByteSlice) BlockUntilReady()     {}
func (f *MockFutureByteSlice) IsReady() bool        { return true }
func (f *MockFutureByteSlice) Cancel()              {}

// MockRangeResult – wraps a slice of KeyValue to satisfy range iteration.
// Because fdb.RangeResult is a concrete struct with unexported fields,
// we cannot construct one with data. Instead, we provide this helper type
// that the MockTransaction uses internally.
type MockRangeResult struct {
	kvs   []fdb.KeyValue
	index int
}

func (r *MockRangeResult) GetSliceOrPanic() []fdb.KeyValue {
	return r.kvs
}

func (r *MockRangeResult) Iterator() *MockRangeIterator {
	return &MockRangeIterator{kvs: r.kvs, index: -1}
}

type MockRangeIterator struct {
	kvs   []fdb.KeyValue
	index int
}

func (ri *MockRangeIterator) Advance() bool {
	ri.index++
	return ri.index < len(ri.kvs)
}

func (ri *MockRangeIterator) MustGet() fdb.KeyValue {
	return ri.kvs[ri.index]
}

// MockTransaction – implements the generated Transaction interface.
// Embeds MockReadTransaction for the fdb.ReadTransaction requirement.
type MockTransaction struct {
	kv *MockKV
}

func NewMockTransaction(kv *MockKV) *MockTransaction {
	return &MockTransaction{kv: kv}
}

func (m *MockTransaction) Set(key fdb.KeyConvertible, value []byte) {
	m.kv.set(key.FDBKey(), value)
}

func (m *MockTransaction) Clear(key fdb.KeyConvertible) {
	m.kv.clear(key.FDBKey())
}

func (m *MockTransaction) AtomicOp(key fdb.KeyConvertible, mutationType fdb.MutationType, param []byte) {
	k := key.FDBKey()
	m.kv.mu.Lock()
	defer m.kv.mu.Unlock()

	current := m.kv.data[string(k)]

	switch mutationType {
	case fdb.MutationTypeAdd:
		var currentVal uint64
		if len(current) >= 8 {
			currentVal = binary.LittleEndian.Uint64(current)
		}
		delta := binary.LittleEndian.Uint64(param)
		newVal := currentVal + delta
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, newVal)
		m.kv.data[string(k)] = buf

	case fdb.MutationTypeMax:
		var currentVal uint64
		if len(current) >= 8 {
			currentVal = binary.LittleEndian.Uint64(current)
		}
		val := binary.LittleEndian.Uint64(param)
		if val > currentVal {
			m.kv.data[string(k)] = param
		}

	case fdb.MutationTypeMin:
		var currentVal uint64
		if len(current) >= 8 {
			currentVal = binary.LittleEndian.Uint64(current)
		} else {
			currentVal = ^uint64(0)
		}
		val := binary.LittleEndian.Uint64(param)
		if val < currentVal {
			m.kv.data[string(k)] = param
		}
	}
}

func (m *MockTransaction) Get(key fdb.KeyConvertible) fdb.FutureByteSlice {
	return &MockFutureByteSlice{value: m.kv.get(key.FDBKey())}
}



// GetRange returns a zero-value fdb.RangeResult. This means GetSliceOrPanic()
// returns an empty slice, and Iterator() produces no results.
//
// For tests that need to verify range-based queries with actual data,
// use GetRangeSlice() or test via integration with a real FDB instance.
func (m *MockTransaction) GetRange(r fdb.Range, options fdb.RangeOptions) fdb.RangeResult {
	return fdb.RangeResult{}
}

// GetRangeSlice is a test helper that performs the same key range scan
// as GetRange but returns data directly from the mock KV store.
// Use this in tests that need to verify range query results.
func (m *MockTransaction) GetRangeSlice(r fdb.Range, options fdb.RangeOptions) []fdb.KeyValue {
	begin, end := r.FDBRangeKeySelectors()
	beginKey := begin.FDBKeySelector().Key.FDBKey()
	endKey := end.FDBKeySelector().Key.FDBKey()
	return m.kv.rangeSlice(beginKey, endKey, options)
}

// Stubs for the remaining fdb.ReadTransaction interface methods.
// These are not used by the generated code but are required by the interface.

func (m *MockTransaction) GetKey(sel fdb.Selectable) fdb.FutureKey                     { return nil }
func (m *MockTransaction) GetReadVersion() fdb.FutureInt64                             { return nil }
func (m *MockTransaction) GetDatabase() fdb.Database                                   { return fdb.Database{} }
func (m *MockTransaction) Snapshot() fdb.Snapshot                                      { return fdb.Snapshot{} }
func (m *MockTransaction) GetEstimatedRangeSizeBytes(r fdb.ExactRange) fdb.FutureInt64 { return nil }
func (m *MockTransaction) GetRangeSplitPoints(r fdb.ExactRange, chunkSize int64) fdb.FutureKeyArray {
	return nil
}
func (m *MockTransaction) Options() fdb.TransactionOptions { return fdb.TransactionOptions{} }
func (m *MockTransaction) ReadTransact(f func(fdb.ReadTransaction) (interface{}, error)) (interface{}, error) {
	return f(m)
}

// MockDirectorySubspace – uses tuple packing with no prefix for simplicity.
// Satisfies directory.DirectorySubspace.
type MockDirectorySubspace struct {
	directory.DirectorySubspace
}

func (m *MockDirectorySubspace) Pack(t tuple.Tuple) fdb.Key {
	return t.Pack()
}

func (m *MockDirectorySubspace) Unpack(k fdb.KeyConvertible) (tuple.Tuple, error) {
	return tuple.Unpack(k.FDBKey())
}

func (m *MockDirectorySubspace) FDBKey() fdb.Key {
	return fdb.Key{}
}

func (m *MockDirectorySubspace) FDBRangeKeySelectors() (fdb.Selectable, fdb.Selectable) {
	begin := fdb.Key([]byte{0x00})
	end := fdb.Key([]byte{0xFF})
	return fdb.FirstGreaterOrEqual(begin), fdb.FirstGreaterOrEqual(end)
}

// Test helper: syncAndSetup creates a RecordStore, syncs metadata, and
// returns everything needed for CRUD tests.
func syncAndSetup() (*RecordStore, *MockTransaction, *MockDirectorySubspace, *MockKV) {
	kv := NewMockKV()
	tr := NewMockTransaction(kv)
	dir := &MockDirectorySubspace{}
	store := NewRecordStore()
	_ = store.SyncMetadata(tr, dir)
	return store, tr, dir, kv
}
