package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	fdblayer "github.com/romannikov/fdb-layer/runtimes/go"
)

// MockKV – in-memory sorted key-value store backing all mock FDB operations.
type MockKV struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMockKV() *MockKV {
	return &MockKV{data: make(map[string][]byte)}
}

func (m *MockKV) Set(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := make([]byte, len(value))
	copy(v, value)
	m.data[string(key)] = v
}

func (m *MockKV) Get(key []byte) []byte {
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

func (m *MockKV) Clear(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
}

// RangeSlice returns all KVs whose keys are in [begin, end).
func (m *MockKV) RangeSlice(begin, end []byte, opts fdb.RangeOptions) []fdb.KeyValue {
	m.mu.RLock()
	defer m.mu.RUnlock()

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

	if opts.Reverse {
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].key > pairs[j].key })
	} else {
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].key < pairs[j].key })
	}

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

func (m *MockKV) PrintKeys() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k := range m.data {
		fmt.Printf("Stored key: %x\n", []byte(k))
	}
}

func (m *MockKV) KeyCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

func (m *MockKV) Snapshot() map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string][]byte, len(m.data))
	for k, v := range m.data {
		out[k] = v
	}
	return out
}

type MockFutureByteSlice struct {
	value []byte
}

func (f *MockFutureByteSlice) Get() ([]byte, error) { return f.value, nil }
func (f *MockFutureByteSlice) MustGet() []byte      { return f.value }
func (f *MockFutureByteSlice) BlockUntilReady()     {}
func (f *MockFutureByteSlice) IsReady() bool        { return true }
func (f *MockFutureByteSlice) Cancel()              {}

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

type MockTransaction struct {
	kv         *MockKV
	SetCalls   []fdb.Key
	ClearCalls []fdb.Key
}

func NewMockTransaction(kv *MockKV) *MockTransaction {
	return &MockTransaction{kv: kv}
}

func (m *MockTransaction) ResetLog() {
	m.SetCalls = nil
	m.ClearCalls = nil
}

func (m *MockTransaction) Set(key fdb.KeyConvertible, value []byte) {
	k := key.FDBKey()
	m.SetCalls = append(m.SetCalls, append(fdb.Key(nil), k...))
	m.kv.Set(k, value)
}

func (m *MockTransaction) SetVersionstampedKey(key fdb.KeyConvertible, value []byte) {
	keyBytes := key.FDBKey()
	if len(keyBytes) < 4 {
		panic("invalid versionstamped key: too short")
	}
	offset := binary.LittleEndian.Uint32(keyBytes[len(keyBytes)-4:])

	dummyVS := []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01")

	newKey := make([]byte, len(keyBytes)-4)
	copy(newKey, keyBytes[:len(keyBytes)-4])

	if int(offset)+10 > len(newKey) {
		panic("invalid versionstamped key: offset out of bounds")
	}
	copy(newKey[offset:], dummyVS)

	m.kv.Set(newKey, value)
}

func (m *MockTransaction) Clear(key fdb.KeyConvertible) {
	k := key.FDBKey()
	m.ClearCalls = append(m.ClearCalls, append(fdb.Key(nil), k...))
	m.kv.Clear(k)
}

func (m *MockTransaction) Add(key fdb.KeyConvertible, param []byte) {
	k := key.FDBKey()
	m.kv.mu.Lock()
	defer m.kv.mu.Unlock()

	current := m.kv.data[string(k)]
	var currentVal uint64
	if len(current) >= 8 {
		currentVal = binary.LittleEndian.Uint64(current)
	}
	delta := binary.LittleEndian.Uint64(param)
	newVal := currentVal + delta
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, newVal)
	m.kv.data[string(k)] = buf
}

func (m *MockTransaction) Max(key fdb.KeyConvertible, param []byte) {
	k := key.FDBKey()
	m.kv.mu.Lock()
	defer m.kv.mu.Unlock()

	current := m.kv.data[string(k)]
	var currentVal uint64
	if len(current) >= 8 {
		currentVal = binary.LittleEndian.Uint64(current)
	}
	val := binary.LittleEndian.Uint64(param)
	if len(current) < 8 || val > currentVal {
		m.kv.data[string(k)] = append([]byte(nil), param...)
	}
}

func (m *MockTransaction) Min(key fdb.KeyConvertible, param []byte) {
	k := key.FDBKey()
	m.kv.mu.Lock()
	defer m.kv.mu.Unlock()

	current := m.kv.data[string(k)]
	var currentVal uint64
	if len(current) >= 8 {
		currentVal = binary.LittleEndian.Uint64(current)
	} else {
		currentVal = ^uint64(0)
	}
	val := binary.LittleEndian.Uint64(param)
	if len(current) < 8 || val < currentVal {
		m.kv.data[string(k)] = append([]byte(nil), param...)
	}
}

func (m *MockTransaction) Get(key fdb.KeyConvertible) fdb.FutureByteSlice {
	return &MockFutureByteSlice{value: m.kv.Get(key.FDBKey())}
}

func (m *MockTransaction) GetRange(r fdb.Range, options fdb.RangeOptions) fdb.RangeResult {
	return fdb.RangeResult{}
}

func (m *MockTransaction) GetRangeSlice(r fdb.Range, options fdb.RangeOptions) []fdb.KeyValue {
	begin, end := r.FDBRangeKeySelectors()
	beginKey := begin.FDBKeySelector().Key.FDBKey()
	endKey := end.FDBKeySelector().Key.FDBKey()
	return m.kv.RangeSlice(beginKey, endKey, options)
}

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

type MockDirectorySubspace struct {
	directory.DirectorySubspace
}

func (m *MockDirectorySubspace) Pack(t tuple.Tuple) fdb.Key {
	return t.Pack()
}

func (m *MockDirectorySubspace) PackWithVersionstamp(t tuple.Tuple) (fdb.Key, error) {
	return t.PackWithVersionstamp(nil)
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

func SyncAndSetup() (*fdblayer.RecordStore, *MockTransaction, *MockDirectorySubspace, *MockKV) {
	kv := NewMockKV()
	tr := NewMockTransaction(kv)
	dir := &MockDirectorySubspace{}
	recordStore := fdblayer.NewRecordStore()
	_ = recordStore.SyncMetadata(context.Background(), tr, dir, []string{"User", "Product", "Post", "TaskMessage"})
	return recordStore, tr, dir, kv
}

func TestDir(t *testing.T, db fdb.Database) (directory.DirectorySubspace, func()) {
	t.Helper()
	path := []string{"test", fmt.Sprintf("%s_%d", t.Name(), time.Now().UnixNano())}
	dir, err := directory.CreateOrOpen(db, path, nil)
	if err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	cleanup := func() {
		_, _ = directory.Root().Remove(db, path)
	}
	return dir, cleanup
}

func WithTx(t *testing.T, db fdb.Database, fn func(tr fdb.Transaction) error) {
	t.Helper()
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return nil, fn(tr)
	})
	if err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}
