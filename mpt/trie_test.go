package mpt

import (
	"bytes"
	"os"
	"testing"

	"github.com/vldmkr/merkle-patricia-trie/storage"
)

func TestPutGet(t *testing.T) {
	store := storage.NewMemoryAdapter()
	trie := New(nil, store)
	err := trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("234567"), []byte("D"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("1234567890"), []byte("E"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("12345678"), []byte("F"))
	if err != nil {
		t.Error(err.Error())
	}
	data, err := trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123456 wrong")
	}
	data, err = trie.Get([]byte("134567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "B" {
		t.Error("key 134567 wrong")
	}
	data, err = trie.Get([]byte("123467"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "C" {
		t.Error("key 123467 wrong")
	}
	data, err = trie.Get([]byte("234567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "D" {
		t.Error("key 234567 wrong")
	}
	data, err = trie.Get([]byte("1234567890"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "E" {
		t.Error("key 1234567890 wrong")
	}

	trie.Put([]byte("123456"), []byte("F"))
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("rewrite key 123456 wrong")
	}
	data, err = trie.Get([]byte("12345678"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("key 12345678 wrong")
	}
}

func TestPutCommitGet(t *testing.T) {
	store := storage.NewMemoryAdapter()
	trie := New(nil, store)
	err := trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("234567"), []byte("D"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("1234567890"), []byte("E"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("12345678"), []byte("F"))
	if err != nil {
		t.Error(err.Error())
	}
	trie.Commit()
	trie.Abort()
	data, err := trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123456 wrong")
	}
	data, err = trie.Get([]byte("134567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "B" {
		t.Error("key 134567 wrong")
	}
	data, err = trie.Get([]byte("123467"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "C" {
		t.Error("key 123467 wrong")
	}
	data, err = trie.Get([]byte("234567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "D" {
		t.Error("key 234567 wrong")
	}
	data, err = trie.Get([]byte("1234567890"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "E" {
		t.Error("key 1234567890 wrong")
	}

	trie.Put([]byte("123456"), []byte("F"))
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("rewrite key 123456 wrong")
	}
	data, err = trie.Get([]byte("12345678"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("key 12345678 wrong")
	}
}

func TestPutAbort(t *testing.T) {
	store := storage.NewMemoryAdapter()
	trie := New(nil, store)
	err := trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}
	trie.Abort()

	_, err = trie.Get([]byte("123456"))
	if err == nil {
		t.Error("Abort failed")
	}

	err = trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}
	trie.Commit()
	err = trie.Put([]byte("234567"), []byte("D"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("1234567890"), []byte("E"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("12345678"), []byte("F"))
	if err != nil {
		t.Error(err.Error())
	}
	data, err := trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123467 wrong")
	}

	trie.Abort()
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123467 wrong (after abort)")
	}
	data, err = trie.Get([]byte("12345678"))
	if err == nil {
		t.Error("Abort failed")
	}
}

func TestNodeSerialize(t *testing.T) {
	valueNode := ValueNode{
		Value: []byte("123"),
		dirty: true,
		cache: nil,
	}
	data := valueNode.Serialize()
	newNode, err := DeserializeNode(data)
	if err != nil {
		t.Error(err.Error())
	}
	if string(newNode.(*ValueNode).Value) != "123" {
		t.Error("content does not match")
	}
	if !bytes.Equal(data, newNode.Serialize()) {
		t.Error("data does not match")
	}
	shortNode := ShortNode{
		Key:   []byte("123"),
		Value: &valueNode,
		dirty: true,
	}
	data = shortNode.Serialize()
	newNode, err = DeserializeNode(data)
	if err != nil {
		t.Error(err.Error())
	}
	if string(newNode.(*ShortNode).Key) != "123" {
		t.Error("content does not match")
	}
	if !bytes.Equal(data, newNode.Serialize()) {
		t.Error("data does not match")
	}
	fullNode := FullNode{}
	fullNode.Children[0] = &shortNode
	shortNode.dirty = true
	valueNode.dirty = true
	fullNode.dirty = true
	data = shortNode.Serialize()
	newNode, err = DeserializeNode(data)
	if err != nil {
		t.Error(err.Error())
	}
	if !bytes.Equal(data, newNode.Serialize()) {
		t.Error("data does not match")
	}
}

func TestSerializeDeserialize(t *testing.T) {
	store := storage.NewMemoryAdapter()
	trie := New(nil, store)
	err := trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("234567"), []byte("D"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("1234567890"), []byte("E"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("12345678"), []byte("F"))
	if err != nil {
		t.Error(err.Error())
	}

	trie.Commit()
	data, err := trie.Serialize()
	if err != nil {
		t.Error(err.Error())
	}
	store = storage.NewMemoryAdapter()
	trie = New(nil, store)
	err = trie.Deserialize(data)
	if err != nil {
		t.Error(err.Error())
	}
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123456 wrong")
	}
	data, err = trie.Get([]byte("134567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "B" {
		t.Error("key 134567 wrong")
	}
	data, err = trie.Get([]byte("123467"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "C" {
		t.Error("key 123467 wrong")
	}
	data, err = trie.Get([]byte("234567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "D" {
		t.Error("key 234567 wrong")
	}
	data, err = trie.Get([]byte("1234567890"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "E" {
		t.Error("key 1234567890 wrong")
	}

	trie.Put([]byte("123456"), []byte("F"))
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("rewrite key 123456 wrong")
	}
	data, err = trie.Get([]byte("12345678"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("key 12345678 wrong")
	}

}

func TestPutCommitGetLevelDB(t *testing.T) {
	store, err := storage.NewLevelDBAdapter("./test")
	if err != nil {
		t.Error(err.Error())
	}
	trie := New(nil, store)
	err = trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Error(err.Error())
	}
	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("123467"), []byte("C"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("234567"), []byte("D"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("1234567890"), []byte("E"))
	if err != nil {
		t.Error(err.Error())
	}

	err = trie.Put([]byte("12345678"), []byte("F"))
	if err != nil {
		t.Error(err.Error())
	}
	trie.Commit()
	trie.Abort()
	data, err := trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "A" {
		t.Error("key 123456 wrong")
	}
	data, err = trie.Get([]byte("134567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "B" {
		t.Error("key 134567 wrong")
	}
	data, err = trie.Get([]byte("123467"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "C" {
		t.Error("key 123467 wrong")
	}
	data, err = trie.Get([]byte("234567"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "D" {
		t.Error("key 234567 wrong")
	}
	data, err = trie.Get([]byte("1234567890"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "E" {
		t.Error("key 1234567890 wrong")
	}

	trie.Put([]byte("123456"), []byte("F"))
	data, err = trie.Get([]byte("123456"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("rewrite key 123456 wrong")
	}
	data, err = trie.Get([]byte("12345678"))
	if err != nil {
		t.Error(err.Error())
	}
	if string(data) != "F" {
		t.Error("key 12345678 wrong")
	}
}

func TestCreateSnapshot(t *testing.T) {
	// Initialize nodes
	nodes := []Node{
		&FullNode{},
		&ShortNode{Key: []byte("key1"), Value: &ValueNode{Value: []byte("value1")}},
		&ValueNode{Value: []byte("value2")},
	}

	// Create snapshot
	snapshot := CreateSnapshot(nodes)

	// Validate snapshot
	if len(snapshot) != 3 {
		t.Errorf("Expected 3 nodes in snapshot, got %d", len(snapshot))
	}
}

func TestExportSnapshot(t *testing.T) {
	// Initialize nodes
	nodes := []Node{
		&FullNode{},
		&ShortNode{Key: []byte("key1"), Value: &ValueNode{Value: []byte("value1")}},
		&ValueNode{Value: []byte("value2")},
	}

	// Export snapshot
	err := ExportSnapshot("test_snapshot.json", nodes)
	if err != nil {
		t.Fatalf("Failed to export snapshot: %v", err)
	}

	// Clean up
	defer os.Remove("test_snapshot.json")
}

func TestImportSnapshot(t *testing.T) {
	// Initialize nodes
	nodes := []Node{
		&FullNode{},
		&ShortNode{Key: []byte("key1"), Value: &ValueNode{Value: []byte("value1")}},
		&ValueNode{Value: []byte("value2")},
	}

	// Export snapshot
	err := ExportSnapshot("test_snapshot.json", nodes)
	if err != nil {
		t.Fatalf("Failed to export snapshot: %v", err)
	}
	defer os.Remove("test_snapshot.json")

	// Import snapshot
	importedNodes, err := ImportSnapshot("test_snapshot.json")
	if err != nil {
		t.Fatalf("Failed to import snapshot: %v", err)
	}

	// Validate imported nodes
	if len(importedNodes) != 3 {
		t.Errorf("Expected 3 nodes in imported snapshot, got %d", len(importedNodes))
	}

	// Additional validation to ensure the nodes are correctly imported
	for _, node := range nodes {
		hash := string(node.Hash())
		importedNode, exists := importedNodes[hash]
		if !exists {
			t.Errorf("Node with hash %s not found in imported snapshot", hash)
		} else if string(importedNode.Hash()) != hash {
			t.Errorf("Imported node hash mismatch: expected %s, got %s", hash, importedNode.Hash())
		}
	}
}
