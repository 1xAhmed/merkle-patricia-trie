package mpt

import (
	"bytes"
	fmt "fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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

	snapshot := trie.CreateSnapshot()
	if len(snapshot) == 0 {
		t.Error("Snapshot is empty")
	}

	// Validate snapshot content
	for key, value := range snapshot {
		node, err := DeserializeNode(value)
		if err != nil {
			t.Error(err.Error())
		}
		if string(node.Hash()) != key {
			t.Errorf("Snapshot key %s does not match node hash %s", key, node.Hash())
		}
	}
}

func TestExportImportSnapshot(t *testing.T) {
	store := storage.NewMemoryAdapter()
	trie := New(nil, store)

	// Populate the trie
	err := trie.Put([]byte("123456"), []byte("A"))
	if err != nil {
		t.Fatalf("Failed to put data in trie: %v", err)
	}
	err = trie.Put([]byte("134567"), []byte("B"))
	if err != nil {
		t.Fatalf("Failed to put data in trie: %v", err)
	}

	// Export the snapshot
	filename := "snapshot_test.json"
	err = trie.ExportSnapshot(filename)
	if err != nil {
		t.Fatalf("Failed to export snapshot: %v", err)
	}
	defer os.Remove(filename) // Clean up the file after the test

	// Clear the trie and import the snapshot
	store = storage.NewMemoryAdapter()
	trie = New(nil, store)
	err = trie.ImportSnapshot(filename)
	if err != nil {
		t.Fatalf("Failed to import snapshot: %v", err)
	}

	// Validate the imported data
	data, err := trie.Get([]byte("123456"))
	if err != nil || string(data) != "A" {
		t.Fatalf("Expected A, got %s (err: %v)", string(data), err)
	}
	data, err = trie.Get([]byte("134567"))
	if err != nil || string(data) != "B" {
		t.Fatalf("Expected B, got %s (err: %v)", string(data), err)
	}
}

func TestValidateSnapshot(t *testing.T) {
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

	snapshot := trie.CreateSnapshot()
	nodes := make(map[string]Node)
	for hash, nodeData := range snapshot {
		node, err := DeserializeNode(nodeData)
		if err != nil {
			t.Error(err.Error())
		}
		nodes[hash] = node
	}

	if !ValidateSnapshot(nodes) {
		t.Error("Snapshot validation failed")
	}
}

func TestPruneOldSnapshots(t *testing.T) {
    // Create a temporary directory for snapshots
    dir, err := os.MkdirTemp("", "snapshots")
    if err != nil {
        t.Fatal(err)
    }
    defer os.RemoveAll(dir) // Clean up the directory after the test

    // Create dummy snapshot files
    for i := 0; i < 5; i++ {
        file, err := os.Create(filepath.Join(dir, fmt.Sprintf("snapshot_%d.json", i)))
        if err != nil {
            t.Fatal(err)
        }
        file.Close()
        time.Sleep(1 * time.Second) // Ensure different modification times
    }

    // List files before pruning
	files, err := os.ReadDir(dir)
    if err != nil {
        t.Fatal(err)
    }
    t.Logf("Files before pruning: %v", getFilenames(files))

    // Prune snapshots to keep only 3
    err = PruneOldSnapshots(dir, 3)
    if err != nil {
        t.Fatal(err)
    }

    // List files after pruning
    files, err = os.ReadDir(dir)
    if err != nil {
        t.Fatal(err)
    }
    t.Logf("Files after pruning: %v", getFilenames(files))

    if len(files) != 3 {
        t.Fatalf("Expected 3 snapshots, got %d", len(files))
    }
}

// Helper function to get filenames from os.DirEntry slice
func getFilenames(files []os.DirEntry) []string {
    var filenames []string
    for _, file := range files {
        filenames = append(filenames, file.Name())
    }
    return filenames
}