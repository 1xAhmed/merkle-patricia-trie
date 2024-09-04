package storage

import (
	"os"
	"testing"
)

func TestMemoryAdapter(t *testing.T) {
	// Initialize the MemoryAdapter
	kv := NewMemoryAdapter()

	// Add some data to the MemoryAdapter
	err := kv.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to put data: %v", err)
	}
	err = kv.Put([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Failed to put data: %v", err)
	}

	// Create a snapshot
	snapshot := kv.CreateSnapshot()
	if len(snapshot) != 2 {
		t.Fatalf("Expected snapshot length 2, got %d", len(snapshot))
	}

	// Export the snapshot to a file
	filename := "snapshot_test.json"
	err = kv.ExportSnapshot(filename)
	if err != nil {
		t.Fatalf("Failed to export snapshot: %v", err)
	}
	defer os.Remove(filename) // Clean up the file after the test

	// Clear the MemoryAdapter
	kv.store = make(map[string][]byte)

	// Import the snapshot from the file
	err = kv.ImportSnapshot(filename)
	if err != nil {
		t.Fatalf("Failed to import snapshot: %v", err)
	}

	// Validate the imported data
	value, err := kv.Get([]byte("key1"))
	if err != nil || string(value) != "value1" {
		t.Fatalf("Expected value1, got %s (err: %v)", string(value), err)
	}
	value, err = kv.Get([]byte("key2"))
	if err != nil || string(value) != "value2" {
		t.Fatalf("Expected value2, got %s (err: %v)", string(value), err)
	}
}

func TestBatchPut(t *testing.T) {
	// Initialize the MemoryAdapter
	kv := NewMemoryAdapter()

	// Add multiple key-value pairs using BatchPut
	kvs := [][2][]byte{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
	}
	err := kv.BatchPut(kvs)
	if err != nil {
		t.Fatalf("Failed to batch put data: %v", err)
	}

	// Validate the data
	value, err := kv.Get([]byte("key1"))
	if err != nil || string(value) != "value1" {
		t.Fatalf("Expected value1, got %s (err: %v)", string(value), err)
	}
	value, err = kv.Get([]byte("key2"))
	if err != nil || string(value) != "value2" {
		t.Fatalf("Expected value2, got %s (err: %v)", string(value), err)
	}
}
