package storage

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type MemoryAdapter struct {
	store map[string][]byte
	lock  *sync.RWMutex
}

func NewMemoryAdapter() *MemoryAdapter {
	return &MemoryAdapter{
		store: make(map[string][]byte),
		lock:  &sync.RWMutex{},
	}
}

func (kv *MemoryAdapter) Get(key []byte) ([]byte, error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()
	keyHex := hex.EncodeToString(key)
	if v, ok := kv.store[keyHex]; ok {
		return v, nil
	}
	return nil, errors.New("key not found")
}

func (kv *MemoryAdapter) Put(key, value []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	keyHex := hex.EncodeToString(key)
	kv.store[keyHex] = value
	return nil
}

func (kv *MemoryAdapter) Has(key []byte) bool {
	kv.lock.RLock()
	defer kv.lock.RUnlock()
	keyHex := hex.EncodeToString(key)
	_, ok := kv.store[keyHex]
	return ok
}

func (kv *MemoryAdapter) Delete(key []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	keyHex := hex.EncodeToString(key)
	if _, ok := kv.store[keyHex]; ok {
		delete(kv.store, keyHex)
	} else {
		return fmt.Errorf("[MemKV] key not found: %s", keyHex)
	}
	return nil
}

func (kv *MemoryAdapter) BatchPut(kvs [][2][]byte) error {
	log.Println("BatchPut: Acquiring lock")
	kv.lock.Lock()
	defer kv.lock.Unlock()
	log.Println("BatchPut: Lock acquired")

	for _, kvp := range kvs {
		keyHex := hex.EncodeToString(kvp[0])
		kv.store[keyHex] = kvp[1]
		log.Printf("BatchPut: Stored key %s", keyHex)
	}
	log.Println("BatchPut: Completed")
	return nil
}

func (kv *MemoryAdapter) CreateSnapshot() map[string][]byte {
	kv.lock.RLock()
	defer kv.lock.RUnlock()

	snapshot := make(map[string][]byte)
	for k, v := range kv.store {
		snapshot[k] = v
	}
	return snapshot
}

func (kv *MemoryAdapter) ExportSnapshot(filename string) error {
	snapshot := kv.CreateSnapshot()
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

func (kv *MemoryAdapter) ImportSnapshot(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	data := make(map[string][]byte)
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		return err
	}

	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.store = data
	return nil
}

func (kv *MemoryAdapter) PruneOldSnapshots(directory string, maxSnapshots int) error {
	files, err := os.ReadDir(directory)
	if err != nil {
		return err
	}

	if len(files) <= maxSnapshots {
		return nil
	}

	// Sort files by modification time
	sort.Slice(files, func(i, j int) bool {
		infoI, _ := files[i].Info()
		infoJ, _ := files[j].Info()
		return infoI.ModTime().Before(infoJ.ModTime())
	})

	// Remove oldest files
	for i := 0; i < len(files)-maxSnapshots; i++ {
		err := os.Remove(filepath.Join(directory, files[i].Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (kv *MemoryAdapter) Close() {}
