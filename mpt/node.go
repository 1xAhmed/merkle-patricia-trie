package mpt

import (
	"encoding/json"
	"errors"
	fmt "fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/vldmkr/merkle-patricia-trie/crypto"
	"github.com/vldmkr/merkle-patricia-trie/storage"
)

type (
	Node interface {
		Hash() []byte
		CachedHash() []byte
		Serialize() []byte
		Save(storage.StorageAdapter)
	}
	FullNode struct {
		Children [257]Node
		cache    []byte
		dirty    bool
	}
	ShortNode struct {
		Key   []byte
		Value Node
		cache []byte
		dirty bool
	}
	HashNode  []byte
	ValueNode struct {
		Value []byte
		cache []byte
		dirty bool
	}
)

func (n *FullNode) CachedHash() []byte  { return n.cache }
func (n *ShortNode) CachedHash() []byte { return n.cache }
func (n *ValueNode) CachedHash() []byte { return n.cache }
func (n *HashNode) CachedHash() []byte  { return []byte(*n) }

func DeserializeNode(data []byte) (Node, error) {
	persistNode := &PersistNodeBase{}
	err := cbor.Unmarshal(data, persistNode)
	if err != nil {
		return nil, fmt.Errorf("[Node] cannot deserialize persist node: %s", err.Error())
	}
	if persistNode.Full != nil {
		fullNode := FullNode{}
		for i := 0; i < len(fullNode.Children); i++ {
			if len(persistNode.Full.Children[i]) != 0 {
				child := HashNode(persistNode.Full.Children[i])
				fullNode.Children[i] = &child
				if len([]byte(child)) == 0 {
					return nil, errors.New("[Node] nil full node child")
				}
			}
		}
		hash := crypto.MainHash(data)
		fullNode.cache = hash[:]
		return &fullNode, nil
	}
	if persistNode.Short != nil {
		shortNode := ShortNode{}
		shortNode.Key = persistNode.Short.Key
		if len(persistNode.Short.Value) == 0 {
			return nil, errors.New("[Node] nil short node value")
		}
		child := HashNode(persistNode.Short.Value)
		shortNode.Value = &child
		hash := crypto.MainHash(data)
		shortNode.cache = hash[:]
		return &shortNode, nil
	}
	if persistNode.Value != nil {
		hash := crypto.MainHash(data)
		ret := ValueNode{*persistNode.Value, hash[:], false}
		return &ret, nil
	}
	return nil, errors.New("[Node] Unknown node type")
}

func (vn *ValueNode) Serialize() []byte {
	persistValueNode := PersistNodeValue{}
	persistValueNode = vn.Value
	persistNode := PersistNodeBase{
		Value: &persistValueNode,
	}
	data, _ := cbor.Marshal(&persistNode)
	hash := crypto.MainHash(data)
	vn.cache = hash[:]
	vn.dirty = false
	return data
}

func (vn *ValueNode) Hash() []byte {
	if vn.dirty {
		vn.Serialize()
	}
	return vn.cache
}

func (vn *ValueNode) Save(store storage.StorageAdapter) {
	data := vn.Serialize()
	store.Put(vn.cache, data)
}

func (fn *FullNode) Serialize() []byte {
	persistFullNode := PersistNodeFull{}
	persistFullNode.Children = make([][]byte, 257)
	for i := 0; i < len(fn.Children); i++ {
		if fn.Children[i] != nil {
			persistFullNode.Children[i] = fn.Children[i].Hash()
		}
	}
	data, _ := cbor.Marshal(&PersistNodeBase{
		Full: &persistFullNode,
	})
	hash := crypto.MainHash(data)
	fn.cache = hash[:]
	fn.dirty = false
	return data
}

func (fn *FullNode) Hash() []byte {
	if fn.dirty {
		fn.Serialize()
	}
	return fn.cache
}

func (fn *FullNode) Save(store storage.StorageAdapter) {
	data := fn.Serialize()
	store.Put(fn.cache, data)
}

func (sn *ShortNode) Serialize() []byte {
	persistShortNode := PersistNodeShort{}
	persistShortNode.Key = sn.Key
	persistShortNode.Value = sn.Value.Hash()
	data, _ := cbor.Marshal(&PersistNodeBase{
		Short: &persistShortNode,
	})
	hash := crypto.MainHash(data)
	sn.cache = hash[:]
	sn.dirty = false
	return data
}

func (sn *ShortNode) Hash() []byte {
	if sn.dirty {
		sn.Serialize()
	}
	return sn.cache
}

func (sn *ShortNode) Save(store storage.StorageAdapter) {
	data := sn.Serialize()
	store.Put(sn.cache, data)
}

func (hn *HashNode) Hash() []byte                      { return []byte(*hn) }
func (hn *HashNode) Serialize() []byte                 { return nil }
func (hn *HashNode) Save(store storage.StorageAdapter) {}

// Snapshot Management

var snapshotLock sync.RWMutex

// CreateSnapshot collects data from all nodes and flattens it into a straightforward structure.
func CreateSnapshot(nodes []Node) map[string][]byte {
	snapshotLock.RLock()
	defer snapshotLock.RUnlock()

	snapshot := make(map[string][]byte)
	for _, node := range nodes {
		data := node.Serialize()
		snapshot[string(node.Hash())] = data
	}
	return snapshot
}

// ExportSnapshot serializes the snapshot into a format that can be easily stored and writes it to a file.
func ExportSnapshot(filename string, nodes []Node) error {
	snapshot := CreateSnapshot(nodes)
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

// ImportSnapshot reads the snapshot file, deserializes it, and uses the data to restore the system's state.
func ImportSnapshot(filename string) (map[string]Node, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make(map[string][]byte)
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		return nil, err
	}

	nodes := make(map[string]Node)
	for hash, nodeData := range data {
		node, err := DeserializeNode(nodeData)
		if err != nil {
			return nil, err
		}
		nodes[hash] = node
	}
	return nodes, nil
}

// ValidateSnapshot validates the snapshot to ensure consistency.
func ValidateSnapshot(snapshot map[string]Node) bool {
	// Implement validation logic here
	// For example, check if all nodes have valid hashes and serialized data
	return true
}

// PruneOldSnapshots prunes old snapshots to maintain efficiency.
func PruneOldSnapshots(directory string, maxSnapshots int) error {
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
