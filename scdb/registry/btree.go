package registry

import (
	"bytes"
	"sync"

	"github.com/google/btree"
	"github.com/sjy-dv/scdb/scdb/storage"
)

type MemBTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

type item struct {
	key []byte
	pos *storage.ChunkPosition
}

func newBTree() *MemBTree {
	return &MemBTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (i *item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*item).key) < 0
}

func (m *MemBTree) Save(key []byte, pos *storage.ChunkPosition) *storage.ChunkPosition {
	m.lock.Lock()
	defer m.lock.Unlock()
	oldVal := m.tree.ReplaceOrInsert(&item{key: key, pos: pos})
	if oldVal != nil {
		return oldVal.(*item).pos
	}
	return nil
}

func (m *MemBTree) Get(key []byte) *storage.ChunkPosition {
	val := m.tree.Get(&item{key: key})
	if val != nil {
		return val.(*item).pos
	}
	return nil
}

func (m *MemBTree) Del(key []byte) (*storage.ChunkPosition, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	val := m.tree.Delete(&item{key: key})
	if val != nil {
		return val.(*item).pos, true
	}
	return nil, false
}

func (m *MemBTree) Size() int {
	return m.tree.Len()
}

func (m *MemBTree) Ascend(callback func(key []byte, pos *storage.ChunkPosition) (bool, error)) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	m.tree.Ascend(func(i btree.Item) bool {
		cont, err := callback(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (m *MemBTree) Descend(callback func(key []byte, position *storage.ChunkPosition) (bool, error)) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	m.tree.Descend(func(i btree.Item) bool {
		cont, err := callback(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (m *MemBTree) AscendRange(startKey, endKey []byte, callback func(key []byte, position *storage.ChunkPosition) (bool, error)) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	m.tree.AscendRange(&item{key: startKey}, &item{key: endKey}, func(i btree.Item) bool {
		cont, err := callback(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (m *MemBTree) DescendRange(startKey, endKey []byte, callback func(key []byte, position *storage.ChunkPosition) (bool, error)) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	m.tree.DescendRange(&item{key: startKey}, &item{key: endKey}, func(i btree.Item) bool {
		cont, err := callback(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (m *MemBTree) AscendGreaterOrEqual(key []byte, callback func(key []byte, position *storage.ChunkPosition) (bool, error)) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	m.tree.AscendGreaterOrEqual(&item{key: key}, func(i btree.Item) bool {
		cont, err := callback(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (m *MemBTree) DescendLessOrEqual(key []byte, callback func(key []byte, position *storage.ChunkPosition) (bool, error)) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	m.tree.DescendLessOrEqual(&item{key: key}, func(i btree.Item) bool {
		cont, err := callback(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}
