package registry

import "github.com/sjy-dv/scdb/scdb/storage"

type Accessor interface {
	Save(key []byte, p *storage.ChunkPosition) *storage.ChunkPosition
	Get(key []byte) *storage.ChunkPosition
	Del(key []byte) (*storage.ChunkPosition, bool)
	Size() int
	Ascend(callback func(key []byte, pos *storage.ChunkPosition) (bool, error))
	AscendRange(startKey, endKey []byte, callback func(key []byte, pos *storage.ChunkPosition) (bool, error))
	AscendGreaterOrEqual(key []byte, callback func(key []byte, pos *storage.ChunkPosition) (bool, error))
	Descend(callback func(key []byte, pos *storage.ChunkPosition) (bool, error))
	DescendRange(startKey, endKey []byte, callback func(key []byte, pos *storage.ChunkPosition) (bool, error))
	DescendLessOrEqual(key []byte, callback func(key []byte, pos *storage.ChunkPosition) (bool, error))
}

type AccessorType = byte

const (
	BTree AccessorType = iota
)

var accessorType = BTree

func NewAccessor() Accessor {
	switch accessorType {
	case BTree:
		return newBTree()
	default:
		panic("unsupported accessor type")
	}
}
