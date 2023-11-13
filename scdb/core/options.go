package core

import (
	"os"
)

type Options struct {
	DirPath        string
	SegmentSize    int64
	BlockCache     uint32
	Sync           bool
	BytesPerSync   uint32
	WatchQueueSize uint64
}

type BatchOptions struct {
	Sync     bool
	ReadOnly bool
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:        tempDir(),
	SegmentSize:    1 * GB,
	BlockCache:     0,
	Sync:           false,
	BytesPerSync:   0,
	WatchQueueSize: 0,
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

func tempDir() string {
	dir, _ := os.MkdirTemp("", "/scdb/tmp")

	return dir
}
