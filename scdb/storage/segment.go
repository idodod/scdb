package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	bytebufferpool "github.com/sjy-dv/scdb/scdb/storage/byte_buffer_pool"
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

type (
	ChunkType = byte
)

const (
	CtFull ChunkType = iota
	CtFirst
	CtMid
	CtEnd
)

const (
	// 7 Bytes
	// Checksum Length Type
	//    4      2     1
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32 * KB

	fileModePerm = 0644

	// uin32 + uint32 + int64 + uin32
	// segmentId + BlockNumber + ChunkOffset + ChunkSize
	maxLen = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

/*
----------------------------------------------------------------
-KOR
이 segment 타입은 Write-Ahead Log(WAL)에서 단일 세그먼트 파일을 나타냅니다. WAL 시스템에서 세그먼트 파일은 데이터를 순차적으로 추가하는 데 사용되며, 파일은 블록 단위로 데이터를 기록합니다. 여기서 각각의 블록은 32KB 크기이며, 데이터는 더 작은 단위인 청크로 기록됩니다. segment 구조체의 각 필드는 다음과 같은 목적을 가지고 있습니다:

id: 세그먼트 파일의 고유 식별자입니다.
fd: *os.File은 파일 디스크립터로, 세그먼트 파일에 대한 참조를 가집니다. 이를 통해 파일 읽기 및 쓰기 작업을 할 수 있습니다.
currentBlockNumber: 현재 쓰고 있는 블록의 순번입니다. 각 블록은 32KB 크기를 가지며, 이 숫자는 현재 어떤 블록에 데이터를 추가하고 있는지를 추적합니다.
currentBlockSize: 현재 블록에 쓰여진 데이터의 크기입니다. 이 크기가 32KB에 도달하면 새로운 블록으로 이동해야 합니다.
closed: 세그먼트 파일이 닫혔는지 여부를 나타내는 불리언 값입니다. 파일이 닫혔으면 더 이상 데이터를 쓸 수 없습니다.
cache: 이 *lru.Cache[uint64, []byte]는 최근 사용된 데이터 블록을 캐싱하여 성능을 향상시킵니다. LRU(Least Recently Used) 캐시는 가장 오랫동안 사용되지 않은 항목을 제거함으로써 제한된 메모리 공간을 관리합니다.
header: 세그먼트 파일의 헤더 정보를 담고 있는 바이트 슬라이스입니다. 헤더에는 파일의 메타데이터가 포함될 수 있습니다.
blockPool: 이 sync.Pool은 성능을 향상시키기 위해 블록 데이터의 메모리를 재사용합니다. 블록 데이터를 할당하고 해제하는 오버헤드를 줄여줍니다.
segment 구조체는 파일 시스템 상의 WAL 세그먼트 파일을 효율적으로 관리하고 데이터를 안정적으로 저장하는 데 필요한 상태와 동작을 캡슐화합니다.

----------------------------------------------------------------
-ENG
The segment type represents a single segment file within a Write-Ahead Log (WAL) system. A segment file in WAL is append-only, meaning data is added sequentially, and it is organized in blocks. Here's a breakdown of the structure's fields for clarity:

id: uniquely identifies the segment file. It's used to keep track of different segment files within the WAL system.

fd: This *os.File is a file descriptor, which is a reference to the segment file. It allows the system to perform read and write operations on the file.

currentBlockNumber: This is the number of the block currently being written to. Each block is 32KB in size, and this number helps track which block is currently in use for appending data.

currentBlockSize: This represents the size of the data that has already been written to the current block. Once this size reaches the limit of 32KB, the system must move on to a new block.

closed: A boolean indicating whether the segment file is closed. If it's closed, no further data can be written to it.

cache: This *lru.Cache[uint64, []byte] is used to cache recently accessed data blocks to improve performance. An LRU (Least Recently Used) cache manages a limited memory space by evicting the least recently used items.

header: A byte slice that holds the header information of the segment file. The header can include metadata about the file.

blockPool: This sync.Pool is used to reuse the memory for block data to enhance performance. It reduces the overhead of allocating and deallocating block data.
*/
type segment struct {
	id                 uint32
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
	cache              *lru.Cache[uint64, []byte]
	header             []byte
	blockPool          sync.Pool
}

/*
----------------------------------------------------------------
-KOR
segmentReader 구조체는 세그먼트 파일로부터 모든 데이터를 순회(iterate)하는 데 사용됩니다. Next 메서드를 호출하여 다음 청크 데이터를 가져올 수 있으며, 데이터가 더 이상 없을 때 io.EOF가 반환됩니다. 구조체의 필드는 다음과 같습니다:

segment: 읽고 있는 세그먼트 파일을 가리키는 포인터입니다.
blockNumber: 현재 읽고 있는 블록의 번호입니다.
chunkOffset: 현재 블록 내에서의 청크 데이터의 오프셋(시작 위치)을 나타냅니다.
**이 구조체를 사용하여 파일의 데이터를 순차적으로 읽어 나갈 수 있으며, 각 청크의 시작점을 알 수 있어 효율적인 데이터 처리가 가능합니다.**

----------------------------------------------------------------
-ENG
The segmentReader struct is used to iterate through all the data from a segment file. You can invoke the Next method to retrieve the next chunk of data, and io.EOF will be returned when there is no more data to read. The fields within the struct are:

segment: A pointer to the segment file being read.
blockNumber: The number of the block that is currently being read.
chunkOffset: The offset, or starting position, of the chunk data within the current block.

**This struct allows for the sequential reading of the file's data, enabling efficient processing by knowing the starting point of each chunk.**
*/
type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
}

/*
----------------------------------------------------------
-KOR
blockAndHeader 구조체는 풀(pool)에 저장되는 블록과 청크 헤더를 포함합니다. 이 구조체는 데이터의 블록과 해당 블록의 청크 헤더 정보를 함께 관리하는 데 사용됩니다. 구조체의 필드는 다음과 같습니다:

block: 실제 데이터가 저장되는 바이트 배열입니다.
header: 해당 데이터 블록에 대한 메타데이터를 포함하는 바이트 배열입니다. 예를 들어, 데이터의 체크섬, 길이 또는 타입 등의 정보를 담을 수 있습니다.
이 구조체는 데이터와 그 데이터에 대한 메타정보를 함께 저장하고 관리함으로써, 데이터 처리 시 필요한 추가적인 정보를 빠르게 액세스할 수 있도록 도와줍니다.

--------------------------------------------------------------
-ENG
The blockAndHeader struct holds a block and its corresponding chunk header, which are saved in a pool. This struct is utilized to manage a chunk of data alongside its metadata header. The fields within the struct are:

block: An array of bytes where the actual data is stored.
header: An array of bytes that contains metadata for the data block. This could include information such as checksums, length, or data type.
This struct facilitates the storage and management of data along with its associated metadata, enabling quick access to essential information required for data processing.
*/
type blockAndHeader struct {
	block  []byte
	header []byte
}

/*
한국어 설명:
ChunkPosition 구조체는 세그먼트 파일 내에서 청크의 위치를 나타냅니다. 이 구조체는 세그먼트 파일로부터 데이터를 읽어올 때 사용됩니다. 구조체의 각 필드는 다음과 같은 정보를 담고 있습니다:

SegmentId: 청크가 속한 세그먼트 파일의 고유 식별자입니다.
BlockNumber: 세그먼트 파일 내에서 청크가 위치한 블록 번호입니다.
ChunkOffset: 세그먼트 파일 내에서 청크의 시작 오프셋(위치)을 나타냅니다.
ChunkSize: 세그먼트 파일 내에서 청크 데이터가 차지하는 바이트 수입니다.
이 구조체는 특정 청크의 정확한 위치와 크기 정보를 제공함으로써, 파일 내에서 데이터를 효과적으로 찾아서 읽을 수 있도록 도와줍니다.

English Explanation:
The ChunkPosition struct represents the position of a chunk within a segment file. It is used to read data from the segment file. Each field in the struct provides specific information:

SegmentId: The unique identifier for the segment file to which the chunk belongs.
BlockNumber: The number of the block within the segment file where the chunk is located.
ChunkOffset: The starting offset of the chunk within the segment file.
ChunkSize: The number of bytes that the chunk data occupies within the segment file.
This struct facilitates efficient data retrieval by providing precise location and size information for a given chunk within a file.
*/
type ChunkPosition struct {
	SegmentId uint32
	// BlockNumber The block number of the chunk in the segment file.
	BlockNumber uint32
	// ChunkOffset The start offset of the chunk in the segment file.
	ChunkOffset int64
	// ChunkSize How many bytes the chunk data takes up in the segment file.
	ChunkSize uint32
}

/*
한국어 설명:
loadSegment 함수는 지정된 디렉토리에서 세그먼트 파일을 로드하는 기능을 합니다. 이 함수는 세그먼트 파일의 식별자(ID), 디렉토리 경로, 파일 확장자, 그리고 데이터 블록을 캐싱하기 위한 LRU 캐시를 인자로 받습니다.

GetSegmentSource 함수를 사용하여 세그먼트 파일의 전체 경로를 구성합니다.
os.OpenFile 함수를 이용해 세그먼트 파일을 생성하거나, 읽기와 쓰기, 추가 모드로 엽니다. 파일이 존재하지 않는 경우 새로 생성됩니다.
파일 디스크립터 fd를 사용하여 파일의 끝으로 이동합니다. 이는 현재 파일의 크기를 알아내기 위함입니다.
파일의 끝으로의 이동에 실패한 경우, 에러 메시지와 함께 프로그램이 패닉 상태에 빠지도록 합니다.
새로운 segment 구조체 인스턴스를 생성하여 반환합니다. 이 구조체는 세그먼트의 ID, 파일 디스크립터, 캐시, 청크 헤더를 위한 바이트 슬라이스, 블록 풀을 포함하고 있습니다. 또한, 현재 블록 번호와 크기도 계산하여 저장합니다.
이 함수는 세그먼트 파일을 적절히 관리하고, 파일에 새로운 데이터를 추가하거나 기존 데이터를 읽는 작업을 수행할 준비를 합니다.

English Explanation:
The loadSegment function is responsible for loading a segment file from the specified directory. It accepts a segment ID, directory path, file extension, and an LRU cache for caching data blocks as its parameters.

It constructs the full path to the segment file using the GetSegmentSource function.
It opens the segment file using os.OpenFile with create, read-write, and append modes. If the file doesn't exist, it is created.
It uses the file descriptor fd to seek to the end of the file. This is to determine the current size of the file.
If seeking to the end of the file fails, the program panics with an error message.
It creates and returns a new instance of the segment struct. This struct includes the segment ID, file descriptor, cache, a byte slice for the chunk header, and a block pool. It also calculates and stores the current block number and size based on the file offset.
This function prepares the segment file for proper management, readying it for appending new data or reading existing data.
*/
func loadSegment(dir, ext string, id uint32, cache *lru.Cache[uint64, []byte]) (*segment, error) {
	fd, err := os.OpenFile(
		GetSegmentSource(dir, ext, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)
	if err != nil {
		return nil, err
	}
	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, ext, err))
	}
	return &segment{
		id:                 id,
		fd:                 fd,
		cache:              cache,
		header:             make([]byte, chunkHeaderSize),
		blockPool:          sync.Pool{New: newBlockAndHeader},
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
	}, nil
}

func newBlockAndHeader() interface{} {
	return &blockAndHeader{
		block:  make([]byte, blockSize),
		header: make([]byte, chunkHeaderSize),
	}
}

func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

// flush seg.file to disk
func (seg *segment) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

// remove seg.file
func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.fd.Close()
	}
	return os.Remove(seg.fd.Name())
}

// return seg.file size
func (seg *segment) Size() int64 {
	return (int64(seg.currentBlockNumber) * int64(blockSize)) + int64(seg.currentBlockSize)
}

/*
한국어 설명:
writeToBuf 함수는 데이터를 청크 단위로 나누어 bytebufferpool.ByteBuffer에 기록하고, 세그먼트의 상태를 업데이트합니다. 데이터는 네 가지 청크 타입(ChunkTypeFull, ChunkTypeFirst, ChunkTypeMiddle, ChunkTypeLast)으로 나누어집니다.

각 청크에는 헤더가 있으며, 헤더에는 길이, 타입, 체크섬이 포함됩니다. 청크의 페이로드는 실제 쓰고자 하는 데이터입니다.

이 함수는 청크의 시작 위치를 계산하고 해당 위치(ChunkPosition)를 반환합니다. 이 위치 정보를 사용하면 나중에 데이터를 읽을 수 있습니다. 또한, 현재 블록의 남은 크기가 청크 헤더를 수용할 수 없는 경우에는 패딩을 추가하여 새 블록을 시작합니다.

데이터가 현재 블록에 모두 들어갈 수 있으면, 청크 타입을 ChunkTypeFull로 설정하여 버퍼에 추가합니다. 그렇지 않으면 데이터를 여러 청크로 나누어 버퍼에 추가하고, 각 청크의 타입을 적절히 설정합니다.

마지막으로, 세그먼트의 현재 블록 크기를 업데이트하고, 필요한 경우 현재 블록 번호도 업데이트합니다.

English Explanation:
The writeToBuf function writes data to a bytebufferpool.ByteBuffer in chunks and updates the segment's status. The data is divided into four types of chunks: ChunkTypeFull, ChunkTypeFirst, ChunkTypeMiddle, and ChunkTypeLast.

Each chunk includes a header containing the length, type, and checksum. The payload of the chunk is the actual data you want to write.

The function calculates the starting position of the chunk and returns the position (ChunkPosition). This position can later be used to read the data. Additionally, if the remaining size of the current block cannot accommodate the chunk header, padding is added to start a new block.

If the entire data fits into the current block, it's appended to the buffer with a ChunkTypeFull type. Otherwise, the data is split into multiple chunks and appended to the buffer, with the appropriate chunk type set for each.

Finally, the current block size of the segment is updated, and if necessary, the current block number is also updated.
*/
func (seg *segment) writeToBuf(data []byte, chunkBuf *bytebufferpool.ByteBuffer) (*ChunkPosition, error) {
	startBufLen := chunkBuf.Len()
	padding := uint32(0)

	if seg.closed {
		return nil, ErrClosed
	}
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		if seg.currentBlockSize < blockSize {
			p := make([]byte, blockSize-seg.currentBlockSize)
			chunkBuf.B = append(chunkBuf.B, p...)
			padding += blockSize - seg.currentBlockSize
			seg.currentBlockNumber += 1
			seg.currentBlockSize = 0
		}
	}
	position := &ChunkPosition{
		SegmentId:   seg.id,
		BlockNumber: seg.currentBlockNumber,
		ChunkOffset: int64(seg.currentBlockSize),
	}
	dataSize := uint32(len(data))
	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		seg.appendChunkBuffer(chunkBuf, data, CtFull)
		position.ChunkSize = dataSize + chunkHeaderSize
	} else {
		var (
			leftSize             = dataSize
			blockCount    uint32 = 0
			currBlockSize        = seg.currentBlockSize
		)
		for leftSize > 0 {
			chunkSize := blockSize - currBlockSize - chunkHeaderSize
			if chunkSize > leftSize {
				chunkSize = leftSize
			}
			var end = dataSize - leftSize + chunkSize
			if end > dataSize {
				end = dataSize
			}
			var chunkType ChunkType
			switch leftSize {
			case dataSize:
				chunkType = CtFirst
			case chunkSize:
				chunkType = CtEnd
			default:
				chunkType = CtMid
			}
			seg.appendChunkBuffer(chunkBuf, data[dataSize-leftSize:end], chunkType)
			leftSize -= chunkSize
			blockCount += 1
			currBlockSize = (currBlockSize + chunkSize + chunkHeaderSize) % blockSize
		}
		position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	}
	endBufferLen := chunkBuf.Len()
	if position.ChunkSize+padding != uint32(endBufferLen-startBufLen) {
		panic(fmt.Sprintf("chunksize %d is not equal to the buffer len %d", position.ChunkSize+padding, endBufferLen-startBufLen))
	}

	seg.currentBlockSize += position.ChunkSize
	if seg.currentBlockSize >= blockSize {
		seg.currentBlockNumber += seg.currentBlockSize / blockSize
		seg.currentBlockSize = seg.currentBlockSize % blockSize
	}
	return position, nil
}

/*
한국어 설명:
writeAll 함수는 세그먼트 파일에 배치로 데이터를 쓰는 역할을 합니다.
만약 세그먼트가 닫혀 있다면 ErrClosed 오류를 반환하고,
에러가 발생하면 세그먼트의 상태를 원래대로 복구합니다.
bytebufferpool에서 버퍼를 가져와 데이터를 기록하고,
모든 데이터가 버퍼에 기록된 후에는 세그먼트 파일에 실제로 기록합니다.
각 데이터 항목에 대한 위치 정보(ChunkPosition)를 반환합니다.

English Explanation:
The writeAll function is responsible for writing batch data to a segment file.
If the segment is closed, it returns an ErrClosed error.
In case of any error, it restores the segment to its original state.
It retrieves a buffer from bytebufferpool, writes the data, and after all data is written to the buffer,
it commits the data to the segment file.
It returns the position information (ChunkPosition) for each piece of data.
*/
func (seg *segment) writeAll(data [][]byte) (positions []*ChunkPosition, err error) {
	if seg.closed {
		return nil, ErrClosed
	}
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize

	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()
	var pos *ChunkPosition
	positions = make([]*ChunkPosition, len(data))
	for i := 0; i < len(positions); i++ {
		pos, err = seg.writeToBuf(data[i], chunkBuffer)
		if err != nil {
			return
		}
		positions[i] = pos
	}

	if err = seg.writeChunkBuf(chunkBuffer); err != nil {
		return
	}
	return
}

/*
한국어 설명:
Write 함수는 세그먼트 파일에 단일 데이터 항목을 쓰는 역할을 합니다. 이 함수도 세그먼트가 닫혀 있을 경우 ErrClosed 오류를 반환하며, bytebufferpool에서 버퍼를 가져와 데이터를 기록합니다. 데이터가 성공적으로 버퍼에 기록되면, 세그먼트 파일에 버퍼의 내용을 실제로 기록하고, 데이터의 위치 정보를 반환합니다.

English Explanation:
The Write function writes a single piece of data to the segment file. Similar to writeAll, it returns an ErrClosed error if the segment is closed. It gets a buffer from bytebufferpool, writes the data, and once the data is successfully written to the buffer, it commits the buffer's contents to the segment file and returns the data's position information.
*/
func (seg *segment) Write(data []byte) (pos *ChunkPosition, err error) {
	if seg.closed {
		return nil, ErrClosed
	}
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()
	pos, err = seg.writeToBuf(data, chunkBuffer)
	if err != nil {
		return
	}
	if err = seg.writeChunkBuf(chunkBuffer); err != nil {
		return
	}

	return
}

/*
한국어 설명:
appendChunkBuffer 함수는 bytebufferpool.ByteBuffer에 데이터의 청크와 해당 헤더를 추가하는 역할을 합니다. 청크의 헤더는 데이터의 길이, 타입, 체크섬을 포함하고 있습니다.

길이 정보: 데이터의 길이를 2바이트로 표현하고, 헤더의 4번째와 5번째 인덱스에 저장합니다.
타입 정보: 청크의 타입을 1바이트로 표현하고, 헤더의 6번째 인덱스에 저장합니다.
체크섬 정보: 헤더의 일부와 데이터 자체로부터 체크섬을 계산하고, 이를 헤더의 처음 4바이트에 저장합니다.
이후, 준비된 헤더와 데이터를 버퍼에 추가합니다. 이 함수는 세그먼트 파일에 데이터를 기록할 때 청크 단위로 정보를 정리하는데 사용됩니다.

English Explanation:
The appendChunkBuffer function is responsible for appending a chunk of data and its corresponding header to a bytebufferpool.ByteBuffer. The chunk header includes information about the data's length, type, and checksum.

Length Information: The length of the data is represented in 2 bytes and stored at the 4th and 5th index positions of the header.
Type Information: The type of the chunk is represented in 1 byte and stored at the 6th index position of the header.
Checksum Information: The checksum is calculated from part of the header and the data itself and is stored in the first 4 bytes of the header.
Then, the prepared header and data are appended to the buffer. This function is used to organize information in chunks when writing data to a segment file.
*/
func (seg *segment) appendChunkBuffer(buf *bytebufferpool.ByteBuffer, data []byte, ct ChunkType) {
	binary.LittleEndian.PutUint16(seg.header[4:6], uint16(len(data)))
	seg.header[6] = ct
	sum := crc32.ChecksumIEEE(seg.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(seg.header[:4], sum)

	buf.B = append(buf.B, seg.header...)
	buf.B = append(buf.B, data...)
}

func (seg *segment) writeChunkBuf(buf *bytebufferpool.ByteBuffer) error {
	if seg.currentBlockSize > blockSize {
		panic("exceed the block size")
	}
	if _, err := seg.fd.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	val, _, err := seg.readInternalBlock(blockNumber, chunkOffset)
	return val, err
}

/*
한국어 설명:
readInternalBlock 함수는 세그먼트의 특정 블록 번호와 청크 오프셋에서 데이터를 읽는 내부 함수입니다. 세그먼트가 닫혀 있으면 ErrClosed 오류를 반환합니다. 이 함수는 데이터를 읽고, 읽은 데이터의 청크 포지션(ChunkPosition)과 함께 반환합니다.

함수는 다음 단계를 따릅니다:

블록과 헤더를 담을 임시 구조체를 blockPool에서 가져옵니다.
세그먼트 파일의 크기(segSize)를 계산합니다.
주어진 블록 번호와 청크 오프셋에서 시작하여 데이터를 읽습니다.
캐시에서 데이터 블록을 찾아보고, 캐시에 있으면 해당 블록을 사용합니다.
캐시에 없으면 세그먼트 파일에서 직접 데이터 블록을 읽습니다.
읽은 데이터의 체크섬을 검사하여 데이터 무결성을 확인합니다.
청크 타입에 따라 다음 청크의 위치를 계산합니다.
모든 청크를 순회하며 데이터를 결과 배열에 추가합니다.
읽기 작업이 완료되면 결과 데이터와 다음 청크의 위치 정보를 포함하는 ChunkPosition을 반환합니다. 만약 체크섬이 일치하지 않으면 ErrInvalidCRC 오류를 반환합니다.

English Explanation:
The readInternalBlock function is an internal method for reading data from a specific block number and chunk offset within a segment. If the segment is closed, it returns an ErrClosed error. The function reads the data and returns it along with the chunk's position (ChunkPosition).

The function proceeds as follows:

It retrieves a temporary structure to hold the block and header from the blockPool.
It calculates the size of the segment file (segSize).
It starts reading data from the given block number and chunk offset.
It attempts to read the data block from the cache, and if it's in the cache, it uses that block.
If the block is not in the cache, it reads the data block directly from the segment file.
It verifies the checksum of the read data to ensure data integrity.
It calculates the position of the next chunk based on the chunk type.
It iterates through all the chunks, appending the data to the result array.
Upon completion of the read operation, it returns the result data and the ChunkPosition for the next chunk. If the checksum does not match, it returns an ErrInvalidCRC error.
*/
func (seg *segment) readInternalBlock(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed {
		return nil, nil, ErrClosed
	}
	var (
		result    []byte
		bh        = seg.blockPool.Get().(*blockAndHeader)
		segSize   = seg.Size()
		nextChunk = &ChunkPosition{SegmentId: seg.id}
	)
	defer func() {
		seg.blockPool.Put(bh)
	}()
	for {
		size := int64(blockSize)
		offset := int64(blockNumber) * blockSize
		if size+offset > segSize {
			size = segSize - offset
		}

		if chunkOffset >= size {
			return nil, nil, io.EOF
		}

		var ok bool
		var cachedBlock []byte

		if seg.cache != nil {
			cachedBlock, ok = seg.cache.Get(seg.getCacheKey(blockNumber))
		}
		if ok {
			copy(bh.block, cachedBlock)
		} else {
			_, err := seg.fd.ReadAt(bh.block[0:size], offset)
			if err != nil {
				return nil, nil, err
			}
			if seg.cache != nil && size == blockSize && len(cachedBlock) == 0 {
				cacheBlock := make([]byte, blockSize)
				copy(cacheBlock, bh.block)
				seg.cache.Add(seg.getCacheKey(blockNumber), cacheBlock)
			}
		}

		copy(bh.header, bh.block[chunkOffset:chunkOffset+chunkHeaderSize])

		length := binary.LittleEndian.Uint16(bh.header[4:6])

		start := chunkOffset + chunkHeaderSize
		result = append(result, bh.block[start:start+int64(length)]...)

		checksumEnd := chunkOffset + chunkHeaderSize + int64(length)
		checksum := crc32.ChecksumIEEE(bh.block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(bh.header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		chunkType := bh.header[6]

		if chunkType == CtFull || chunkType == CtEnd {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd
			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			break
		}
		blockNumber += 1
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

func (seg *segment) getCacheKey(blockNumber uint32) uint64 {
	return uint64(seg.id)<<32 | uint64(blockNumber)
}

func (segReader *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	if segReader.segment.closed {
		return nil, nil, ErrClosed
	}
	chunkPosition := &ChunkPosition{
		SegmentId:   segReader.segment.id,
		BlockNumber: segReader.blockNumber,
		ChunkOffset: segReader.chunkOffset,
	}

	value, nextChunk, err := segReader.segment.readInternalBlock(
		segReader.blockNumber,
		segReader.chunkOffset,
	)
	if err != nil {
		return nil, nil, err
	}

	chunkPosition.ChunkSize =
		nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) -
			(segReader.blockNumber*blockSize + uint32(segReader.chunkOffset))

	segReader.blockNumber = nextChunk.BlockNumber
	segReader.chunkOffset = nextChunk.ChunkOffset

	return value, chunkPosition, nil
}

func (cp *ChunkPosition) Encode() []byte {
	return cp.encode(true)
}

func (cp *ChunkPosition) EncodeFixedSize() []byte {
	return cp.encode(false)
}

func (cp *ChunkPosition) encode(shrink bool) []byte {
	buf := make([]byte, maxLen)

	var index = 0
	// SegmentId
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentId))
	// BlockNumber
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	// ChunkOffset
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	// ChunkSize
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))

	if shrink {
		return buf[:index]
	}
	return buf
}

func DecodeChunkPosition(buf []byte) *ChunkPosition {
	if len(buf) == 0 {
		return nil
	}

	var index = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[index:])
	index += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n

	return &ChunkPosition{
		SegmentId:   uint32(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
