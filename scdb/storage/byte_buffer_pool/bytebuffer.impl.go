package bytebufferpool

import "io"

/*
----------------------------------------------------------------
-KOR
ByteBuffer 구조체는 Go 언어로 작성된 바이트 버퍼를 관리하기 위한 사용자 정의 타입입니다. 다양한 메서드를 통해 데이터를 읽고 쓰며, 버퍼의 내용을 조작할 수 있습니다:

구조체 정의 (ByteBuffer):

B []byte: 바이트 버퍼의 실제 데이터를 저장하는 슬라이스입니다.
메서드:

Len(): 버퍼에 저장된 바이트의 현재 길이(수)를 반환합니다.

ReadFrom(r io.Reader): io.Reader로부터 데이터를 읽어 버퍼에 추가합니다. 필요한 경우 버퍼의 용량을 늘리고, io.EOF가 발생하거나 오류가 발생할 때까지 읽기를 계속합니다.

WriteTo(w io.Writer): 버퍼의 내용을 io.Writer에 쓰고, 쓴 바이트 수와 오류를 반환합니다.

Bytes(): 버퍼에 저장된 바이트 슬라이스를 반환합니다. 이 메서드는 bytes.Buffer와의 호환성을 위해 있습니다.

Write(p []byte): 바이트 슬라이스 p를 버퍼에 추가합니다.

WriteByte(c byte): 단일 바이트 c를 버퍼에 추가합니다. 항상 성공하기 때문에 nil을 반환합니다.

WriteString(s string): 문자열 s를 버퍼에 추가합니다.

Set(p []byte): 버퍼의 내용을 주어진 바이트 슬라이스 p로 설정합니다. 기존 데이터를 대체합니다.

SetString(s string): 버퍼의 내용을 주어진 문자열 s로 설정합니다. 기존 데이터를 대체합니다.

String(): 버퍼의 내용을 문자열로 반환합니다.

Reset(): 버퍼를 비웁니다. 길이를 0으로 설정하지만, 기존 배열의 용량은 변경되지 않아 메모리 할당을 재사용할 수 있습니다.


----------------------------------------------------------------
-ENG

Struct Definition:

B []byte: This is the underlying slice that holds the buffer's data.
Methods:

Len(): Returns the length of the buffer (number of bytes currently stored in B).

ReadFrom(r io.Reader): Reads data from any io.Reader and appends it to the buffer. It starts by making sure there is enough capacity in the buffer, doubling it if necessary, until it reads an io.EOF indicating the end of the reader or encounters an error.

WriteTo(w io.Writer): Writes the buffer's content to any io.Writer and returns the number of bytes written and any error encountered.

Bytes(): Returns the slice of bytes currently in the buffer. This method is for compatibility with bytes.Buffer.

Write(p []byte): Appends the slice p to the buffer, increasing its length.

WriteByte(c byte): Appends a single byte c to the buffer. It always returns nil because it's guaranteed to succeed.

WriteString(s string): Appends a string s to the buffer, increasing its length by the length of the string.

Set(p []byte): Sets the buffer's content to the provided slice p, replacing any existing data.

SetString(s string): Sets the buffer's content to the provided string s, replacing any existing data.

String(): Returns a string representation of the buffer's contents.

Reset(): Empties the buffer by setting its length to zero, but the underlying array's capacity remains unchanged, allowing for reusing the memory allocation.

*/
type ByteBufferInterface interface {
	Len() int
	ReadFrom(r io.Reader) (int64, error)
	WriteTo(w io.Writer) (int64, error)
	Bytes() []byte
	Write(p []byte) (int, error)
	WriteByte(b byte) error
	WriteString(s string) (int, error)
	Set(b []byte)
	SetString(s string)
	String() string
	Reset()
}

var _ ByteBufferInterface = (*ByteBuffer)(nil)

/*
----------------------------------------------------------------
-KOR
메모리 할당 및 가비지 컬렉션 오버헤드를 줄이기 위해 ByteBuffer 객체를 재사용할 수 있도록 하는 바이트 버퍼 풀을 구현하고 있습니다.
상수 정의:

minimumBitSize, steps, minSize, maxSize는 버퍼 풀에서 관리할 버퍼의 최소 및 최대 크기를 결정하는 데 사용됩니다.
calibrateCallsThreshold와 maxPercentile은 풀의 크기를 조정하는 데 사용되는 임계값과 비율입니다.
Pool 구조체:

calls 배열은 각 크기의 버퍼가 얼마나 자주 요청되는지 추적합니다.
calibrating은 풀이 현재 크기를 조정 중인지를 나타내는 플래그입니다.
defaultSize와 maxSize는 풀에서 생성하는 버퍼의 기본 및 최대 크기입니다.
pool은 실제로 ByteBuffer 객체를 재사용하기 위한 sync.Pool입니다.
defaultPool:

defaultPool은 사용자가 따로 풀을 초기화하지 않아도 기본적으로 사용할 수 있는 전역 풀입니다.
Get 함수:

Get() 메서드는 풀에서 ByteBuffer 객체를 가져오거나 새로운 것을 생성하여 반환합니다.
Put 함수:

Put() 메서드는 사용이 끝난 ByteBuffer 객체를 풀에 반환합니다. 이때 객체의 크기에 따라 필요하면 풀의 크기를 조정하는 calibrate 메서드를 호출합니다.
calibrate 메서드:

calibrate() 메서드는 풀의 크기를 동적으로 조정합니다. 각 크기의 버퍼에 대한 호출 빈도수를 기반으로 가장 효율적인 defaultSize와 maxSize를 계산합니다.
callSize 및 callSizes 타입:

이들은 풀 크기 조정 알고리즘에서 사용되는 내부 데이터 구조체로, 각 크기의 버퍼에 대한 호출 횟수(calls)와 해당 크기(size)를 저장합니다.
callSizes는 sort.Interface를 구현하여 호출 횟수에 따라 정렬될 수 있습니다.
index 함수:

index() 함수는 주어진 크기의 버퍼가 어느 인덱스에 해당하는지 계산합니다. 이는 버퍼 크기를 이진 단계로 매핑하여 calls 배열의 적절한 위치를 찾는 데 사용됩니다.

----------------------------------------------------------------
-ENG
This pool is used to reuse ByteBuffer instances to reduce memory allocation and garbage collection overhead.

Constants:

minimumBitSize, steps, minSize, and maxSize are constants that define the minimum and maximum buffer sizes managed by the pool.
calibrateCallsThreshold and maxPercentile are thresholds used for calibrating the pool's size based on usage patterns.
Pool Struct:

The calls array tracks how often buffers of each size are requested.
calibrating is a flag indicating whether the pool is currently adjusting its size.
defaultSize and maxSize define the default and maximum sizes of buffers created by the pool.
pool is a sync.Pool that is actually used to reuse ByteBuffer instances.
defaultPool:

defaultPool is a global pool instance that can be used without explicit initialization.
Get Function:

The Get() method retrieves a ByteBuffer from the pool or creates a new one if the pool is empty.
Put Function:

The Put() method returns a used ByteBuffer to the pool. If necessary, it may trigger the calibrate method to adjust the pool's size based on the buffer's size.
calibrate Method:

The calibrate() method dynamically adjusts the size of the pool. It calculates the most efficient defaultSize and maxSize based on the frequency of calls for buffers of each size.
callSize and callSizes Types:

These are internal data structures used in the pool's sizing algorithm. They hold the number of calls (calls) and the corresponding size (size) for buffers.
callSizes implements the sort.Interface to be sortable by the number of calls.
index Function:

The index() function calculates the index for a buffer of a given size. This is used to map the buffer size to a binary step and find the appropriate position in the calls array.
*/
type PoolInterface interface {
	Get() *ByteBuffer
	Put(b *ByteBuffer)
}

var _ PoolInterface = (*Pool)(nil)

type callSizeInterface interface {
	Len() int
	Less(i, j int) bool
	Swap(i, j int)
}
