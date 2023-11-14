package internal

import (
	"errors"
	"io"
)

var ErrTooLarge = errors.New("ReaderBuffer: too large")

type ReaderBuffer struct {
	reader io.Reader
	buf    []byte
	max    int // max buf Len
	begin  int // data begin, [)
	end    int // data end, [)
}

func NewReaderBuffer(reader io.Reader, len, max int) *ReaderBuffer {
	return &ReaderBuffer{
		reader: reader,
		buf:    make([]byte, len, len),
		max:    max,
	}
}

func (b *ReaderBuffer) Release() {
	b.reader = nil
	b.buf = nil
}

func (b *ReaderBuffer) Len() int {
	return b.end - b.begin
}

func (b *ReaderBuffer) Data() []byte {
	return b.buf[b.begin:b.end]
}

// Read discards offset bytes，then Read n bytes
// b.begin+offset+n <= b.end, equivalent to b.Len() >= offset+n
func (b *ReaderBuffer) Read(offset, n int, out []byte) {
	b.begin += offset
	copy(out, b.buf[b.begin:b.begin+n])
	b.begin += n
}

func (b *ReaderBuffer) ReadFromReader() (int, error) {
	if !b.grow() {
		return 0, ErrTooLarge
	}
	n, err := b.reader.Read(b.buf[b.end:])
	if err != nil {
		return n, err
	}
	b.end += n
	return n, nil
}

func (b *ReaderBuffer) grow() bool {
	if b.begin == 0 {
		l := len(b.buf)
		if b.end >= l {
			if b.end >= b.max {
				return false
			}
			double := l + l
			if double > b.max {
				double = b.max
			}
			buf := make([]byte, double)
			copy(buf, b.buf)
			b.buf = buf
		}
		return true
	}
	if b.begin == b.end {
		b.begin, b.end = 0, 0
		return true
	}
	copy(b.buf, b.buf[b.begin:b.end])
	b.end -= b.begin
	b.begin = 0
	return true
}
