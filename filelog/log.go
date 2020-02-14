package filelog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/coyove/gouch/clock"
)

const (
	blockSize    = 24
	blockKeySize = blockSize - 8
)

type Handler struct {
	sync.Mutex
	f    *os.File
	path string
}

func getHeadLastTimestamp(f *os.File) (int64, int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	end := fi.Size()
	if end == 0 {
		return 0, 0, nil
	}

	if end/blockSize*blockSize != end {
		return 0, 0, fmt.Errorf("corrupted data: not aligned")
	}

	c := Cursor{fd: f}
	last, _, err := c.readBlock(end - blockSize)
	if err != nil {
		return 0, 0, err
	}

	head, _, err := c.readBlock(0)
	if err != nil {
		return 0, 0, err
	}
	return head, last, nil
}

func Open(path string) (*Handler, error) {
	ts := clock.Timestamp()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	_, lastts, err := getHeadLastTimestamp(f)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(0, 2); err != nil {
		return nil, err
	}

	if ts < lastts {
		return nil, fmt.Errorf("filelog time skew: last: %v, now: %v", lastts, ts)
	}

	return &Handler{f: f, path: path}, nil
}

func (handle *Handler) Close() error {
	return handle.f.Close()
}

func (handle *Handler) GetTimestampForKey(key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, fmt.Errorf("null key not allowed")
	}

	handle.Lock()
	defer handle.Unlock()

	ts := clock.Timestamp()

	p := make([]byte, blockSize)
	buf := bytes.Buffer{}

	for i := 0; i < len(key); i += blockKeySize {
		end := i + blockKeySize
		if end > len(key) {
			end = len(key)
		}

		ln := uint64(len(key[i:end]))

		binary.BigEndian.PutUint64(p, uint64(ts)|(ln<<56))
		copy(p[8:], key[i:end])

		buf.Write(p)
	}

	if _, err := handle.f.Write(buf.Bytes()); err != nil {
		return 0, err
	}

	return ts, nil
}

type Cursor struct {
	fd     *os.File
	offset int64
	end    int64
}

func (c *Cursor) Next() bool {
	c.offset += blockSize
	return c.offset < c.end
}

func (c *Cursor) Data() (int64, []byte, error) {
	ts, key, err := c.readBlock(c.offset)
	if err == nil {
		for off := c.offset + blockSize; c.offset < c.end; off += blockSize {
			ts2, key2, err := c.readBlock(off)
			if err != nil {
				break
			}
			if ts2 != ts {
				break
			}
			key = append(key, key2...)
			c.offset += blockSize
		}
	}
	return ts, key, err
}

func (c *Cursor) readBlock(offset int64) (int64, []byte, error) {
	if _, err := c.fd.Seek(offset, 0); err != nil {
		return 0, nil, err
	}

	buf := make([]byte, blockSize)
	if _, err := io.ReadFull(c.fd, buf); err != nil {
		return 0, nil, err
	}

	head := binary.BigEndian.Uint64(buf)
	ts := int64(head << 8 >> 8)
	ln := byte(head >> 56)
	if ln > blockKeySize {
		return 0, nil, fmt.Errorf("invalid head length: %v", ln)
	}
	return ts, buf[8 : 8+ln], nil
}

func (c *Cursor) Close() error {
	return c.fd.Close()
}

func (c *Cursor) findNeig() {
	ts, _, err := c.readBlock(c.offset)
	if err != nil {
		return
	}

	for c.offset > 0 {
		ts2, _, err := c.readBlock(c.offset - blockSize)
		if err != nil {
			return
		}
		if ts != ts2 {
			break
		}
		c.offset -= blockSize
	}
}

func (handle *Handler) GetCursor(startTimestamp int64) (*Cursor, error) {
	fi, err := os.Stat(handle.path)
	if err != nil {
		return nil, err
	}

	end := fi.Size()
	if end/blockSize*blockSize != end {
		return nil, fmt.Errorf("corrupted data, not %v bytes aligned", blockSize)
	}

	f, err := os.Open(handle.path)
	if err != nil {
		return nil, err
	}

	start := int64(0)
	c := &Cursor{
		fd:  f,
		end: end,
	}

	for start <= end-blockSize {
		h := (start + end) / 2
		h = h / blockSize * blockSize

		ts, _, err := c.readBlock(h)
		if err != nil {
			f.Close()
			return nil, err
		}

		// log.Println(ts, startTimestamp, string(bytes.Trim(buf[8:], "\x00")), start, h, end)
		if startTimestamp == ts {
			c.offset = h
			c.findNeig()
			return c, nil
		}

		if startTimestamp > ts {
			start = h + blockSize
		} else {
			end = h
		}
	}

	c.offset = start
	// c.findNeig()
	return c, nil
}
