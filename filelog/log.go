package filelog

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/coyove/common/clock"
)

type Handler struct {
	sync.Mutex
	f    *os.File
	path string
}

func getLastRecord(f *os.File) (int64, []byte, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, nil, err
	}
	end := fi.Size()
	if end == 0 {
		return 0, nil, nil
	}

	if end < 32 {
		return 0, nil, fmt.Errorf("corrupted data: too short")
	}

	if _, err := f.Seek(end-32, 0); err != nil {
		return 0, nil, err
	}

	buf := make([]byte, 32)
	if _, err := io.ReadFull(f, buf); err != nil {
		return 0, nil, err
	}

	head := binary.BigEndian.Uint64(buf)
	ts := int64(head << 8 >> 8)
	ln := byte(head >> 56)
	if ln > 24 {
		return 0, nil, fmt.Errorf("invalid head length: %v", ln)
	}
	return ts, buf[8 : 8+ln], nil
}

func Open(path string) (*Handler, error) {
	ts := clock.Timestamp()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	lastts, lastk, err := getLastRecord(f)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(0, 2); err != nil {
		return nil, err
	}

	if ts < lastts {
		return nil, fmt.Errorf("time skew: last: %v, now: %v", lastts, ts)
	}

	log.Printf("Last record from: %s, key: %q, timestamp: %v, now: %v", path, lastk, lastts, ts)

	return &Handler{
		f:    f,
		path: path,
	}, nil
}

func (handle *Handler) GetTimestampForKey(key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, fmt.Errorf("null key not allowed")
	}

	handle.Lock()
	defer handle.Unlock()

	ts := clock.Timestamp()

	ln := uint64(len(key))
	if ln > 24 {
		ln = 24
	}

	p := make([]byte, 32)
	binary.BigEndian.PutUint64(p, uint64(ts)|(ln<<56))
	copy(p[8:], key)

	if _, err := handle.f.Write(p); err != nil {
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
	c.offset += 32
	return c.offset < c.end
}

func (c *Cursor) Data() (int64, []byte, error) {
	if _, err := c.fd.Seek(c.offset, 0); err != nil {
		return 0, nil, err
	}

	buf := make([]byte, 32)
	if _, err := io.ReadFull(c.fd, buf); err != nil {
		return 0, nil, err
	}

	head := binary.BigEndian.Uint64(buf)
	ts := int64(head << 8 >> 8)
	ln := byte(head >> 56)
	if ln > 24 {
		return 0, nil, fmt.Errorf("invalid head length: %v", ln)
	}
	return ts, buf[8 : 8+ln], nil
}

func (c *Cursor) Close() error {
	return c.fd.Close()
}

func (handle *Handler) GetCursor(startTimestamp int64) (*Cursor, error) {
	fi, err := os.Stat(handle.path)
	if err != nil {
		return nil, err
	}

	end := fi.Size()
	if end/32*32 != end {
		return nil, fmt.Errorf("corrupted data, not 32 bytes aligned")
	}

	f, err := os.Open(handle.path)
	if err != nil {
		return nil, err
	}

	start := int64(0)
	buf := make([]byte, 32)
	c := &Cursor{
		fd:  f,
		end: end,
	}

	for start < end-32 {
		h := (start + end) / 2
		h = h / 32 * 32

		if _, err := f.Seek(h, 0); err != nil {
			f.Close()
			return nil, err
		}

		if _, err := io.ReadFull(f, buf); err != nil {
			f.Close()
			return nil, err
		}

		ts := int64(binary.BigEndian.Uint64(buf) << 8 >> 8)

		// log.Println(ts, startTimestamp, string(bytes.Trim(buf[8:], "\x00")), start, h, end)

		if startTimestamp == ts {
			c.offset = h
			return c, nil
		} else if startTimestamp > ts {
			start = h + 32
		} else {
			end = h
		}
	}

	c.offset = start
	return c, nil
}
