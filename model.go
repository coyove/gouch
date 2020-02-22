package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
	"unsafe"

	"github.com/coyove/gouch/clock"
)

var (
	deletionUUID = []byte{0x91, 0xee, 0x48, 0xda, 0x52, 0x75, 0x4e, 0xc7, 0xa5, 0x76, 0xcb, 0x80, 0xad, 0x1c, 0x12, 0x03}
	appendUUID   = []byte{0x92, 0xef, 0x49, 0xdb, 0x53, 0x76, 0x4f, 0xc8, 0xa6, 0x77, 0xcc, 0x81, 0xae, 0x1d, 0x13, 0x04}
)

type Entry struct {
	Key      string    `json:"key,omitempty"`
	Value    string    `json:"value,omitempty"`
	Ver      int64     `json:"version,omitempty"`
	ValueLen int64     `json:"length,omitempty"`
	Unix     time.Time `json:"unix_ts,omitempty"`
	Node     string    `json:"node,omitempty"`
	Future   bool      `json:"future,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
	Append   bool      `json:"append,omitempty"`
}

func createEntry(k, v []byte, keyOnly bool) (e Entry) {
	e.ValueLen, e.Deleted, e.Append =
		int64(len(v)),
		bytes.Equal(v, deletionUUID),
		bytes.HasPrefix(v, appendUUID)

	if e.Append {
		v = v[16:]
		e.ValueLen -= 16
	}
	if keyOnly {
		v = nil
	}
	if e.Deleted {
		v, e.ValueLen = nil, 0
	}

	ver := versionInKey(k)
	e.Key = string(k[:bytes.IndexByte(k, 0)])
	e.Value = string(v)
	e.Ver = ver
	e.Node = bytesToNodeName(k[bytes.IndexByte(k, 0)+8:])
	e.Unix = time.Unix(clock.UnixSecFromTimestamp(ver), 0)
	return
}

func versionInKey(key []byte) int64 {
	idx := bytes.IndexByte(key, 0)
	if idx == -1 || len(key[idx:]) != 16 {
		panic(fmt.Sprintf("invalid key: %q", key))
	}
	return int64(binary.BigEndian.Uint64(key[idx:]))
}

func (p Entry) String() string {
	return fmt.Sprintf("%s/%x-%s:%q", p.Key, p.Ver, p.Node, p.Value)
}

func (e Entry) ValueBytes() []byte {
	x := struct {
		a string
		b int
	}{e.Value, len(e.Value)}
	return *(*[]byte)(unsafe.Pointer(&x))
}
