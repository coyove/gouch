package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"time"
	"unsafe"

	"github.com/coyove/gouch/clock"
)

var (
	deletionUUID = []byte{0x91, 0xee, 0x48, 0xda, 0x52, 0x75, 0x4e, 0xc7, 0xa5, 0x76, 0xcb, 0x80, 0xad, 0x1c, 0x12, 0x03}
	casUUID      = []byte{0x92, 0xef, 0x49, 0xdb, 0x53, 0x76, 0x4f, 0xc8, 0xa6, 0x77, 0xcc, 0x81, 0xae, 0x1d, 0x13, 0x04}
)

type Entry struct {
	Key      string    `json:"key,omitempty"`
	Value    string    `json:"value,omitempty"`
	CasValue string    `json:"cas_value,omitempty"`
	Ver      int64     `json:"ver,omitempty"`
	ValueLen int64     `json:"value_len,omitempty"`
	Unix     time.Time `json:"unix_ts,omitempty"`
	Node     string    `json:"node,omitempty"`
	Future   bool      `json:"future,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
	Cas      bool      `json:"cas,omitempty"`
}

func createEntry(k, v []byte, keyOnly bool) Entry {
	l, deleted := len(v), bytes.Equal(v, deletionUUID)

	var newValue []byte
	var cas bool

	if bytes.HasPrefix(v, casUUID) {
		if idx := bytes.Index(v[16:], casUUID); idx != -1 {
			v, newValue = v[16:16+idx], v[16+idx+16:]
			cas = true
		}
	}

	if keyOnly {
		v = nil
		newValue = nil
	}

	if deleted {
		v, l = nil, 0
	}

	ver := versionInKey(k)
	return Entry{
		Key:      realKeyInKey(k),
		Value:    string(v),
		CasValue: string(newValue),
		ValueLen: int64(l),
		Ver:      ver,
		Node:     nodeNameInKey(k),
		Unix:     time.Unix(clock.UnixSecFromTimestamp(ver), 0),
		Deleted:  deleted,
		Cas:      cas,
	}
}

func versionInKey(key []byte) int64 {
	idx := bytes.IndexByte(key, 0)
	if idx == -1 || len(key[idx:]) != 16 {
		panic(fmt.Sprintf("invalid key: %q", key))
	}
	return int64(binary.BigEndian.Uint64(key[idx:]))
}

func realKeyInKey(key []byte) string {
	idx := bytes.IndexByte(key, 0)
	if idx == -1 || len(key[idx:]) != 16 {
		panic(fmt.Sprintf("invalid key: %q", key))
	}
	return string(key[:idx])
}

func nodeNameInKey(key []byte) string {
	idx := bytes.IndexByte(key, 0)
	if idx == -1 || len(key[idx:]) != 16 {
		panic(fmt.Sprintf("invalid key: %q", key))
	}
	return base64.URLEncoding.EncodeToString(key[idx+8:])[:10]
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
