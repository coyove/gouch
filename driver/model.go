package driver

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

type Entry struct {
	Key      []byte `protobuf:"bytes,1,rep" json:"key"`
	Value    []byte `protobuf:"bytes,2,rep" json:"value"`
	ValueLen int64  `protobuf:"fixed64,3,opt" json:"ver"`
}

func (p Entry) dup() Entry {
	return Entry{
		Key:      append([]byte{}, p.Key...),
		Value:    append([]byte{}, p.Value...),
		ValueLen: p.ValueLen,
	}
}

func (p Entry) RealKey() string {
	idx := bytes.IndexByte(p.Key, 0)
	if idx == -1 || len(p.Key[idx:]) != 16 {
		panic(fmt.Sprintf("invalid key: %q", p.Key))
	}
	return string(p.Key[:idx])
}

func (p Entry) Node() string {
	idx := bytes.IndexByte(p.Key, 0)
	if idx == -1 || len(p.Key[idx:]) != 16 {
		panic(fmt.Sprintf("invalid key: %q", p.Key))
	}
	return base64.URLEncoding.EncodeToString(p.Key[idx+8:])[:10]
}

func (p Entry) Version() int64 {
	idx := bytes.IndexByte(p.Key, 0)
	if idx == -1 || len(p.Key[idx:]) != 16 {
		panic(fmt.Sprintf("invalid key: %q", p.Key))
	}
	return int64(binary.BigEndian.Uint64(p.Key[idx:]))
}

func (p Entry) String() string {
	return fmt.Sprintf("%s/%x-%s:%q", p.RealKey(), p.Version(), p.Node()[:4], p.Value)
}
