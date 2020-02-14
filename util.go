package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gogo/protobuf/proto"
)

type Pair struct {
	Key   []byte `protobuf:"bytes,1,rep" json:"key"`
	Value []byte `protobuf:"bytes,2,rep" json:"value"`
}

func (p Pair) SplitKeyInfo() (string, int64, string) {
	idx := bytes.IndexByte(p.Key, 0)
	if idx == -1 || len(p.Key[idx:]) != 16 {
		panic(fmt.Sprintf("invalid key: %q", p.Key))
	}
	return string(p.Key[:idx]),
		int64(binary.BigEndian.Uint64(p.Key[idx:])),
		base64.URLEncoding.EncodeToString(p.Key[idx+8:])
}

func (p Pair) String() string {
	k, ts, node := p.SplitKeyInfo()
	return fmt.Sprintf("%s/%x-%s:%q", k, ts, node[:4], p.Value)
}

type Pairs struct {
	Data []Pair `protobuf:"bytes,1,rep" json:"data"`
	Next int64  `protobuf:"fixed64,2,rep" json:"next"`
}

func (p *Pairs) Reset() { *p = Pairs{} }

func (p *Pairs) String() string { return proto.CompactTextString(p) }

func (p *Pairs) ProtoMessage() {}

func getKeyBounds(key string, startTimestamp int64) (lower []byte, upper []byte) {
	lower = append([]byte(key),
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(lower[len(key):], uint64(startTimestamp))
	lower[len(key)] = 0

	upper = append([]byte(key),
		0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
	return
}

func writeJSON(w http.ResponseWriter, r *http.Request, kvs ...interface{}) {
	m := map[string]interface{}{}

	for i := 0; i < len(kvs); i += 2 {
		m[kvs[i].(string)] = kvs[i+1]
	}

	var buf []byte
	if r.FormValue("pretty") != "" {
		buf, _ = json.MarshalIndent(m, "", "  ")
	} else {
		buf, _ = json.Marshal(m)
	}

	w.Header().Add("Content-Type", "application/json")
	w.Header().Add("X-Server", "gouch")
	w.Write(buf)
}

func writeProtobuf(w http.ResponseWriter, r *http.Request, m *Pairs) {
	if m != nil {
		w.Header().Add("X-Payload-Count", strconv.Itoa(len(m.Data)))
	}

	if r.FormValue("pretty") != "" {
		writeJSON(w, r, "ok", true, "data", m)
		return
	}
	buf, _ := proto.Marshal(m)
	w.Header().Add("Content-Type", "application/protobuf")
	w.Header().Add("X-Server", "gouch")
	w.Write(buf)
}
