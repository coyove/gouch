package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
)

type Pairs struct {
	Data             []Pair `protobuf:"bytes,1,rep" json:"data"`
	Next             int64  `protobuf:"fixed64,2,opt" json:"next"`
	NodeInternalName string `protobuf:"bytes,3,opt" json:"node_internal_name"`
}

func (p *Pairs) Reset() { *p = Pairs{} }

func (p *Pairs) String() string { return proto.CompactTextString(p) }

func (p *Pairs) ProtoMessage() {}

type Pair struct {
	Key   []byte `protobuf:"bytes,1,rep" json:"key"`
	Value []byte `protobuf:"bytes,2,rep" json:"value"`
}

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
	w.Header().Add("X-Server", "gouch")

	m := map[string]interface{}{}
	for i := 0; i < len(kvs); i += 2 {
		m[kvs[i].(string)] = kvs[i+1]
	}

	var buf []byte
	if r.FormValue("pretty") != "" || strings.Contains(r.UserAgent(), "Mozilla") {
		buf, _ = json.MarshalIndent(m, "", "  ")
	} else {
		buf, _ = json.Marshal(m)
	}

	w.Header().Add("Content-Type", "application/json")
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

func bytesToNodeName(p []byte) string {
	return base64.URLEncoding.EncodeToString(p)[:10]
}
