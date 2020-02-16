package main

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/coyove/gouch/driver"
	"github.com/gogo/protobuf/proto"
)

type Pairs struct {
	Data             []driver.Entry `protobuf:"bytes,1,rep" json:"data"`
	Next             int64          `protobuf:"fixed64,2,opt" json:"next"`
	NodeInternalName string         `protobuf:"bytes,3,opt" json:"node_internal_name"`
}

func (p *Pairs) Reset() { *p = Pairs{} }

func (p *Pairs) String() string { return proto.CompactTextString(p) }

func (p *Pairs) ProtoMessage() {}

type Entry struct {
	Key      string    `json:"key,omitempty"`
	Value    string    `json:"value,omitempty"`
	Ver      int64     `json:"ver,omitempty"`
	ValueLen int64     `json:"value_len,omitempty"`
	Unix     time.Time `json:"unix_ts,omitempty"`
	Node     string    `json:"node,omitempty"`
	Future   bool      `json:"future,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
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

func jsonBytes(buf []byte) string {
	if utf8.Valid(buf) {
		return string(buf)
	}
	return "[... binary data " + strconv.Itoa(len(buf)) + "b]"
}
