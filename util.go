package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/gouch/clock"
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
	CasValue string    `json:"cas_value,omitempty"`
	Ver      int64     `json:"ver,omitempty"`
	ValueLen int64     `json:"value_len,omitempty"`
	Unix     time.Time `json:"unix_ts,omitempty"`
	Node     string    `json:"node,omitempty"`
	Future   bool      `json:"future,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
	Cas      bool      `json:"cas,omitempty"`
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

func createDriverEntry(k, v []byte, keyOnly bool) driver.Entry {
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

	return driver.Entry{
		Key:      append([]byte{}, k...),
		Value:    append([]byte{}, v...),
		ValueLen: int64(l),
		Deleted:  deleted,
		Cas:      cas,
		CasValue: newValue,
	}
}

func convertEntry(e driver.Entry) Entry {
	return Entry{
		Key:      e.RealKey(),
		Value:    string(e.Value),
		CasValue: string(e.CasValue),
		ValueLen: e.ValueLen,
		Ver:      e.Version(),
		Node:     e.Node(),
		Unix:     time.Unix(clock.UnixSecFromTimestamp(e.Version()), 0),
		Deleted:  e.Deleted,
		Cas:      e.Cas,
	}
}
