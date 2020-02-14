package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"path/filepath"
	"sort"
	"time"

	"github.com/coyove/gouch/clock"
)

type repState struct {
	NodeAddress      string
	NodeName         string
	Checkpoint       int64
	LastJobAt        time.Time
	LastJobTimestamp int64
}

func (n *Node) readRepState(name string) *repState {
	fn := filepath.Join(n.path, "replicate_"+name+".log")
	buf, err := ioutil.ReadFile(fn)
	if err != nil {
		log.Println("WARN: read rep state error:", err)
		return &repState{}
	}

	r := &repState{}
	json.Unmarshal(buf, r)
	return r
}

func (n *Node) writeRepState(name string, r *repState) {
	r.LastJobAt = time.Now()
	r.LastJobTimestamp = clock.Timestamp()

	buf, _ := json.Marshal(r)

	fn := filepath.Join(n.path, "replicate_"+name+".log")
	if err := ioutil.WriteFile(fn, buf, 0777); err != nil {
		log.Println("WARN: write rep state error:", err)
	}
}

func (n *Node) GetChangedKeysSince(startTimestamp int64, count int) (*Pairs, error) {
	c, err := n.log.GetCursor(startTimestamp)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	res := &Pairs{}

	for len(res.Data) < count {
		ts, key, err := c.Data()
		if err != nil {
			return nil, err
		}

		dbkey := n.combineKeyVer(string(key), ts)
		k, v, err := n.db.Get(dbkey)
		if err != nil {
			return nil, err
		}

		if bytes.Equal(k, dbkey) {
			res.Data = append(res.Data, Pair{k, v})
		}

		if !c.Next() {
			break
		}
	}

	if len(res.Data) > 0 {
		_, res.Next, _ = res.Data[len(res.Data)-1].SplitKeyInfo()
		res.Next++
	}
	return res, nil
}

func (n *Node) PutKeyParis(pairs []Pair) error {
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].Key, pairs[j].Key) == -1
	})

	kvs := [][]byte{}
	for _, p := range pairs {
		if len(p.Key) == 0 {
			continue
		}
		kvs = append(kvs, p.Key, p.Value)
	}
	return n.db.Put(kvs...)
}
