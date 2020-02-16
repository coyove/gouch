package main

import (
	"bytes"
	"time"

	"github.com/coyove/gouch/clock"
	"github.com/coyove/gouch/driver"
)

func (n *Node) rangePartial(key string, count int, dir int, keyOnly bool) (kvs []driver.Entry, next string, err error) {
	start, _ := getKeyBounds(key, 0)
	m := map[string]driver.Entry{}
	keys := []string{}

	err = n.db.Seek(start, func(k, v []byte) int {
		if bytes.Equal(k, internalNodeName) {
			return dir
		}

		l := len(v)
		if keyOnly {
			v = nil
		}

		kv := driver.Entry{
			Key:      append([]byte{}, k...),
			Value:    append([]byte{}, v...),
			ValueLen: int64(l),
		}

		key := kv.RealKey()

		upper := n.combineKeyVer(key, clock.Timestamp())
		copy(upper[len(upper)-8:], "\xff\xff\xff\xff\xff\xff\xff\xff")

		if bytes.Compare(kv.Key, upper) <= 0 {
			if oldEntry, ok := m[key]; !ok {
				m[key] = kv
				keys = append(keys, key)
			} else if bytes.Compare(kv.Key, oldEntry.Key) == 1 {
				m[key] = kv
			}
		}

		if len(keys) >= count+1 {
			return driver.SeekAbort
		}
		return dir
	})

	if err != nil {
		return nil, "", err
	}

	if len(keys) >= count+1 {
		next = keys[count]
		keys = keys[:count]
	}

	for _, k := range keys {
		kvs = append(kvs, m[k])
	}

	return
}

func (n *Node) Range(key string, count int, keyOnly bool, desc bool) (kvs []Entry, next string, err error) {
	dir := driver.SeekNext
	if desc {
		dir = driver.SeekPrev
	}
	next = key
	for len(kvs) < count {
		partial := []driver.Entry{}
		partial, next, err = n.rangePartial(next, count-len(kvs), dir, keyOnly)

		if err != nil {
			return nil, "", err
		}

		for _, r := range partial {
			if !bytes.Equal(r.Value, deletionUUID) {
				kvs = append(kvs, Entry{
					Key:      r.RealKey(),
					Value:    string(r.Value),
					ValueLen: r.ValueLen,
					Ver:      r.Version(),
					Node:     r.Node(),
					Unix:     time.Unix(clock.UnixSecFromTimestamp(r.Version()), 0),
				})
			}
		}

		if next == "" {
			break
		}
	}

	return kvs, next, nil
}
