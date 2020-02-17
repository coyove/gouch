package main

import (
	"bytes"
	"strings"

	"github.com/coyove/gouch/clock"
	"github.com/coyove/gouch/driver"
)

func (n *Node) rangePartial(key, endKey string, count int, dir int, keyOnly bool) (
	kvs []driver.Entry,
	next string,
	ended bool,
	err error,
) {
	start, _ := getKeyBounds(key, 0)

	m := map[string]driver.Entry{}
	keys := []string{}
	now := clock.Timestamp()

	err = n.db.Seek(start, func(k, v []byte) int {
		if bytes.Equal(k, internalNodeName) {
			return dir
		}

		kv := createDriverEntry(k, v, keyOnly)
		key := kv.RealKey()
		if endKey != "" && strings.Compare(key, endKey) == dir {
			next = key
			ended = true
			return driver.SeekAbort
		}

		upper := n.combineKeyVer(key, now)
		copy(upper[len(upper)-8:], "\xff\xff\xff\xff\xff\xff\xff\xff")

		if bytes.Compare(kv.Key, upper) <= 0 { // Future keys (>0) will not be stored
			if oldEntry, ok := m[key]; !ok {
				m[key] = kv
				keys = append(keys, key)
			} else if bytes.Compare(kv.Key, oldEntry.Key) == 1 {
				m[key] = kv
			}
		}

		if len(keys) >= count+1 {
			next = keys[count]
			keys = keys[:count]
			return driver.SeekAbort
		}

		return dir
	})

	if err != nil {
		return
	}

	for _, k := range keys {
		kvs = append(kvs, m[k])
	}
	return
}

func (n *Node) Range(key, endKey string, count int, keyOnly, includeDeleted, desc bool) (kvs []Entry, next string, err error) {
	dir := driver.SeekNext
	if desc {
		dir = driver.SeekPrev
	}

	next = key
	for len(kvs) < count {
		partial := []driver.Entry{}
		ended := false
		partial, next, ended, err = n.rangePartial(next, endKey, count-len(kvs), dir, keyOnly)

		if err != nil {
			return nil, "", err
		}

		for _, r := range partial {
			if !r.Deleted || includeDeleted {
				kvs = append(kvs, convertEntry(r))
			}
		}

		if next == "" || ended {
			break
		}
	}

	return kvs, next, nil
}
