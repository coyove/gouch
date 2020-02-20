package main

import (
	"bytes"
	"strings"

	"github.com/coyove/gouch/clock"
	"github.com/coyove/gouch/driver"
)

func (n *Node) rangePartial(key, endKey string, count int, dir int, keyOnly bool) (
	kvs []Entry,
	next string,
	ended bool,
	err error,
) {
	start, upper := getKeyBounds(key, 0)
	if dir == driver.SeekPrev {
		start = upper
	}

	m, keys := map[string]Entry{}, []string{}
	now := clock.Timestamp()
	shouldSkipFirst := dir == driver.SeekPrev

	err = n.db.Seek(start, func(k, v []byte) int {
		if shouldSkipFirst {
			shouldSkipFirst = false
			return dir
		}

		if bytes.Equal(k, internalNodeName) {
			return dir
		}

		kv := createEntry(k, v, keyOnly)
		key := kv.Key
		if endKey != "" && strings.Compare(key, endKey) == dir {
			next = key
			ended = true
			return driver.SeekAbort
		}

		upper := n.combineKeyVer(key, now)
		copy(upper[len(upper)-8:], "\xff\xff\xff\xff\xff\xff\xff\xff")

		if bytes.Compare(k, upper) <= 0 { // Future keys (>0) will not be stored
			if _, ok := m[key]; !ok {
				keys = append(keys, key)
			}

			if dir == driver.SeekNext {
				m[key] = kv
			} else {
				if _, ok := m[key]; !ok {
					m[key] = kv
				}
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
		partial := []Entry{}
		ended := false
		partial, next, ended, err = n.rangePartial(next, endKey, count-len(kvs), dir, keyOnly)

		if err != nil {
			return nil, "", err
		}

		for _, r := range partial {
			if !r.Deleted || includeDeleted {
				kvs = append(kvs, r)
			}
		}

		if next == "" || ended {
			break
		}
	}

	return kvs, next, nil
}
