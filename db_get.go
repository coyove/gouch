package main

import (
	"bytes"
	"encoding/binary"

	"github.com/coyove/gouch/clock"
)

func (n *Node) Get(key string) (Entry, error) {
	start := n.combineKeyVer(key, clock.Timestamp())
	copy(start[len(start)-8:], "\xff\xff\xff\xff\xff\xff\xff\xff")

	k, v, err := n.getcas(start, 0)
	if err != nil {
		return Entry{}, err
	}

	return convertEntry(createDriverEntry(k, v, false)), nil
}

func decbytes(key []byte) {
	v := binary.BigEndian.Uint64(key[len(key)-8:])
	if v == 0 {
		panic("TODO")
	}
	v--
	binary.BigEndian.PutUint64(key[len(key)-8:], v)
}

func hasCommonPrefixTill0(a, b []byte) bool {
	idxa, idxb := bytes.IndexByte(a, 0), bytes.IndexByte(b, 0)
	if idxa > -1 && idxb > -1 {
		return bytes.Equal(a[:idxa], b[:idxb])
	}
	return false
}

func (n *Node) getcas(key []byte, depth int) ([]byte, []byte, error) {
	if depth > 10 {
		return nil, nil, ErrDeepCas
	}

	k, v, err := n.db.Get(key)
	if err != nil {
		return nil, nil, err
	}

	if hasCommonPrefixTill0(k, key) && len(k) > 16 {
		if bytes.Equal(v, deletionUUID) {
			return nil, nil, ErrNotFound
		}

		k0 := k
		if bytes.HasPrefix(v, casUUID) {
			if idx := bytes.Index(v[16:], casUUID); idx != -1 {
				oldValue, newValue := v[16:16+idx], v[16+idx+16:]
				decbytes(k)
				_, prevv, err := n.getcas(k, depth+1)
				if err != nil {
					// ErrNotFound -> not fully replicated, so just return it
					return nil, nil, err
				}

				if !bytes.Equal(oldValue, prevv) {
					return k0, prevv, nil
				}

				return k0, newValue, nil
			}
		}

		return k, v, nil
	}

	return nil, nil, ErrNotFound
}

func (n *Node) GetVersion(key string, ver int64) (Entry, error) {
	k, v, err := n.db.Get(n.combineKeyVer(key, ver))
	if err != nil {
		return Entry{}, err
	}
	if bytes.HasPrefix(k, []byte(key)) && len(k) > 16 {
		if !bytes.Equal(v, deletionUUID) {
			ts := int64(binary.BigEndian.Uint64(k[len(k)-16:]))
			if ts == ver {
				return convertEntry(createDriverEntry(k, v, false)), nil
			}
		}
	}
	return Entry{}, ErrNotFound
}
