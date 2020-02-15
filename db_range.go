package main

import "bytes"

func (n *Node) Range(key string, count int, keyOnly bool) (kvs []Pair, err error) {
	kvs = []Pair{}
	next, _ := getKeyBounds(key, 0)
	prefix := append([]byte(key), 0)

	var res [][2][]byte
	var lastKey []byte

	for len(next) > 0 {
		res, next, err = n.db.Seek(next, 10, true)
		if err != nil {
			return nil, err
		}

		for _, kv := range res {
			if bytes.Equal(kv[0], internalNodeName) {
				continue
			}
			if bytes.HasPrefix(kv[0], prefix) {
				kvs.Data = append(kvs.Data, Pair{kv[0], kv[1]})
			}
		}
	}

	return
}
