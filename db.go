package gouch

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/coyove/common/clock"
	"github.com/coyove/gouch/driver"
	"github.com/coyove/gouch/filelog"
)

var ErrNotFound = fmt.Errorf("not found")

type KeyValueDatabase interface {
	Seek([]byte, int) ([]byte, []byte, error)
	SeekN([]byte, int) ([][2][]byte, []byte, error)
	Put(...[]byte) error
	PutOrder(...[]byte) error
	Delete(...[]byte) error
}

type Node struct {
	db   KeyValueDatabase
	log  *filelog.Handler
	path string
}

func NewNode(driverName string, path string) (*Node, error) {
	err := os.MkdirAll(path, 0777)
	if err != nil {
		return nil, err
	}

	n := &Node{
		path: path,
	}

	switch driverName {
	case "bbolt", "bolt":
		n.db, err = driver.NewBBolt(filepath.Join(path, "gouch.db"))
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown driver: %v", driverName)
	}

	n.log, err = filelog.Open(filepath.Join(path, "gouch.log"))
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (n *Node) Get(key string) ([]byte, error) {
	k, v, err := n.db.Seek(convertVersionsToKeys(key, clock.Timestamp())[0], -1)
	if bytes.HasPrefix(k, []byte(key)) {
		return v, err
	}
	return nil, ErrNotFound
}

func (n *Node) Put(key string, v []byte) error {
	ts, err := n.log.GetTimestampForKey([]byte(key))
	if err != nil {
		return err
	}
	return n.db.Put(convertVersionsToKeys(key, ts)[0], v)
}

func (n *Node) GetAllVersions(key string, startTimestamp int64) ([]int64, error) {
	var vers []int64

	next := []byte(key)
	if startTimestamp != 0 {
		next = convertVersionsToKeys(key, startTimestamp)[0]
	}

MAIN:
	for len(next) > 0 {
		var res [][2][]byte
		var err error

		res, next, err = n.db.SeekN(next, 10)
		if err != nil {
			return nil, err
		}

		for _, kv := range res {
			// log.Println(kv[0], key)
			if bytes.HasPrefix(kv[0], []byte(key)) {
				tmp := bytes.TrimPrefix(kv[0], []byte(key))
				ver, err := strconv.ParseInt(string(tmp), 16, 64)
				if err != nil {
					return nil, fmt.Errorf("error parsing version of %q, master: %s", kv[0], key)
				}
				vers = append(vers, ver)
			} else {
				break MAIN
			}
		}
	}

	return vers, nil
}

func splitRealKey(key []byte) ([]byte, error) {
	idx := bytes.LastIndexByte(key, '/')
	if idx == -1 {
		return nil, fmt.Errorf("invalid key, no version delimeter")
	}
	return key[:idx], nil
}

func convertVersionsToKeys(key string, vers ...int64) [][]byte {
	var keys [][]byte
	for _, v := range vers {
		keys = append(keys, []byte(key+"/"+strconv.FormatInt(v, 16)))
	}
	return keys
}

func (n *Node) Delete(key string) error {
	vers, err := n.GetAllVersions(key, 0)
	if err != nil {
		return err
	}

	if len(vers) == 0 {
		return nil
	}

	return n.db.Delete(convertVersionsToKeys(key, vers...)...)
}

// func (n *Node) DeleteOutdatedVersions(key string) error {
// 	vers, err := n.GetAllVersions(key, 0)
// 	if err != nil {
// 		return err
// 	}
//
// 	if len(vers) <= 1 {
// 		return nil
// 	}
//
// 	ts := clock.Timestamp()
// 	for i := range vers {
// 		if vers
// 	}
//
// 	return n.db.Delete(convertVersionsToKeys(key, vers...)...)
// }
