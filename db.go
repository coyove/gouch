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
	Delete(...[]byte) error
}

type Node struct {
	db  KeyValueDatabase
	log *filelog.Handler
}

func NewNode(driverName string, path string) (*Node, error) {
	err := os.MkdirAll(path, 0777)
	if err != nil {
		return nil, err
	}

	n := &Node{}

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
	k, v, err := n.db.Seek([]byte(key+strconv.FormatInt(clock.Timestamp(), 16)), -1)
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
	return n.db.Put([]byte(key+strconv.FormatInt(ts, 16)), v)
}

func (n *Node) GetAllVersions(key string, startTimestamp int64) ([]int64, error) {
	var vers []int64

	next := []byte(key)
	if startTimestamp != 0 {
		next = append(next, []byte(strconv.FormatInt(startTimestamp, 16))...)
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

func (n *Node) Delete(key string) error {
	var keys [][]byte

	vers, err := n.GetAllVersions(key, 0)
	if err != nil {
		return err
	}

	if len(vers) == 0 {
		return nil
	}

	for _, v := range vers {
		keys = append(keys, []byte(key+strconv.FormatInt(v, 16)))
	}

	return n.db.Delete(keys...)
}
