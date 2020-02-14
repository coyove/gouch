package gouch

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/coyove/common/clock"
	"github.com/coyove/gouch/driver"
	"github.com/coyove/gouch/filelog"
)

var ErrNotFound = fmt.Errorf("not found")

var (
	nodeName     = []byte("node_name")
	deletionUUID = []byte{0x91, 0xee, 0x48, 0xda, 0x52, 0x75, 0x4e, 0xc7, 0xa5, 0x76, 0xcb, 0x80, 0xad, 0x1c, 0x12, 0x03}
)

type KeyValueDatabase interface {
	// Get finds the requested key and its value, if not found, the biggest key before
	// the requested key will and should be returned
	// If no keys can be returned, callee should return (nil, nil, nil)
	Get(key []byte) ([]byte, []byte, error)

	// Put puts the key-value pairs into the database,
	// kvs should be arranged in a form like: key1, value1, key2, value2, ...
	// Caller shall ensure: len(kvs) % 2 == 0
	// All key-value pairs should all be stored successfully or not
	Put(keyvalues ...[]byte) error

	// Delete deletes keys from the database
	Delete(keys ...[]byte) error

	// Close closes the database
	Close() error
}

type Node struct {
	db           KeyValueDatabase
	log          *filelog.Handler
	path         string
	internalName []byte
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
		n.db.Close()
		return nil, err
	}

	k, v, err := n.db.Get(nodeName)
	if err != nil {
		n.db.Close()
		n.log.Close()
		return nil, err
	}

	if !bytes.Equal(k, nodeName) || len(v) != 8 {
		name := make([]byte, 8)
		rand.Read(name)
		if err := n.db.Put(nodeName, name); err != nil {
			n.db.Close()
			n.log.Close()
			return nil, err
		}
		n.internalName = name
	} else {
		n.internalName = v
	}

	return n, nil
}

func (n *Node) Get(key string) ([]byte, error) {
	k, v, err := n.db.Get(n.convertVersionsToKeys(key, clock.Timestamp())[0])
	if err != nil {
		return nil, err
	}
	if bytes.HasPrefix(k, []byte(key)) {
		if bytes.Equal(v, deletionUUID) {
			return nil, ErrNotFound
		}
		return v, nil
	}
	return nil, ErrNotFound
}

func (n *Node) Put(key string, v []byte) error {
	ts, err := n.log.GetTimestampForKey([]byte(key))
	if err != nil {
		return err
	}
	return n.db.Put(n.convertVersionsToKeys(key, ts)[0], v)
}

// func (n *Node) GetAllVersions(key string, startTimestamp int64) ([]int64, error) {
// 	var vers []int64
//
// 	next := []byte(key)
// 	if startTimestamp != 0 {
// 		next = n.convertVersionsToKeys(key, startTimestamp)[0]
// 	}
//
// MAIN:
// 	for len(next) > 0 {
// 		var res [][2][]byte
// 		var err error
//
// 		res, next, err = n.db.SeekN(next, 10)
// 		if err != nil {
// 			return nil, err
// 		}
//
// 		for _, kv := range res {
// 			// log.Println(kv[0], key)
// 			if bytes.HasPrefix(kv[0], []byte(key)) {
// 				tmp := bytes.TrimPrefix(kv[0], []byte(key))
// 				ver, err := strconv.ParseInt(string(tmp), 16, 64)
// 				if err != nil {
// 					return nil, fmt.Errorf("error parsing version of %q, master: %s", kv[0], key)
// 				}
// 				vers = append(vers, ver)
// 			} else {
// 				break MAIN
// 			}
// 		}
// 	}
//
// 	return vers, nil
// }

// func splitRealKey(key []byte) (string, error) {
// 	if len(key) < 16 {
// 		return "", fmt.Errorf("invalid key: too short")
// 	}
// 	return string(key[:len(key)-16]), nil
// }

func (n *Node) convertVersionsToKeys(key string, vers ...int64) [][]byte {
	var keys [][]byte
	for _, v := range vers {
		tmp := bytes.Buffer{}

		// Format: key + 8b (timestamp) + 8b (internal name)
		tmp.WriteString(key)
		binary.Write(&tmp, binary.BigEndian, v)
		tmp.Write(n.internalName)

		keys = append(keys, tmp.Bytes())
	}
	return keys
}

func (n *Node) Delete(key string) error {
	return n.Put(key, deletionUUID)
}
