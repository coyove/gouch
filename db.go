package gouch

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/coyove/gouch/clock"
	"github.com/coyove/gouch/driver"
	"github.com/coyove/gouch/filelog"
)

var ErrNotFound = fmt.Errorf("key not found")

var (
	internalNodeName    = []byte("_internal_node_name")
	internalNodeNameLen = 8
	deletionUUID        = []byte{0x91, 0xee, 0x48, 0xda, 0x52, 0x75, 0x4e, 0xc7, 0xa5, 0x76, 0xcb, 0x80, 0xad, 0x1c, 0x12, 0x03}
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

	// Seek seeks the requested key and its followings, returns at most n results and a next cursor
	// which indicates the start of the next round of seeking
	Seek(key []byte, n int) ([][2][]byte, []byte, error)

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

	k, v, err := n.db.Get(internalNodeName)
	if err != nil {
		n.db.Close()
		n.log.Close()
		return nil, err
	}

	if !bytes.Equal(k, internalNodeName) || len(v) != internalNodeNameLen {
		name := make([]byte, internalNodeNameLen)
		rand.Read(name)
		if err := n.db.Put(internalNodeName, name); err != nil {
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
	k, v, err := n.db.Get(n.combineKeyVer(key, clock.Timestamp()))
	if err != nil {
		return nil, err
	}
	if bytes.HasPrefix(k, []byte(key)) {
		if !bytes.Equal(v, deletionUUID) {
			return v, nil
		}
	}
	return nil, ErrNotFound
}

func (n *Node) Put(key string, v []byte) error {
	if strings.Contains(key, "\x00") {
		return fmt.Errorf("invalid key: contains '\x00'")
	}

	ts, err := n.log.GetTimestampForKey([]byte(key))
	if err != nil {
		return err
	}
	return n.db.Put(n.combineKeyVer(key, ts), v)
}

func (n *Node) GetAllVersions(key string, startTimestamp int64) (kvs []Pair, err error) {
	next, _ := getKeyBounds(key, startTimestamp)
	prefix := append([]byte(key), 0)

	var res [][2][]byte
MAIN:
	for len(next) > 0 {
		log.Println(next)
		res, next, err = n.db.Seek(next, 10)
		if err != nil {
			return nil, err
		}

		for _, kv := range res {
			if bytes.Equal(kv[0], internalNodeName) {
				continue
			}
			if bytes.HasPrefix(kv[0], prefix) {
				kvs = append(kvs, Pair{kv[0], kv[1]})
			} else {
				break MAIN
			}
		}
	}
	return
}

func (n *Node) Delete(key string) error {
	return n.Put(key, deletionUUID)
}

func (n *Node) Purge(keys ...[]byte) error {
	return n.db.Delete(keys...)
}

func (n *Node) InternalName() string {
	return base64.URLEncoding.EncodeToString(n.internalName)[:10]
}

func (n *Node) combineKeyVer(key string, v int64) []byte {
	tmp := bytes.Buffer{}

	// Format: key + 8b (timestamp) + 8b (internal name)
	// The MSB of timestamp is 0x00, serving as the delimeter
	tmp.WriteString(key)
	binary.Write(&tmp, binary.BigEndian, v)
	tmp.Write(n.internalName)
	return tmp.Bytes()
}
