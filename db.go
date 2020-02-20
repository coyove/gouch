package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/coyove/gouch/clock"
	"github.com/coyove/gouch/driver"
	"github.com/coyove/gouch/filelog"
)

var (
	ErrNotFound = fmt.Errorf("key not found")
	ErrDeepCas  = fmt.Errorf("deep CAS operations")
)

var (
	internalNodeName    = []byte("_internal_node_name")
	internalNodeNameLen = 8
)

type KeyValueDatabase interface {
	// Get finds the requested key and its value, if not found, the biggest key before
	// the requested key will and should be returned
	// If no keys can be returned, callee should return (nil, nil, nil)
	Get(key []byte) ([]byte, []byte, error)

	// Put puts the key-value pairs into the database,
	// All key-value pairs should all be stored successfully or not
	Put(keyvalues ...[]byte) error

	// Delete deletes keys from the database
	Delete(keys ...[]byte) error

	// Seek seeks the requested key and use the callback function to determine
	// whether it should go forward (next key), backward (prev key) or quit
	Seek(startKey []byte, cb func(k, v []byte) int) error

	// Close closes the database
	Close() error

	Info() map[string]interface{}
}

type Node struct {
	db           KeyValueDatabase
	log          *filelog.Handler
	path         string
	driver       string
	Name         string
	internalName []byte
	startAt      int64
	writelocks   [65536]sync.Mutex
	friends      struct {
		contacts map[string]string
		states   map[string]*repState
		sync.Mutex
	}
}

func NewNode(name, driverName string, path string, friends string) (*Node, error) {
	err := os.MkdirAll(path, 0777)
	if err != nil {
		return nil, err
	}

	n := &Node{
		Name:    name,
		path:    path,
		driver:  driverName,
		startAt: clock.Timestamp(),
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

	n.readRepState(friends)
	for _, f := range n.friends.states {
		go n.replicationWorker(f)
	}

	return n, nil
}

func (n *Node) Put(key string, v []byte) (int64, error) {
	if strings.Contains(key, "\x00") {
		return 0, fmt.Errorf("invalid key: contains '0x00'")
	}

	if len(key) == 0 {
		return 0, fmt.Errorf("invalid key: empty")
	}

	keybuf := []byte(key)

	// Handle concurrent writes here, for the sake of CAS operations
	mu := &n.writelocks[crc32.ChecksumIEEE(keybuf)%uint32(len(n.writelocks))]
	mu.Lock()
	defer mu.Unlock()

	ts, err := n.log.GetTimestampForKey(keybuf)
	if err != nil {
		return 0, err
	}
	return ts, n.db.Put(n.combineKeyVer(key, ts), v)
}

func (n *Node) CasPut(key string, oldValue, newValue []byte) (Entry, error) {
	if strings.Contains(key, "\x00") {
		return Entry{}, fmt.Errorf("invalid key: contains '0x00'")
	}

	if len(key) == 0 {
		return Entry{}, fmt.Errorf("invalid key: empty")
	}

	keybuf := []byte(key)

	mu := &n.writelocks[crc32.ChecksumIEEE(keybuf)%uint32(len(n.writelocks))]
	mu.Lock()
	defer mu.Unlock()

	v, err := n.Get(key)
	if err != nil {
		return Entry{}, err
	}

	if !bytes.Equal([]byte(v.Value), oldValue) {
		return v, nil
	}

	ts, err := n.log.GetTimestampForKey(keybuf)
	if err != nil {
		return Entry{}, err
	}

	cas := append([]byte{}, casUUID...)
	cas = append(cas, oldValue...)
	cas = append(cas, casUUID...)
	cas = append(cas, newValue...)

	e := n.combineKeyVer(key, ts)
	if err := n.db.Put(e, cas); err != nil {
		return Entry{}, err
	}

	return createEntry(e, cas, false), nil
}

func (n *Node) GetAllVersions(key string, startTimestamp int64, count int, keyOnly bool) (kvs []Entry, next int64, err error) {
	_, upper := getKeyBounds(key, startTimestamp)
	if startTimestamp != 0 {
		binary.BigEndian.PutUint64(upper[len(upper)-16:], uint64(startTimestamp))
	}

	prefix, skipFirst := []byte(key), false
	err = n.db.Seek(upper, func(k, v []byte) int {
		if !skipFirst {
			skipFirst = true // Skip the first value as it's > upper
			return driver.SeekPrev
		}
		if bytes.Equal(k, internalNodeName) {
			return driver.SeekPrev
		}
		if bytes.HasPrefix(k, prefix) {
			kvs = append(kvs, createEntry(k, v, keyOnly))
			if len(kvs) == count+1 {
				next = kvs[count].Ver
				kvs = kvs[:count]
				return driver.SeekAbort
			}
			return driver.SeekPrev
		}
		return driver.SeekAbort
	})
	if err != nil {
		return nil, 0, err
	}
	return
}

func (n *Node) Delete(key string) (int64, error) {
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

func (n *Node) Whois(internalName string) string {
	for _, v := range n.friends.states {
		if v.NodeInternalName == internalName {
			return v.NodeName
		}
	}
	return ""
}
