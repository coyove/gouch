package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	// All key-value pairs should all be stored successfully or not
	Put(keyvalues ...driver.Entry) error

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
	db               KeyValueDatabase
	log              *filelog.Handler
	path             string
	driver           string
	Name             string
	internalName     []byte
	startAt          time.Time
	startAtTimestamp int64
	friends          struct {
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
		Name:             name,
		path:             path,
		driver:           driverName,
		startAt:          time.Now(),
		startAtTimestamp: clock.Timestamp(),
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
		if err := n.db.Put(driver.Entry{Key: internalNodeName, Value: name}); err != nil {
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

func (n *Node) Get(key string) ([]byte, int64, error) {
	start := n.combineKeyVer(key, clock.Timestamp())
	copy(start[len(start)-8:], "\xff\xff\xff\xff\xff\xff\xff\xff")
	k, v, err := n.db.Get(start)
	if err != nil {
		return nil, 0, err
	}
	if bytes.HasPrefix(k, []byte(key)) && len(k) > 16 {
		if !bytes.Equal(v, deletionUUID) {
			ts := int64(binary.BigEndian.Uint64(k[len(k)-16:]))
			return v, ts, nil
		}
	}
	return nil, 0, ErrNotFound
}

func (n *Node) GetVersion(key string, ver int64) ([]byte, error) {
	k, v, err := n.db.Get(n.combineKeyVer(key, ver))
	if err != nil {
		return nil, err
	}
	if bytes.HasPrefix(k, []byte(key)) && len(k) > 16 {
		if !bytes.Equal(v, deletionUUID) {
			ts := int64(binary.BigEndian.Uint64(k[len(k)-16:]))
			if ts == ver {
				return v, nil
			}
		}
	}
	return nil, ErrNotFound
}

func (n *Node) Put(key string, v []byte) (int64, error) {
	if strings.Contains(key, "\x00") {
		return 0, fmt.Errorf("invalid key: contains '0x00'")
	}

	if len(key) == 0 {
		return 0, fmt.Errorf("invalid key: empty")
	}

	ts, err := n.log.GetTimestampForKey([]byte(key))
	if err != nil {
		return 0, err
	}
	return ts, n.db.Put(driver.Entry{Key: n.combineKeyVer(key, ts), Value: v})
}

func (n *Node) GetAllVersions(key string, startTimestamp int64, keyOnly bool) (kvs []Entry, err error) {
	data := []driver.Entry{}
	next, _ := getKeyBounds(key, startTimestamp)
	prefix := []byte(key)

	err = n.db.Seek(next, func(k, v []byte) int {
		if bytes.Equal(k, internalNodeName) {
			return driver.SeekNext
		}
		if bytes.HasPrefix(k, prefix) {
			l := len(v)
			if keyOnly {
				v = nil
			}
			data = append(data, driver.Entry{
				Key:      append([]byte{}, k...),
				Value:    append([]byte{}, v...),
				ValueLen: int64(l),
			})
			return driver.SeekNext
		}
		return driver.SeekAbort
	})
	if err != nil {
		return nil, err
	}

	x, now := []Entry{}, clock.Timestamp()
	for i := len(data) - 1; i >= 0; i-- {
		p := data[i]
		ts := p.Version()
		x = append(x, Entry{
			Value:    jsonBytes(p.Value),
			Ver:      ts,
			ValueLen: p.ValueLen,
			Unix:     time.Unix(clock.UnixSecFromTimestamp(ts), 0),
			Node:     p.Node(),
			Future:   ts > now,
			Deleted:  bytes.Equal(p.Value, deletionUUID),
		})
	}
	return x, nil
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
