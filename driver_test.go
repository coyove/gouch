package main

import (
	"strconv"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
)

func TestDriver(t *testing.T) {
	// var x KeyValueDatabase
	// x, _ = driver.NewBBolt("testlog")
	// x.Put([]byte("hello"), []byte("zzz"))
	// x.Put([]byte("hello2"), []byte("zzz"))
	// x.Put([]byte("hello4"), []byte("zzz"))
	// t.Log(x.Seek([]byte("hello3"), 0))
	// t.Log(x.Seek([]byte("hello3"), -1))
	// t.Log(x.Seek([]byte("hello3"), 1))
}

func TestNode(t *testing.T) {
	n, _ := NewNode("test", "bbolt", "testlog")
	t.Log(n.Get("aaa"))
	n.Put("aaa", []byte{})
	t.Log(n.Get("aaa"))
	n.Put("aaa", []byte("haha"))
	n.Put("aaa1", []byte("one"+strconv.Itoa(int(time.Now().Unix()))))
	t.Log(n.Get("aaa"))
	t.Log(n.Get("aaa1"))

	// t.Log(n.GetChangedKeysSince(0, 100))
	res, _ := n.GetAllVersions("aaa1", 0)
	t.Log(proto.Marshal(res))
}
