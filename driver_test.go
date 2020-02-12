package gouch

import (
	"testing"
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
	n, _ := NewNode("bbolt", "testlog")
	t.Log(n.Get("aaa"))
	n.Put("aaa", []byte{})
	t.Log(n.Get("aaa"))
	n.Put("aaa", []byte("haha"))
	t.Log(n.Get("aaa"))
	n.Delete("aaa")
	t.Log(n.Get("aaa"))
}
