package driver

import (
	"bytes"
	"fmt"
	"os"

	"go.etcd.io/bbolt"
)

var bkd = []byte("default")

type bboltDatabase struct {
	db   *bbolt.DB
	path string
}

func NewBBolt(path string) (*bboltDatabase, error) {
	db, err := bbolt.Open(path, 0777, nil)
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bkd)
		return err
	}); err != nil {
		return nil, err
	}

	return &bboltDatabase{
		db:   db,
		path: path,
	}, nil
}

func (db *bboltDatabase) Close() error {
	return db.db.Close()
}

func (db *bboltDatabase) Put(kvs ...[]byte) error {
	if len(kvs)%2 != 0 {
		panic("odd")
	}

	if len(kvs) == 0 {
		return nil
	}

	return db.db.Update(func(tx *bbolt.Tx) error {
		for i := 0; i < len(kvs); i += 2 {
			if len(kvs[i]) == 0 {
				continue
			}
			if err := tx.Bucket(bkd).Put(kvs[i], kvs[i+1]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *bboltDatabase) Delete(keys ...[]byte) error {
	return db.db.Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(bkd)
		for _, k := range keys {
			if err := bk.Delete(k); err != nil {
				return fmt.Errorf("error deleting %q: %v", k, err)
			}
		}
		return nil
	})
}

func (db *bboltDatabase) Get(k []byte) ([]byte, []byte, error) {
	var v []byte
	err := db.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bkd).Cursor()

		sk, sv := c.Seek(k)

		if !bytes.Equal(sk, k) {
			sk, sv = c.Prev()
		}

		v = append([]byte{}, sv...)
		k = append([]byte{}, sk...)
		return nil
	})
	return k, v, err
}

func (db *bboltDatabase) Seek(startKey []byte, cb func(k, v []byte) int) error {
	return db.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bkd).Cursor()

		k, v := c.Seek(startKey)
		if len(k) == 0 {
			return nil
		}

		for todo := cb(k, v); ; todo = cb(k, v) {
			switch todo {
			case SeekPrev:
				k, v = c.Prev()
			case SeekNext:
				k, v = c.Next()
			default:
				return nil
			}

			if len(k) == 0 {
				return nil
			}
		}
	})
}

func (db *bboltDatabase) Info() map[string]interface{} {
	m := map[string]interface{}{}
	m["error"] = db.db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(bkd).Stats()
		m["branch_page_n"] = bk.BranchPageN
		m["leaf_page_n"] = bk.LeafPageN
		m["key_n"] = bk.KeyN
		m["depth"] = bk.Depth
		m["branch_alloc"] = bk.BranchAlloc
		m["branch_inuse"] = bk.BranchInuse
		m["leaf_alloc"] = bk.LeafAlloc
		m["leaf_inuse"] = bk.LeafInuse
		return nil
	})
	if fi, _ := os.Stat(db.path); fi != nil {
		m["db_size"] = fi.Size()
		m["db_size_human"] = fmt.Sprintf("%.3fG", float64(fi.Size())/1024/1024/1024)
	}
	return m
}
