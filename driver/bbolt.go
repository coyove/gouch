package driver

import (
	"bytes"
	"fmt"

	"go.etcd.io/bbolt"
)

var bkd = []byte("default")

type bboltDatabase struct {
	db *bbolt.DB
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
		db: db,
	}, nil
}

func (db *bboltDatabase) Close() error {
	return db.db.Close()
}

func (db *bboltDatabase) Put(kvs ...[]byte) error {
	if len(kvs)%2 != 0 {
		panic(kvs)
	}

	return db.db.Update(func(tx *bbolt.Tx) error {
		for i := 0; i < len(kvs); i += 2 {
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
		k = sk
		return nil
	})
	return k, v, err
}

func (db *bboltDatabase) SeekN(k []byte, n int) ([][2][]byte, []byte, error) {
	var res [][2][]byte
	var next []byte

	err := db.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bkd).Cursor()

		k, v := c.Seek(k)
		if len(k) > 0 {
			res = append(res, [2][]byte{k, append([]byte{}, v...)})
		}

		for i := 0; i < n-1; i++ {
			k, v := c.Next()
			if len(k) > 0 {
				res = append(res, [2][]byte{k, append([]byte{}, v...)})
			} else {
				break
			}
		}

		next, _ = c.Next()

		return nil
	})
	return res, next, err
}
