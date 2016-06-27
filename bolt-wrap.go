package bolt_wrap

import (
	"github.com/boltdb/bolt"
	"strconv"
	"net/http"
)

type DB struct {
	db *bolt.DB
}

func New(path string) DB {
	var err error
	d := DB{}
	d.db, err = bolt.Open(path, 0655, bolt.DefaultOptions)
	ErrorHandler(err)
	return d
}

func (db *DB) Set(bucket, key string, value []byte) error {
	tx, err := db.db.Begin(true)
	defer tx.Rollback()
	if err != nil {
		ErrorHandler(err)
		return err
	}
	b := tx.Bucket([]byte(bucket))
	if b == nil {
		b, err = tx.CreateBucket([]byte(bucket))
		ErrorHandler(err)
	}
	err = b.Put([]byte(key), value)
	if err != nil {
		ErrorHandler(err)
		return err
	}
	err = tx.Commit()
	ErrorHandler(err)
	return err

}

func (db *DB) Get(bucket, key string) []byte {
	tx, err := db.db.Begin(true)
	ErrorHandler(err)
	defer tx.Rollback()
	b := tx.Bucket([]byte(bucket))
	if b == nil {
		b, err = tx.CreateBucket([]byte(bucket))
		ErrorHandler(err)
	}
	result := b.Get([]byte(key))
	err = tx.Commit()
	ErrorHandler(err)
	return result
}

func (db *DB) Delete(bucket, key string) error {
	tx, err := db.db.Begin(true)
	defer tx.Rollback()
	if err != nil {
		ErrorHandler(err)
		return err
	}
	b := tx.Bucket([]byte(bucket))
	if b == nil {
		b, err = tx.CreateBucket([]byte(bucket))
		ErrorHandler(err)
	}
	err = b.Delete([]byte(key))
	if err != nil {
		ErrorHandler(err)
		return err
	}
	err = tx.Commit()
	ErrorHandler(err)
	return err
}

func (db *DB) GetBackUp(w http.ResponseWriter, fileName string) {
	err := db.db.View(func(tx *bolt.Tx) error {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="` + fileName + `"`)
		w.Header().Set("Content-Length", strconv.Itoa(int(tx.Size())))
		_, err := tx.WriteTo(w)
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
