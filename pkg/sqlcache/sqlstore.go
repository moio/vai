package cache

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"io"
	"k8s.io/client-go/tools/cache"
	"reflect"
)

// IOStore is a cache.Store that works uses some backing I/O, thus:
// 1) it has a Close() method and 2) List* methods may return errors
type IOStore interface {
	cache.Store
	io.Closer

	// SafeList returns a list of all the currently non-empty accumulators
	SafeList() ([]interface{}, error)

	// SafeListKeys returns a list of all the keys currently associated with non-empty accumulators
	SafeListKeys() ([]string, error)
}

// sqlStore stores information in a SQL database
type sqlStore struct {
	keyfunc cache.KeyFunc
	typ     reflect.Type

	db *sql.DB

	addStmt       *sql.Stmt
	getStmt       *sql.Stmt
	updateStmt    *sql.Stmt
	deleteStmt    *sql.Stmt
	listStmt      *sql.Stmt
	deleteAllStmt *sql.Stmt
	listKeysStmt  *sql.Stmt
}

func NewSQLStore(keyfunc cache.KeyFunc, typ reflect.Type) (IOStore, error) {
	db, err := sql.Open("sqlite3", "./sqlstore.sqlite")
	if err != nil {
		return nil, err
	}

	sqlStmt := `DROP TABLE IF EXISTS objects;
	CREATE TABLE objects (key VARCHAR(128) NOT NULL PRIMARY KEY, object BLOB);`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return nil, err
	}

	addStmt, err := db.Prepare("INSERT INTO objects(key, object) VALUES (?, ?)")
	if err != nil {
		return nil, err
	}

	getStmt, err := db.Prepare("SELECT object FROM objects WHERE key = ?")
	if err != nil {
		return nil, err
	}

	updateStmt, err := db.Prepare("UPDATE objects SET object = ? WHERE key = ?")
	if err != nil {
		return nil, err
	}

	deleteStmt, err := db.Prepare("DELETE FROM objects WHERE key = ?")
	if err != nil {
		return nil, err
	}

	listStmt, err := db.Prepare("SELECT object FROM objects")
	if err != nil {
		return nil, err
	}

	deleteAllStmt, err := db.Prepare("DELETE FROM objects")
	if err != nil {
		return nil, err
	}

	listKeysStmt, err := db.Prepare("SELECT key FROM objects")
	if err != nil {
		return nil, err
	}

	return &sqlStore{typ: typ, keyfunc: keyfunc, db: db, addStmt: addStmt, getStmt: getStmt, updateStmt: updateStmt, deleteStmt: deleteStmt, listStmt: listStmt, deleteAllStmt: deleteAllStmt, listKeysStmt: listKeysStmt}, nil
}

func (s *sqlStore) Close() error {
	return s.db.Close()
}

func (s *sqlStore) Add(obj interface{}) error {
	key, err := s.keyfunc(obj)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(obj)
	if err != nil {
		return err
	}

	_, err = s.addStmt.Exec(key, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (s *sqlStore) Update(obj interface{}) error {
	key, err := s.keyfunc(obj)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(obj)
	if err != nil {
		return err
	}

	_, err = s.updateStmt.Exec(buf.Bytes(), key)
	if err != nil {
		return err
	}

	return nil
}

func (s *sqlStore) Delete(obj interface{}) error {
	key, err := s.keyfunc(obj)
	if err != nil {
		return err
	}

	_, err = s.deleteStmt.Exec(key)
	return err
}

func (s *sqlStore) SafeList() ([]interface{}, error) {
	rows, err := s.listStmt.Query()
	if err != nil {
		return nil, err
	}

	var result []any
	for rows.Next() {
		var buf sql.RawBytes
		err = rows.Scan(&buf)
		if err != nil {
			return closeOnError(rows, err)
		}

		dec := gob.NewDecoder(bytes.NewReader(buf))
		singleResult := reflect.New(s.typ)
		err = dec.DecodeValue(singleResult)
		if err != nil {
			return closeOnError(rows, err)
		}
		result = append(result, singleResult.Elem().Interface())
	}
	err = rows.Err()
	if err != nil {
		if err != nil {
			return closeOnError(rows, err)
		}
		return nil, err
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func closeOnError(rows *sql.Rows, err error) ([]interface{}, error) {
	ce := rows.Close()
	if ce != nil {
		return nil, errors.Wrap(ce, "while handling "+err.Error())
	}

	return nil, err
}

func (s *sqlStore) List() []interface{} {
	result, err := s.SafeList()
	if err != nil {
		fmt.Printf("Error in sqlStore.List %v", err)
	}

	return result
}

func (s *sqlStore) SafeListKeys() ([]string, error) {
	rows, err := s.listKeysStmt.Query()
	if err != nil {
		return nil, err
	}

	var result []string
	for rows.Next() {
		var key string
		err = rows.Scan(&key)
		if err != nil {
			ce := rows.Close()
			if ce != nil {
				return nil, errors.Wrap(ce, "while handling "+err.Error())
			}
		}

		result = append(result, key)
	}
	err = rows.Err()
	if err != nil {
		ce := rows.Close()
		if ce != nil {
			return nil, errors.Wrap(ce, "while handling "+err.Error())
		}
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *sqlStore) ListKeys() []string {
	result, err := s.SafeListKeys()
	if err != nil {
		fmt.Printf("Error in sqlStore.ListKeys %v", err)
	}

	return result
}

func (s *sqlStore) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := s.keyfunc(obj)
	if err != nil {
		return nil, false, err
	}

	return s.GetByKey(key)
}

func (s *sqlStore) GetByKey(key string) (item interface{}, exists bool, err error) {
	var buf []byte
	err = s.getStmt.QueryRow(key).Scan(&buf)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	dec := gob.NewDecoder(bytes.NewReader(buf))
	result := reflect.New(s.typ)
	err = dec.DecodeValue(result)
	if err != nil {
		return nil, false, err
	}

	return result.Elem().Interface(), true, nil
}

func (s *sqlStore) Replace(objects []interface{}, _ string) error {
	_, err := s.deleteAllStmt.Exec()
	if err != nil {
		return err
	}

	for _, object := range objects {
		err := s.Add(object)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *sqlStore) Resync() error {
	return nil
}
