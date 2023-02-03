package sqlcache

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"k8s.io/client-go/tools/cache"
	"reflect"
)

type VersionFunc func(obj interface{}) (int, error)

type VersionedStore struct {
	typ         reflect.Type
	db          *sql.DB
	keyFunc     cache.KeyFunc
	versionFunc VersionFunc

	addStmt     *sql.Stmt
	delStmt     *sql.Stmt
	listStmt    *sql.Stmt
	listKeyStmt *sql.Stmt
	getStmt     *sql.Stmt
	delAllStmt  *sql.Stmt
}

// NewVersionedStore creates a SQLite-backed cache.Store for the type typ
func NewVersionedStore(typ reflect.Type, keyFunc cache.KeyFunc, versionFunc VersionFunc) (*VersionedStore, error) {
	stmts := []string{
		`CREATE TABLE objects (
			key VARCHAR NOT NULL,
			version INTEGER,
			object BLOB,
			deleted INTEGER,
			PRIMARY KEY (key, version)
        )`,
		`CREATE VIEW latest_objects AS
			SELECT o1.*
				FROM objects o1
				WHERE o1.deleted = 0
					AND o1.version >= (SELECT MAX(o2.version) FROM objects o2 WHERE o2.key = o1.key)
		`,
	}

	db, err := initTempSQLiteDB(stmts)
	if err != nil {
		return nil, err
	}

	addStmt, err := db.Prepare("INSERT INTO objects(key, version, object, deleted) VALUES (?, ?, ?, 0) ON CONFLICT DO UPDATE SET object = excluded.object, deleted = 0")
	if err != nil {
		return nil, err
	}

	delStmt, err := db.Prepare(`UPDATE objects SET deleted = 1 WHERE key = ? AND version = (
    	SELECT MAX(version)
    		FROM objects o2
    		WHERE objects.key = o2.key 
	)`)
	if err != nil {
		return nil, err
	}

	listStmt, err := db.Prepare(`SELECT object FROM latest_objects`)
	if err != nil {
		return nil, err
	}

	listKeyStmt, err := db.Prepare(`SELECT key FROM latest_objects`)
	if err != nil {
		return nil, err
	}

	getStmt, err := db.Prepare(`SELECT object FROM latest_objects WHERE key = ?`)
	if err != nil {
		return nil, err
	}

	delAllStmt, err := db.Prepare(`UPDATE objects SET deleted = 1 WHERE version = (
    	SELECT MAX(version)
    		FROM objects o2
    		WHERE objects.key = o2.key 
	)`)
	if err != nil {
		return nil, err
	}

	return &VersionedStore{typ: typ, db: db, keyFunc: keyFunc, versionFunc: versionFunc, addStmt: addStmt, delStmt: delStmt, listStmt: listStmt, listKeyStmt: listKeyStmt, getStmt: getStmt, delAllStmt: delAllStmt}, nil
}

/* Satisfy IOStore */

// Add saves an obj, or updates it if it exists in this store
func (s *VersionedStore) Add(obj interface{}) error {
	key, err := s.keyFunc(obj)
	if err != nil {
		return err
	}

	version, err := s.versionFunc(obj)
	if err != nil {
		return err
	}

	_, err = s.addStmt.Exec(key, version, toBytes(obj))
	return err
}

// Update saves an obj, or updates it if it exists in this store
func (s *VersionedStore) Update(obj interface{}) error {
	return s.Add(obj)
}

// Delete deletes the given object, if it exists in this store
func (s *VersionedStore) Delete(obj interface{}) error {
	key, err := s.keyFunc(obj)
	if err != nil {
		return err
	}

	_, err = s.delStmt.Exec(key)
	return err
}

// List wraps SafeList and panics in case of I/O errors
func (s *VersionedStore) List() []interface{} {
	result, err := s.SafeList()
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in VersionedStore.SafeList"))
	}
	return result
}

// SafeList returns a list of all the currently known objects
func (s *VersionedStore) SafeList() ([]interface{}, error) {
	return queryObjects(s.listStmt, s.typ)
}

// ListKeys wraps SafeListKeys and panics in case of I/O errors
func (s *VersionedStore) ListKeys() []string {
	result, err := s.SafeListKeys()
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in VersionedStore.SafeListKeys"))
	}
	return result
}

// SafeListKeys returns a list of all the keys currently in this store
func (s *VersionedStore) SafeListKeys() ([]string, error) {
	return queryStrings(s.listKeyStmt)
}

// Get returns the object with the same key as obj
func (s *VersionedStore) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := s.keyFunc(obj)
	if err != nil {
		return nil, false, err
	}

	return s.GetByKey(key)
}

// GetByKey returns the object associated with the given object's key
func (s *VersionedStore) GetByKey(key string) (item interface{}, exists bool, err error) {
	result, err := queryObjects(s.getStmt, s.typ, key)
	if err != nil {
		return nil, false, err
	}

	if len(result) == 0 {
		return nil, false, nil
	}

	return result[0], true, nil
}

// Replace will delete the contents of the store, using instead the given list
func (s *VersionedStore) Replace(objects []interface{}, _ string) error {
	_, err := s.delAllStmt.Exec()
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

// Resync is a no-op and is deprecated
func (s *VersionedStore) Resync() error {
	return nil
}

// Close closes the database and prevents new queries from starting
func (s *VersionedStore) Close() error {
	return s.db.Close()
}
