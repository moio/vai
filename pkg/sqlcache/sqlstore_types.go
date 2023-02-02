package sqlcache

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"io"
	"k8s.io/client-go/tools/cache"
	"reflect"
	"strings"
)

// IOStore is a cache.Store that uses some backing I/O, thus:
// 1) it has a Close() method
// 2) List* methods may panic on I/O errors. Safe* (error-returning) variants are added
type IOStore interface {
	cache.Store
	io.Closer

	// SafeList returns a list of all the currently non-empty accumulators
	SafeList() ([]interface{}, error)

	// SafeListKeys returns a list of all the keys currently associated with non-empty accumulators
	SafeListKeys() ([]string, error)
}

// IOIndexer is a cache.Indexer that uses some backing I/O, thus:
// 1) it has a Close() method
// 2) List* methods may panic on I/O errors. Safe* (error-returning) variants are added
type IOIndexer interface {
	cache.Indexer
	io.Closer

	// SafeList returns a list of all the currently non-empty accumulators
	SafeList() ([]interface{}, error)

	// SafeListKeys returns a list of all the keys currently associated with non-empty accumulators
	SafeListKeys() ([]string, error)

	// SafeListIndexFuncValues returns all the indexed values of the given index
	SafeListIndexFuncValues(indexName string) ([]string, error)
}

// sqlIndexer is a cache.Indexer which stores objects in a SQL database
type sqlIndexer struct {
	keyfunc cache.KeyFunc
	typ     reflect.Type

	db *sql.DB

	addStmt                 *sql.Stmt
	addIndexStmt            *sql.Stmt
	getStmt                 *sql.Stmt
	deleteStmt              *sql.Stmt
	listStmt                *sql.Stmt
	deleteAllStmt           *sql.Stmt
	listKeysStmt            *sql.Stmt
	listObjectsFromIndex    *sql.Stmt
	listKeysFromIndexStmt   *sql.Stmt
	listIndexFuncValuesStmt *sql.Stmt

	indexers cache.Indexers
}

func initSchema(db *sql.DB, indexers cache.Indexers) error {
	// sanity checks
	for key := range indexers {
		if strings.Contains(key, `"`) {
			panic("Quote characters (\") in indexer names are not supported")
		}
	}

	// schema definition statements
	stmts := []string{
		`DROP TABLE IF EXISTS indices`,
		`DROP TABLE IF EXISTS objects`,
		`CREATE TABLE objects (
			id INTEGER PRIMARY KEY,
			key VARCHAR UNIQUE NOT NULL,
			object BLOB
        )`,
		`CREATE TABLE indices (
			id INTEGER PRIMARY KEY,
			name VARCHAR NOT NULL,
			value VARCHAR NOT NULL,
			object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE
        )`,
		"CREATE INDEX key_index ON objects(key)",
		"CREATE INDEX indices_name_value_index ON indices(name, value)",
	}

	for _, stmt := range stmts {
		_, err := db.Exec(stmt)
		if err != nil {
			return errors.Wrap(err, "Error initializing DB")
		}
	}

	return nil
}

// NewIndexer returns an IOIndexer backed by SQLite for the type typ
func NewIndexer(keyfunc cache.KeyFunc, typ reflect.Type, indexers cache.Indexers) (IOIndexer, error) {
	db, err := sql.Open("sqlite3", "./sqlstore.sqlite?mode=rwc&_journal_mode=memory&_synchronous=off&_mutex=no&_foreign_keys=on")
	if err != nil {
		return nil, err
	}

	err = initSchema(db, indexers)
	if err != nil {
		return nil, err
	}

	// Using UPSERT for both Add() and Update()
	// Add() calls will not fail on existing keys and Update() calls new objects will not fail as well
	// This seems to be a common pattern at least in client-go, specifically cache.ThreadSafeStore
	addStmt, err := db.Prepare("INSERT INTO objects(key, object) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET object = excluded.object")
	if err != nil {
		return nil, err
	}

	addIndexStmt, err := db.Prepare("INSERT INTO indices(name, value, object_id) VALUES (?, ?, ?)")
	if err != nil {
		return nil, err
	}

	getStmt, err := db.Prepare("SELECT object FROM objects WHERE key = ?")
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

	listObjectsFromIndexStmt, err := db.Prepare(`
		SELECT object FROM objects
			WHERE id IN (
			    SELECT object_id FROM indices
			    	WHERE name = ? AND value = ?
			)
	`)
	if err != nil {
		return nil, err
	}

	listKeysFromIndexStmt, err := db.Prepare(`
		SELECT key FROM objects
			WHERE id IN (
			    SELECT object_id FROM indices
			    	WHERE name = ? AND value = ?
			)
	`)
	if err != nil {
		return nil, err
	}

	listIndexFuncValuesStmt, err := db.Prepare(`SELECT DISTINCT value FROM indices WHERE name = ?`)
	if err != nil {
		return nil, err
	}

	return &sqlIndexer{
		typ:                     typ,
		keyfunc:                 keyfunc,
		db:                      db,
		addStmt:                 addStmt,
		addIndexStmt:            addIndexStmt,
		getStmt:                 getStmt,
		deleteStmt:              deleteStmt,
		listStmt:                listStmt,
		deleteAllStmt:           deleteAllStmt,
		listKeysStmt:            listKeysStmt,
		indexers:                indexers,
		listObjectsFromIndex:    listObjectsFromIndexStmt,
		listKeysFromIndexStmt:   listKeysFromIndexStmt,
		listIndexFuncValuesStmt: listIndexFuncValuesStmt,
	}, nil
}

// NewStore returns an IOStore backed by SQLite for the type typ
func NewStore(keyfunc cache.KeyFunc, typ reflect.Type) (IOStore, error) {
	return NewIndexer(keyfunc, typ, cache.Indexers{})
}
