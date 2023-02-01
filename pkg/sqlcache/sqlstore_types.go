package cache

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
	updateStmt              *sql.Stmt
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

// NewSQLIndexer returns a SQLite-backed IOIndexer for the type typ
func NewSQLIndexer(keyfunc cache.KeyFunc, typ reflect.Type, indexers cache.Indexers) (IOIndexer, error) {
	db, err := sql.Open("sqlite3", "./sqlstore.sqlite")
	if err != nil {
		return nil, err
	}

	err = initSchema(db, indexers)
	if err != nil {
		return nil, err
	}

	addStmt, err := db.Prepare("INSERT INTO objects(key, object) VALUES (?, ?)")
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
		updateStmt:              updateStmt,
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
