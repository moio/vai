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

	listKeysFromIndexStmt, err := db.Prepare(`
		SELECT key FROM objects
			WHERE id IN (
			    SELECT object_id FROM indices
			    	WHERE name = ? AND value = ?
			)
	`)

	listIndexFuncValuesStmt, err := db.Prepare(`SELECT DISTINCT value FROM indices WHERE name = ?`)

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

/* Satisfy io.Closer */

func (s *sqlIndexer) Close() error {
	return s.db.Close()
}

/* Satisfy cache.Store */

func (s *sqlIndexer) Add(obj interface{}) error {
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

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	result, err := tx.Stmt(s.addStmt).Exec(key, buf.Bytes())
	if err != nil {
		return err
	}

	objectId, err := result.LastInsertId()
	if err != nil {
		return err
	}

	for indexName, indexFunc := range s.indexers {
		values, err := indexFunc(obj)
		if err != nil {
			return err
		}

		for _, value := range values {
			_, err = tx.Stmt(s.addIndexStmt).Exec(indexName, value, objectId)
			if err != nil {
				return err
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *sqlIndexer) Update(obj interface{}) error {
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

func (s *sqlIndexer) Delete(obj interface{}) error {
	key, err := s.keyfunc(obj)
	if err != nil {
		return err
	}

	_, err = s.deleteStmt.Exec(key)
	return err
}

func (s *sqlIndexer) SafeList() ([]interface{}, error) {
	rows, err := s.listStmt.Query()
	if err != nil {
		return nil, err
	}
	return s.processObjectRows(rows)
}

func (s *sqlIndexer) processObjectRows(rows *sql.Rows) ([]interface{}, error) {
	var result []any
	for rows.Next() {
		var buf sql.RawBytes
		err := rows.Scan(&buf)
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
	err := rows.Err()
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

func (s *sqlIndexer) List() []interface{} {
	result, err := s.SafeList()
	if err != nil {
		fmt.Printf("Error in sqlIndexer.List %v", err)
	}

	return result
}

func (s *sqlIndexer) SafeListKeys() ([]string, error) {
	rows, err := s.listKeysStmt.Query()
	if err != nil {
		return nil, err
	}

	return s.processStringRows(rows)
}

func (s *sqlIndexer) processStringRows(rows *sql.Rows) ([]string, error) {
	var result []string
	for rows.Next() {
		var key string
		err := rows.Scan(&key)
		if err != nil {
			ce := rows.Close()
			if ce != nil {
				return nil, errors.Wrap(ce, "while handling "+err.Error())
			}
		}

		result = append(result, key)
	}
	err := rows.Err()
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

func (s *sqlIndexer) ListKeys() []string {
	result, err := s.SafeListKeys()
	if err != nil {
		fmt.Printf("Error in sqlIndexer.ListKeys %v", err)
	}

	return result
}

func (s *sqlIndexer) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := s.keyfunc(obj)
	if err != nil {
		return nil, false, err
	}

	return s.GetByKey(key)
}

func (s *sqlIndexer) GetByKey(key string) (item interface{}, exists bool, err error) {
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

func (s *sqlIndexer) Replace(objects []interface{}, _ string) error {
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

func (s *sqlIndexer) Resync() error {
	return nil
}

/* Satisfy cache.Indexer */

// Index returns a list of items that match the given object on the index function.
func (s *sqlIndexer) Index(indexName string, obj interface{}) ([]interface{}, error) {
	indexFunc := s.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	values, err := indexFunc(obj)
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, nil
	}

	// typical case
	if len(values) == 1 {
		s.ByIndex(indexName, values[0])
	}

	// untypical case - more than one value to lookup
	// HACK: sql.Statement.Query does not allow to pass slices in as of go 1.19 - use an unprepared statement
	query := fmt.Sprintf(`
			SELECT object FROM objects
				WHERE id IN (
					SELECT object_id FROM indices
						WHERE name = ? AND value IN (?%s)
				)
		`, strings.Repeat(", ?", len(values)-1))

	// HACK: Query will accept []any but not []string
	params := []any{indexName}
	for _, value := range values {
		params = append(params, value)
	}

	rows, err := s.db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	return s.processObjectRows(rows)
}

// IndexKeys returns a list of the Store keys of the objects whose indexed values in the given index include the given indexed value.
func (s *sqlIndexer) IndexKeys(indexName, indexedValue string) ([]string, error) {
	indexFunc := s.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	rows, err := s.listKeysFromIndexStmt.Query(indexName, indexedValue)
	if err != nil {
		return nil, err
	}

	return s.processStringRows(rows)
}

// SafeListIndexFuncValues returns all the indexed values of the given index
func (s *sqlIndexer) SafeListIndexFuncValues(indexName string) ([]string, error) {
	rows, err := s.listIndexFuncValuesStmt.Query(indexName)
	if err != nil {
		return nil, err
	}

	return s.processStringRows(rows)
}

func (s *sqlIndexer) ListIndexFuncValues(indexName string) []string {
	result, err := s.SafeListIndexFuncValues(indexName)
	if err != nil {
		fmt.Printf("Error in sqlIndexer.List %v", err)
	}

	return result
}

// ByIndex returns the stored objects whose set of indexed values
// for the named index includes the given indexed value
func (s *sqlIndexer) ByIndex(indexName, indexedValue string) ([]interface{}, error) {
	rows, err := s.listObjectsFromIndex.Query(indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	return s.processObjectRows(rows)
}

// GetIndexers return the indexers
func (s *sqlIndexer) GetIndexers() cache.Indexers {
	return s.indexers
}

// AddIndexers adds more indexers to this store.  If you call this after you already have data
// in the store, the results are undefined.
func (s *sqlIndexer) AddIndexers(newIndexers cache.Indexers) error {
	for k, v := range newIndexers {
		s.indexers[k] = v
	}
	return nil
}
