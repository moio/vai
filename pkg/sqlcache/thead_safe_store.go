package sqlcache

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"k8s.io/client-go/tools/cache"
	"os"
	"reflect"
	"strings"
)

// sqlThreadSafeStore is a cache.ThreadSafeStore which stores objects in a SQL database
type sqlThreadSafeStore struct {
	typ reflect.Type

	db *sql.DB

	stmts map[string]*sql.Stmt

	indexers cache.Indexers
}

const DB_LOCATION = "./sqlstore.sqlite"

// NewThreadSafeStore returns a cache.ThreadSafeStore backed by SQLite for the type typ
func NewThreadSafeStore(typ reflect.Type, indexers cache.Indexers, queries []string) (IOThreadSafeStore, error) {
	err := os.RemoveAll(DB_LOCATION)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", DB_LOCATION+"?mode=rwc&_journal_mode=memory&_synchronous=off&_mutex=no&_foreign_keys=on")
	if err != nil {
		return nil, err
	}

	err = initSchema(db, indexers)
	if err != nil {
		return nil, err
	}

	stmts := map[string]*sql.Stmt{}
	prepare(db, stmts, `INSERT INTO objects(key, object) VALUES (?, ?) ON CONFLICT DO UPDATE SET object = excluded.object`)
	prepare(db, stmts, `SELECT key FROM objects`)
	prepare(db, stmts, `SELECT object FROM objects`)
	prepare(db, stmts, `SELECT object FROM objects WHERE key = ?`)
	prepare(db, stmts, `
		SELECT object FROM objects
			WHERE key IN (
			    SELECT key FROM indices
					WHERE name = ? AND value = ?
			)
	`)
	prepare(db, stmts, `DELETE FROM objects WHERE key = ?`)
	prepare(db, stmts, `DELETE FROM objects`)

	prepare(db, stmts, `INSERT INTO indices(name, value, key) VALUES (?, ?, ?)`)
	prepare(db, stmts, `SELECT DISTINCT key FROM indices WHERE name = ? AND value = ?`)
	prepare(db, stmts, `SELECT DISTINCT value FROM indices WHERE name = ?`)
	prepare(db, stmts, `DELETE FROM indices WHERE key = ?`)

	for _, query := range queries {
		prepare(db, stmts, query)
	}

	return &sqlThreadSafeStore{
		typ:      typ,
		db:       db,
		indexers: indexers,
		stmts:    stmts,
	}, nil
}

// prepare prepares a statement and puts in in the stmts map
func prepare(db *sql.DB, stmts map[string]*sql.Stmt, sql string) {
	stmt, err := db.Prepare(sql)
	if err != nil {
		panic(errors.Errorf("Could not prepare statement: %s\n%v", sql, err))
	}
	stmts[sql] = stmt
}

// initSchema prepares the schema on a fresh SQLite database
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
			key VARCHAR UNIQUE NOT NULL PRIMARY KEY,
			object BLOB
        )`,
		`CREATE TABLE indices (
			name VARCHAR NOT NULL,
			value VARCHAR NOT NULL,
			key VARCHAR NOT NULL REFERENCES objects(key) ON DELETE CASCADE,
			PRIMARY KEY (name, value, key)
        )`,
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

// Add wraps SafeAdd and panics in case of I/O errors
func (s *sqlThreadSafeStore) Add(key string, obj interface{}) {
	err := s.SafeAdd(key, obj)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in sqlThreadSafeStore.Add"))
	}
}

// SafeAdd saves an obj with its key, or updates key with obj if it exists in this store
func (s *sqlThreadSafeStore) SafeAdd(key string, obj interface{}) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	if err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	// Using UPSERT for both Add() and Update()
	// Add() calls will not fail on existing keys and Update() calls new objects will not fail as well
	// This seems to be a common pattern at least in client-go, specifically cache.ThreadSafeStore
	stmt := s.stmts[`INSERT INTO objects(key, object) VALUES (?, ?) ON CONFLICT DO UPDATE SET object = excluded.object`]
	_, err = tx.Stmt(stmt).Exec(key, buf.Bytes())
	if err != nil {
		return err
	}

	stmt = s.stmts[`DELETE FROM indices WHERE key = ?`]
	_, err = tx.Stmt(stmt).Exec(key)
	if err != nil {
		return err
	}

	for indexName, indexFunc := range s.indexers {
		values, err := indexFunc(obj)
		if err != nil {
			return err
		}

		for _, value := range values {
			stmt = s.stmts[`INSERT INTO indices(name, value, key) VALUES (?, ?, ?)`]
			_, err = tx.Stmt(stmt).Exec(indexName, value, key)
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

// Update delegates to Add
func (s *sqlThreadSafeStore) Update(key string, obj interface{}) {
	s.Add(key, obj)
}

// SafeUpdate delegates to SafeAdd
func (s *sqlThreadSafeStore) SafeUpdate(key string, obj interface{}) error {
	return s.SafeAdd(key, obj)
}

// Delete wraps SafeDelete and panics in case of I/O errors
func (s *sqlThreadSafeStore) Delete(key string) {
	err := s.SafeDelete(key)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in sqlThreadSafeStore.SafeDelete"))
	}
}

// SafeDelete deletes the object associated with key, if it exists in this store
func (s *sqlThreadSafeStore) SafeDelete(key string) error {
	stmt := s.stmts[`DELETE FROM objects WHERE key = ?`]
	_, err := stmt.Exec(key)
	return err
}

// Get wraps SafeGet and panics in case of I/O errors
func (s *sqlThreadSafeStore) Get(key string) (item interface{}, exists bool) {
	item, exists, err := s.SafeGet(key)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in sqlThreadSafeStore.SafeGet"))
	}
	return item, exists
}

// SafeGet returns the object associated with the given object's key
func (s *sqlThreadSafeStore) SafeGet(key string) (item interface{}, exists bool, err error) {
	result, err := s.QueryObjects(`SELECT object FROM objects WHERE key = ?`, key)
	if err != nil {
		return nil, false, err
	}

	if len(result) == 0 {
		return nil, false, nil
	}

	return result[0], true, nil
}

// List wraps SafeList and panics in case of I/O errors
func (s *sqlThreadSafeStore) List() []interface{} {
	result, err := s.SafeList()
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in sqlThreadSafeStore.SafeList"))
	}
	return result
}

// SafeList returns a list of all the currently known objects
func (s *sqlThreadSafeStore) SafeList() ([]interface{}, error) {
	return s.QueryObjects(`SELECT object FROM objects`)
}

// ListKeys wraps SafeListKeys and panics in case of I/O errors
func (s *sqlThreadSafeStore) ListKeys() []string {
	result, err := s.SafeListKeys()
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in sqlThreadSafeStore.SafeListKeys"))
	}
	return result
}

// SafeListKeys returns a list of all the keys currently in this store
func (s *sqlThreadSafeStore) SafeListKeys() ([]string, error) {
	return s.QueryStrings(`SELECT key FROM objects`)
}

// Replace wraps SafeReplace and panics in case of I/O errors
func (s *sqlThreadSafeStore) Replace(objects map[string]interface{}, dc string) {
	err := s.SafeReplace(objects, dc)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in sqlThreadSafeStore.SafeReplace"))
	}
}

// SafeReplace will delete the contents of the store, using instead the given list
func (s *sqlThreadSafeStore) SafeReplace(objects map[string]interface{}, _ string) error {
	stmt := s.stmts[`DELETE FROM objects`]
	_, err := stmt.Exec()
	if err != nil {
		return err
	}

	for key, object := range objects {
		err := s.SafeAdd(key, object)
		if err != nil {
			return err
		}
	}

	return nil
}

// Index returns a list of items that match the given object on the index function
func (s *sqlThreadSafeStore) Index(indexName string, obj interface{}) ([]interface{}, error) {
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
		return s.ByIndex(indexName, values[0])
	}

	// atypical case - more than one value to lookup
	// HACK: sql.Statement.Query does not allow to pass slices in as of go 1.19 - create an ad-hoc statement
	query := fmt.Sprintf(`
			SELECT object FROM objects
				WHERE key IN (
					SELECT key FROM indices
						WHERE name = ? AND value IN (?%s)
				)
		`, strings.Repeat(", ?", len(values)-1))
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}

	// HACK: Query will accept []any but not []string
	params := []any{indexName}
	for _, value := range values {
		params = append(params, value)
	}

	return s.queryObjects(stmt, params...)
}

// IndexKeys returns a list of the Store keys of the objects whose indexed values in the given index include the given indexed value
func (s *sqlThreadSafeStore) IndexKeys(indexName, indexedValue string) ([]string, error) {
	indexFunc := s.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	return s.QueryStrings(`SELECT DISTINCT key FROM indices WHERE name = ? AND value = ?`, indexName, indexedValue)
}

// ListIndexFuncValues wraps SafeListIndexFuncValues and panics in case of I/O errors
func (s *sqlThreadSafeStore) ListIndexFuncValues(name string) []string {
	result, err := s.SafeListIndexFuncValues(name)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in sqlThreadSafeStore.SafeListIndexFuncValues"))
	}
	return result
}

// SafeListIndexFuncValues returns all the indexed values of the given index
func (s *sqlThreadSafeStore) SafeListIndexFuncValues(indexName string) ([]string, error) {
	return s.QueryStrings(`SELECT DISTINCT value FROM indices WHERE name = ?`, indexName)
}

// ByIndex returns the stored objects whose set of indexed values
// for the named index includes the given indexed value
func (s *sqlThreadSafeStore) ByIndex(indexName, indexedValue string) ([]interface{}, error) {
	return s.QueryObjects(`
		SELECT object FROM objects
			WHERE key IN (
			    SELECT key FROM indices
					WHERE name = ? AND value = ?
			)
	`, indexName, indexedValue)
}

// GetIndexers return the indexers
func (s *sqlThreadSafeStore) GetIndexers() cache.Indexers {
	return s.indexers
}

// AddIndexers adds more indexers to this store.  If you call this after you already have data
// in the store, the results are undefined.
func (s *sqlThreadSafeStore) AddIndexers(newIndexers cache.Indexers) error {
	for k, v := range newIndexers {
		s.indexers[k] = v
	}
	return nil
}

// Resync is a no-op and is deprecated
func (s *sqlThreadSafeStore) Resync() error {
	return nil
}

// Close closes the database and prevents new queries from starting
func (s *sqlThreadSafeStore) Close() error {
	return s.db.Close()
}

// QueryObjects runs a query that was previously prepared (see NewThreadSafeStore)
func (s *sqlThreadSafeStore) QueryObjects(sql string, params ...any) ([]interface{}, error) {
	stmt, ok := s.stmts[sql]
	if !ok {
		panic(errors.Errorf("Attempted to execute unprepared query: %s", sql))
	}
	return s.queryObjects(stmt, params...)
}

// queryObjects runs a prepared statement returning a list of objects
func (s *sqlThreadSafeStore) queryObjects(stmt *sql.Stmt, params ...any) ([]interface{}, error) {
	rows, err := stmt.Query(params...)
	if err != nil {
		return nil, err
	}

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

// closeOnError closes the sql.Rows object and wraps errors if needed
func closeOnError(rows *sql.Rows, err error) ([]interface{}, error) {
	ce := rows.Close()
	if ce != nil {
		return nil, errors.Wrap(ce, "while handling "+err.Error())
	}

	return nil, err
}

// QueryStrings runs a query that was previously prepared (see NewThreadSafeStore)
func (s *sqlThreadSafeStore) QueryStrings(sql string, params ...any) ([]string, error) {
	stmt, ok := s.stmts[sql]
	if !ok {
		panic(errors.Errorf("Attempted to execute unprepared query: %s", sql))
	}
	return s.queryStrings(stmt, params...)
}

// queryObjects runs a prepared statement returning a list of strings
func (s *sqlThreadSafeStore) queryStrings(stmt *sql.Stmt, params ...any) ([]string, error) {
	rows, err := stmt.Query(params...)
	if err != nil {
		return nil, err
	}

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
