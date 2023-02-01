package cache

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"github.com/pkg/errors"
	"reflect"
)

/* Satisfy IOStore */

// Add adds the given object to the accumulator associated with the given object's key
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

// Update updates the given object in the accumulator associated with the given object's key
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

// Delete deletes the given object from the accumulator associated with the given object's key
func (s *sqlIndexer) Delete(obj interface{}) error {
	key, err := s.keyfunc(obj)
	if err != nil {
		return err
	}

	_, err = s.deleteStmt.Exec(key)
	return err
}

// List returns a list of all the currently non-empty accumulators
func (s *sqlIndexer) List() []interface{} {
	result, err := s.SafeList()
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in sqlIndexer.List"))
	}

	return result
}

// SafeList returns a list of all the currently non-empty accumulators
func (s *sqlIndexer) SafeList() ([]interface{}, error) {
	rows, err := s.listStmt.Query()
	if err != nil {
		return nil, err
	}
	return s.processObjectRows(rows)
}

// ListKeys returns a list of all the keys currently associated with non-empty accumulators
func (s *sqlIndexer) ListKeys() []string {
	result, err := s.SafeListKeys()
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in sqlIndexer.ListKeys"))
	}

	return result
}

// SafeListKeys returns a list of all the keys currently associated with non-empty accumulators
func (s *sqlIndexer) SafeListKeys() ([]string, error) {
	rows, err := s.listKeysStmt.Query()
	if err != nil {
		return nil, err
	}

	return s.processStringRows(rows)
}

// Get returns the accumulator associated with the given object's key
func (s *sqlIndexer) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := s.keyfunc(obj)
	if err != nil {
		return nil, false, err
	}

	return s.GetByKey(key)
}

// GetByKey returns the accumulator associated with the given key
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

// Replace will delete the contents of the store, using instead the given list
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

// Resync is a no-op
func (s *sqlIndexer) Resync() error {
	return nil
}

// processObjectRows expects a sql.Rows pointer with one column which is byte slice containing a
// gobbed object, and returns a slice of objects
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

// closeOnError closes the sql.Rows object and wraps errors if needed
func closeOnError(rows *sql.Rows, err error) ([]interface{}, error) {
	ce := rows.Close()
	if ce != nil {
		return nil, errors.Wrap(ce, "while handling "+err.Error())
	}

	return nil, err
}

// processStringRows expects a sql.Rows pointer with one column which is a string,
// and returns a slice of strings
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

// Close closes the database and prevents new queries from starting
func (s *sqlIndexer) Close() error {
	return s.db.Close()
}
