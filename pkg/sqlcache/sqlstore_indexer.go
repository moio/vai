package cache

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"k8s.io/client-go/tools/cache"
	"strings"
)

/* Satisfy cache.IOIndexer */

// Index returns a list of items that match the given object on the index function
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

	// atypical case - more than one value to lookup
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

// IndexKeys returns a list of the Store keys of the objects whose indexed values in the given index include the given indexed value
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

// ListIndexFuncValues returns all the indexed values of the given index
func (s *sqlIndexer) ListIndexFuncValues(indexName string) []string {
	result, err := s.SafeListIndexFuncValues(indexName)
	if err != nil {
		fmt.Printf("Error in sqlIndexer.List %v", err)
	}

	return result
}

// SafeListIndexFuncValues returns all the indexed values of the given index
func (s *sqlIndexer) SafeListIndexFuncValues(indexName string) ([]string, error) {
	rows, err := s.listIndexFuncValuesStmt.Query(indexName)
	if err != nil {
		return nil, err
	}

	return s.processStringRows(rows)
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
