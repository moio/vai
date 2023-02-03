package sqlcache

import (
	_ "github.com/mattn/go-sqlite3"
	"io"
	"k8s.io/client-go/tools/cache"
	"reflect"
)

// IOIndexer is a cache.Indexer that uses some backing I/O, thus:
// 1) it has a Close() method
// 2) data methods may panic on I/O errors. Safe* (error-returning) variants are added
type IOIndexer interface {
	cache.Indexer
	io.Closer

	// SafeList returns a list of all the currently known objects
	SafeList() ([]interface{}, error)

	// SafeListKeys returns a list of all the keys currently in this store
	SafeListKeys() ([]string, error)

	// SafeListIndexFuncValues returns all the indexed values of the given index
	SafeListIndexFuncValues(indexName string) ([]string, error)
}

// sqlIndexer is an IOIndexer which stores objects in a SQL database
// delegates the heavy lifting to a cache.ThreadSafeStore
type sqlIndexer struct {
	keyfunc         cache.KeyFunc
	threadSafeStore IOThreadSafeStore
}

// NewStore returns an cache.Store backed by SQLite for the type typ
func NewStore(keyfunc cache.KeyFunc, typ reflect.Type) (IOStore, error) {
	return NewIndexer(keyfunc, typ, cache.Indexers{})
}

// NewIndexer returns an cache.Indexer backed by SQLite for the type typ
func NewIndexer(keyfunc cache.KeyFunc, typ reflect.Type, indexers cache.Indexers) (IOIndexer, error) {
	threadSafeStore, err := NewThreadSafeStore(typ, indexers)
	if err != nil {
		return nil, err
	}
	return &sqlIndexer{
		keyfunc:         keyfunc,
		threadSafeStore: threadSafeStore,
	}, nil
}

/* Satisfy IOStore */

// Add saves an obj, or updates it if it exists in this store
func (s *sqlIndexer) Add(obj interface{}) error {
	key, err := s.keyfunc(obj)
	if err != nil {
		return err
	}

	return s.threadSafeStore.SafeAdd(key, obj)
}

// Update saves an obj, or updates it if it exists in this store
func (s *sqlIndexer) Update(obj interface{}) error {
	key, err := s.keyfunc(obj)
	if err != nil {
		return err
	}
	return s.threadSafeStore.SafeUpdate(key, obj)
}

// Delete deletes the given object, if it exists in this store
func (s *sqlIndexer) Delete(obj interface{}) error {
	key, err := s.keyfunc(obj)
	if err != nil {
		return err
	}

	return s.threadSafeStore.SafeDelete(key)
}

// Get returns the object with the same key as obj
func (s *sqlIndexer) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := s.keyfunc(obj)
	if err != nil {
		return nil, false, err
	}

	return s.GetByKey(key)
}

// GetByKey returns the object associated with the given object's key
func (s *sqlIndexer) GetByKey(key string) (item interface{}, exists bool, err error) {
	return s.threadSafeStore.SafeGet(key)
}

// List wraps SafeList and panics in case of I/O errors
func (s *sqlIndexer) List() []interface{} {
	return s.threadSafeStore.List()
}

// SafeList returns a list of all the currently known objects
func (s *sqlIndexer) SafeList() ([]interface{}, error) {
	return s.threadSafeStore.SafeList()
}

// ListKeys wraps SafeListKeys and panics in case of I/O errors
func (s *sqlIndexer) ListKeys() []string {
	return s.threadSafeStore.ListKeys()
}

// SafeListKeys returns a list of all the keys currently in this store
func (s *sqlIndexer) SafeListKeys() ([]string, error) {
	return s.threadSafeStore.SafeListKeys()
}

// Replace will delete the contents of the store, using instead the given list
func (s *sqlIndexer) Replace(objects []interface{}, dc string) error {
	objectMap := map[string]interface{}{}

	for _, object := range objects {
		key, err := s.keyfunc(object)
		if err != nil {
			return err
		}
		objectMap[key] = object
	}

	return s.threadSafeStore.SafeReplace(objectMap, dc)
}

// Resync is a no-op and is deprecated
func (s *sqlIndexer) Resync() error {
	return s.threadSafeStore.Resync()
}

/* Satisfy cache.IOIndexer */

// Index returns a list of items that match the given object on the index function
func (s *sqlIndexer) Index(indexName string, obj interface{}) ([]interface{}, error) {
	return s.threadSafeStore.Index(indexName, obj)
}

// IndexKeys returns a list of the Store keys of the objects whose indexed values in the given index include the given indexed value
func (s *sqlIndexer) IndexKeys(indexName, indexedValue string) ([]string, error) {
	return s.threadSafeStore.IndexKeys(indexName, indexedValue)
}

// ListIndexFuncValues returns all the indexed values of the given index
func (s *sqlIndexer) ListIndexFuncValues(indexName string) []string {
	return s.threadSafeStore.ListIndexFuncValues(indexName)
}

// SafeListIndexFuncValues returns all the indexed values of the given index
func (s *sqlIndexer) SafeListIndexFuncValues(indexName string) ([]string, error) {
	return s.threadSafeStore.SafeListIndexFuncValues(indexName)
}

// ByIndex returns the stored objects whose set of indexed values
// for the named index includes the given indexed value
func (s *sqlIndexer) ByIndex(indexName, indexedValue string) ([]interface{}, error) {
	return s.threadSafeStore.ByIndex(indexName, indexedValue)
}

// GetIndexers return the indexers
func (s *sqlIndexer) GetIndexers() cache.Indexers {
	return s.threadSafeStore.GetIndexers()
}

// AddIndexers adds more indexers to this store.  If you call this after you already have data
// in the store, the results are undefined.
func (s *sqlIndexer) AddIndexers(newIndexers cache.Indexers) error {
	return s.threadSafeStore.AddIndexers(newIndexers)
}

/* Satisfy io.Closer */

// Close closes the database and prevents new queries from starting
func (s *sqlIndexer) Close() error {
	return s.threadSafeStore.Close()
}
