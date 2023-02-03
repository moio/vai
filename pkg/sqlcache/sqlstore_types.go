package sqlcache

import (
	_ "github.com/mattn/go-sqlite3"
	"io"
	"k8s.io/client-go/tools/cache"
)

// IOStore is a cache.Store that uses some backing I/O, thus:
// 1) it has a Close() method
// 2) List* methods may panic on I/O errors. Safe* (error-returning) variants are added
type IOStore interface {
	cache.Store
	io.Closer

	// SafeList returns a list of all the currently known objects
	SafeList() ([]interface{}, error)

	// SafeListKeys returns a list of all the keys currently in this store
	SafeListKeys() ([]string, error)
}

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

// IOThreadSafeStore is a cache.ThreadSafeStore that uses some backing I/O, thus:
// 1) it has a Close() method
// 2) data methods may panic on I/O errors. Safe* (error-returning) variants are added
type IOThreadSafeStore interface {
	cache.ThreadSafeStore
	io.Closer

	// SafeAdd saves an obj with its key, or updates key with obj if it exists in this store
	SafeAdd(key string, obj interface{}) error

	// SafeUpdate saves an obj with its key, or updates key with obj if it exists in this store
	SafeUpdate(key string, obj interface{}) error

	// SafeDelete deletes the object associated with key, if it exists in this store
	SafeDelete(key string) error

	// SafeGet returns the object associated with the given object's key
	SafeGet(key string) (item interface{}, exists bool, err error)

	// SafeReplace will delete the contents of the store, using instead the given list
	SafeReplace(map[string]interface{}, string) error

	// SafeList returns a list of all the currently known objects
	SafeList() ([]interface{}, error)

	// SafeListKeys returns a list of all the keys currently in this store
	SafeListKeys() ([]string, error)

	// SafeListIndexFuncValues returns all the indexed values of the given index
	SafeListIndexFuncValues(indexName string) ([]string, error)
}
