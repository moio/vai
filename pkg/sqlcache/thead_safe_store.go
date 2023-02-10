package sqlcache

import (
	"github.com/pkg/errors"
	"k8s.io/client-go/tools/cache"
)

// threadSafeStore is a SQLite-backed cache.ThreadSafeStore which builds upon Index
type threadSafeStore struct {
	*Indexer
}

// NewThreadSafeStore returns a cache.ThreadSafeStore backed by SQLite for the example type
func NewThreadSafeStore(example any, path string, indexers cache.Indexers) (cache.ThreadSafeStore, error) {
	i, err := NewIndexer(example, dummyKeyFunc, path, indexers)
	if err != nil {
		return nil, err
	}

	return &threadSafeStore{i}, nil
}

// Add saves an obj with its key, or updates key with obj if it exists in this store
// Note: I/O errors will panic this function, as the interface signature does not allow returning errors
func (t threadSafeStore) Add(key string, obj any) {
	err := t.Upsert(key, obj)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in threadSafeStore.Add"))
	}
}

// Update delegates to Add
// Note: I/O errors will panic this function, as the interface signature does not allow returning errors
func (t threadSafeStore) Update(key string, obj any) {
	t.Add(key, obj)
}

// Delete deletes the object associated with key, if it exists in this store
// Note: I/O errors will panic this function, as the interface signature does not allow returning errors
func (t threadSafeStore) Delete(key string) {
	err := t.DeleteByKey(key)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in threadSafeStore.Delete"))
	}
}

// Get returns the object associated with the given object's key
// Note: I/O errors will panic this function, as the interface signature does not allow returning errors
func (t threadSafeStore) Get(key string) (any, bool) {
	item, exists, err := t.GetByKey(key)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in threadSafeStore.Get"))
	}
	return item, exists
}

// Replace will delete the contents of the store, using instead the given list
// Note: I/O errors will panic this function, as the interface signature does not allow returning errors
func (t threadSafeStore) Replace(m map[string]any, _ string) {
	err := t.ReplaceByKey(m)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in threadSafeStore.Replace"))
	}
}

// dummyKeyFunc panics - ThreadSafeStore is designed to work without one
func dummyKeyFunc(obj any) (string, error) {
	panic("keyFunc called from ThreadSafeStore")
}
