/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package sqlcache

import (
	"fmt"
	"k8s.io/client-go/tools/cache"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

const TEST_DB_LOCATION = "./sqlstore.sqlite"

func TestThreadSafeStoreDeleteRemovesEmptySetsFromIndex(t *testing.T) {
	testIndexer := "testIndexer"

	indexers := cache.Indexers{
		testIndexer: func(obj interface{}) (strings []string, e error) {
			indexes := []string{obj.(string)}
			return indexes, nil
		},
	}

	store, err := NewThreadSafeStore("", TEST_DB_LOCATION, indexers)
	if err != nil {
		t.Errorf("Unexpected error creating ThreadSafeStore: %v", err)
	}

	testKey := "testKey"

	store.Add(testKey, testKey)

	// Assumption check, there should be a set for the `testKey` with one element in the added index
	set, err := store.Index(testIndexer, testKey)
	if err != nil {
		t.Errorf("Unexpected error reading index: %v", err)
	}
	if len(set) != 1 {
		t.Errorf("Initial assumption of index backing string set having 1 element failed. Actual elements: %d", len(set))
		return
	}

	store.Delete(testKey)
	set, err = store.Index(testIndexer, testKey)
	if err != nil {
		t.Errorf("Unexpected error reading index: %v", err)
	}
	if len(set) > 0 {
		t.Errorf("Index backing string set not deleted from index. Set length: %d", len(set))
	}
}

func TestThreadSafeStoreAddKeepsNonEmptySetPostDeleteFromIndex(t *testing.T) {
	testIndexer := "testIndexer"
	testIndex := "testIndex"

	indexers := cache.Indexers{
		testIndexer: func(obj interface{}) (strings []string, e error) {
			indexes := []string{testIndex}
			return indexes, nil
		},
	}

	store, err := NewThreadSafeStore("", TEST_DB_LOCATION, indexers)
	if err != nil {
		t.Errorf("Unexpected error creating ThreadSafeStore: %v", err)
	}

	store.Add("retain", "retain")
	store.Add("delete", "delete")

	// Assumption check, there should be a set for the `testIndex` with two elements
	set, err := store.Index(testIndexer, "retain")
	if err != nil {
		t.Errorf("Unexpected error reading index: %v", err)
	}
	if len(set) != 2 {
		t.Errorf("Initial assumption of index backing string set having 2 elements failed. Actual elements: %d", len(set))
		return
	}

	store.Delete("delete")
	set, err = store.Index(testIndexer, "delete")
	if err != nil {
		t.Errorf("Unexpected error reading index: %v", err)
	}

	if len(set) == 0 {
		t.Errorf("Index backing string set erroneously deleted from index.")
		return
	}

	if len(set) != 1 {
		t.Errorf("Index backing string set has incorrect length, expect 1. Set length: %d", len(set))
	}
}

func TestThreadSafeStoreIndexingFunctionsWithMultipleValues(t *testing.T) {
	testIndexer := "testIndexer"

	indexers := cache.Indexers{
		testIndexer: func(obj interface{}) ([]string, error) {
			return strings.Split(obj.(string), ","), nil
		},
	}

	store, err := NewThreadSafeStore("", TEST_DB_LOCATION, indexers)
	if err != nil {
		t.Errorf("Unexpected error creating ThreadSafeStore: %v", err)
	}

	store.Add("key1", "foo")
	store.Add("key2", "bar")

	assert := assert.New(t)

	compare := func(key string, expected []string) error {
		set, err := store.Index(testIndexer, key)
		if err != nil {
			t.Errorf("Unexpected error reading index: %v", err)
		}

		values := []string{}
		for _, value := range set {
			values = append(values, fmt.Sprintf("%v", value))
		}
		sort.Strings(values)

		if cmp.Equal(values, expected) {
			return nil
		}
		return fmt.Errorf("unexpected index for key %s, diff=%s", key, cmp.Diff(values, expected))
	}

	assert.NoError(compare("foo", []string{"foo"}))
	assert.NoError(compare("bar", []string{"bar"}))

	store.Update("key2", "foo,bar")

	assert.NoError(compare("foo", []string{"foo", "foo,bar"}))
	assert.NoError(compare("bar", []string{"foo,bar"}))

	store.Update("key1", "foo,bar")

	assert.NoError(compare("foo", []string{"foo,bar", "foo,bar"}))
	assert.NoError(compare("bar", []string{"foo,bar", "foo,bar"}))

	store.Add("key3", "foo,bar,baz")

	assert.NoError(compare("foo", []string{"foo,bar", "foo,bar", "foo,bar,baz"}))
	assert.NoError(compare("bar", []string{"foo,bar", "foo,bar", "foo,bar,baz"}))
	assert.NoError(compare("baz", []string{"foo,bar,baz"}))

	store.Update("key1", "foo")

	assert.NoError(compare("foo", []string{"foo", "foo,bar", "foo,bar,baz"}))
	assert.NoError(compare("bar", []string{"foo,bar", "foo,bar,baz"}))
	assert.NoError(compare("baz", []string{"foo,bar,baz"}))

	store.Update("key2", "bar")

	assert.NoError(compare("foo", []string{"foo", "foo,bar,baz"}))
	assert.NoError(compare("bar", []string{"bar", "foo,bar,baz"}))
	assert.NoError(compare("baz", []string{"foo,bar,baz"}))

	store.Delete("key1")

	assert.NoError(compare("foo", []string{"foo,bar,baz"}))
	assert.NoError(compare("bar", []string{"bar", "foo,bar,baz"}))
	assert.NoError(compare("baz", []string{"foo,bar,baz"}))

	store.Delete("key3")

	assert.NoError(compare("foo", []string{}))
	assert.NoError(compare("bar", []string{"bar"}))
	assert.NoError(compare("baz", []string{}))
}

func BenchmarkIndexer(b *testing.B) {
	testIndexer := "testIndexer"

	indexers := cache.Indexers{
		testIndexer: func(obj interface{}) (strings []string, e error) {
			indexes := []string{obj.(string)}
			return indexes, nil
		},
	}

	store, err := NewThreadSafeStore("", TEST_DB_LOCATION, indexers)
	if err != nil {
		b.Errorf("Unexpected error creating ThreadSafeStore: %v", err)
	}

	// The following benchmark imitates what is happening in indexes
	// used in storage layer, where indexing is mostly static (e.g.
	// indexing objects by their (namespace, name)).
	// The 5000 number imitates indexing nodes in 5000-node cluster.
	objectCount := 5000
	objects := make([]string, 0, 5000)
	for i := 0; i < objectCount; i++ {
		objects = append(objects, fmt.Sprintf("object-number-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Update(objects[i%objectCount], objects[i%objectCount])
	}
}
