/*
Copyright 2012 SUSE LLC
*/

package cache

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
)

// Test public interface
func doTestStore(t *testing.T, store cache.Store) {
	mkObj := func(id string, val string) testStoreObject {
		return testStoreObject{Id: id, Val: val}
	}

	err := store.Add(mkObj("foo", "bar"))
	if err != nil {
		t.Error(err)
	}
	if item, ok, err := store.Get(mkObj("foo", "")); !ok {
		if err != nil {
			t.Error(err)
		}
		t.Errorf("didn't find inserted item")
	} else {
		if e, a := "bar", item.(testStoreObject).Val; e != a {
			t.Errorf("expected %v, got %v", e, a)
		}
	}
	if err != nil {
		t.Error(err)
	}
	err = store.Update(mkObj("foo", "baz"))
	if err != nil {
		t.Error(err)
	}
	if item, ok, err := store.Get(mkObj("foo", "")); !ok {
		if err != nil {
			t.Error(err)
		}
		t.Errorf("didn't find inserted item")
	} else {
		if e, a := "baz", item.(testStoreObject).Val; e != a {
			t.Errorf("expected %v, got %v", e, a)
		}
	}
	err = store.Delete(mkObj("foo", ""))
	if err != nil {
		t.Error(err)
	}
	if _, ok, err := store.Get(mkObj("foo", "")); ok {
		if err != nil {
			t.Error(err)
		}
		t.Errorf("found deleted item??")
	}

	// Test List.
	err = store.Add(mkObj("a", "b"))
	if err != nil {
		t.Error(err)
	}
	err = store.Add(mkObj("c", "d"))
	if err != nil {
		t.Error(err)
	}
	err = store.Add(mkObj("e", "e"))
	if err != nil {
		t.Error(err)
	}
	{
		found := sets.String{}
		for _, item := range store.List() {
			found.Insert(item.(testStoreObject).Val)
		}
		if !found.HasAll("b", "d", "e") {
			t.Errorf("missing items, found: %v", found)
		}
		if len(found) != 3 {
			t.Errorf("extra items")
		}
	}

	// Test ListKeys
	keys := store.ListKeys()
	found := sets.String{}
	for _, key := range keys {
		found.Insert(key)
	}
	if !found.HasAll("a", "c", "e") {
		t.Errorf("missing items, found: %v", found)
	}
	if len(found) != 3 {
		t.Errorf("extra items")
	}

	// Test Replace.
	err = store.Replace([]interface{}{
		mkObj("foo", "foo"),
		mkObj("bar", "bar"),
	}, "0")
	if err != nil {
		t.Error(err)
	}

	{
		found := sets.String{}
		for _, item := range store.List() {
			found.Insert(item.(testStoreObject).Val)
		}
		if !found.HasAll("foo", "bar") {
			t.Errorf("missing items")
		}
		if len(found) != 2 {
			t.Errorf("extra items")
		}
	}
}

// Test public interface
func doTestIndex(t *testing.T, indexer cache.Indexer) {
	mkObj := func(id string, val string) testStoreObject {
		return testStoreObject{Id: id, Val: val}
	}

	// Test Index
	expected := map[string]sets.String{}
	expected["b"] = sets.NewString("a", "c")
	expected["f"] = sets.NewString("e")
	expected["h"] = sets.NewString("g")
	indexer.Add(mkObj("a", "b"))
	indexer.Add(mkObj("c", "b"))
	indexer.Add(mkObj("e", "f"))
	indexer.Add(mkObj("g", "h"))
	{
		for k, v := range expected {
			found := sets.String{}
			indexResults, err := indexer.Index("by_val", mkObj("", k))
			if err != nil {
				t.Errorf("Unexpected error %v", err)
			}
			for _, item := range indexResults {
				found.Insert(item.(testStoreObject).Id)
			}
			items := v.List()
			if !found.HasAll(items...) {
				t.Errorf("missing items, index %s, expected %v but found %v", k, items, found.List())
			}
		}
	}
}

func testStoreKeyFunc(obj interface{}) (string, error) {
	return obj.(testStoreObject).Id, nil
}

func testStoreIndexFunc(obj interface{}) ([]string, error) {
	return []string{obj.(testStoreObject).Val}, nil
}

func testStoreIndexers() cache.Indexers {
	indexers := cache.Indexers{}
	indexers["by_val"] = testStoreIndexFunc
	return indexers
}

type testStoreObject struct {
	Id  string
	Val string
}

func TestSQLStore(t *testing.T) {
	store, err := NewSQLStore(testStoreKeyFunc, reflect.TypeOf(testStoreObject{}))
	if err != nil {
		t.Error(err)
	}
	doTestStore(t, store)
	err = store.Close()
	if err != nil {
		return
	}
}
