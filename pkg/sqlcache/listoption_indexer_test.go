/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package sqlcache

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"

	"k8s.io/client-go/tools/cache"
)

type Car struct {
	Key      string
	Revision int
	Wheels   int
	Brand    string
	Color    string
}

func keyfunc(c any) (string, error) {
	return c.(*Car).Key, nil
}

func versionfunc(c any) (int, error) {
	return c.(*Car).Revision, nil
}

func wheelsfunc(c any) any {
	return c.(*Car).Wheels
}

func brandfunc(c any) any {
	return c.(*Car).Brand
}

func colorfunc(c any) any {
	return c.(*Car).Color
}

var fieldFunc = map[string]FieldFunc{
	"Wheels": wheelsfunc,
	"Brand":  brandfunc,
	"Color":  colorfunc,
}

func TestListOptionIndexer(t *testing.T) {
	assert := assert.New(t)

	l, err := NewListOptionIndexer(reflect.TypeOf(Car{}), keyfunc, versionfunc, TEST_DB_LOCATION, cache.Indexers{}, fieldFunc)
	if err != nil {
		t.Error(err)
	}

	revision := 1
	red := &Car{
		Key:      "testa rossa",
		Revision: revision,
		Wheels:   4,
		Brand:    "ferrari",
		Color:    "red",
	}
	err = l.Add(red)
	if err != nil {
		t.Error(err)
	}

	// add two cars and list with default options
	revision++
	blue := &Car{
		Key:      "focus",
		Revision: revision,
		Wheels:   4,
		Brand:    "ford",
		Color:    "blue",
	}
	err = l.Add(blue)
	if err != nil {
		t.Error(err)
	}

	lo := ListOptions{
		Filters:    nil,
		Sort:       Sort{},
		Pagination: Pagination{},
		Revision:   "",
	}
	r, err := l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 2)

	// delete one and list again. Should be gone
	err = l.Delete(red)
	if err != nil {
		t.Error(err)
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)
	assert.Equal(r[0].(Car).Key, "focus")
	// gone also from most-recent store
	r = l.List()
	assert.Len(r, 1)
	assert.Equal(r[0].(Car).Key, "focus")

	// updating the car brings it back
	revision++
	red.Wheels = 3
	red.Revision = revision
	err = l.Update(red)
	if err != nil {
		t.Error(err)
	}
	r = l.List()
	assert.Len(r, 2)
	lo = ListOptions{
		Filters:    []Filter{{field: []string{"Brand"}, match: "ferrari"}},
		Sort:       Sort{},
		Pagination: Pagination{},
		Revision:   "",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)
	assert.Equal(r[0].(Car).Key, "testa rossa")
	assert.Equal(r[0].(Car).Revision, 3)

	// historically, car still exists in version 1, gone in version 2, back in version 3
	lo = ListOptions{
		Filters:    []Filter{{field: []string{"Brand"}, match: "ferrari"}},
		Sort:       Sort{},
		Pagination: Pagination{},
		Revision:   "1",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)
	lo.Revision = "2"
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 0)
	lo.Revision = "3"
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)

	// add another car, test filter by substring and sorting
	revision++
	black := &Car{
		Key:      "model 3",
		Revision: revision,
		Wheels:   3,
		Brand:    "tesla",
		Color:    "black",
	}
	err = l.Add(black)
	if err != nil {
		t.Error(err)
	}
	lo = ListOptions{
		Filters:    []Filter{{field: []string{"Brand"}, match: "f"}}, // tesla filtered out
		Sort:       Sort{primaryField: []string{"Color"}, primaryOrder: DESC},
		Pagination: Pagination{},
		Revision:   "",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 2)
	assert.Equal(r[0].(Car).Color, "red")
	assert.Equal(r[1].(Car).Color, "blue")

	// test pagination
	lo = ListOptions{
		Filters:    []Filter{},
		Sort:       Sort{primaryField: []string{"Color"}},
		Pagination: Pagination{pageSize: 2},
		Revision:   "",
	}
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 2)
	assert.Equal(r[0].(Car).Color, "black")
	assert.Equal(r[1].(Car).Color, "blue")
	lo.Pagination.page = 2
	r, err = l.ListByOptions(lo)
	if err != nil {
		t.Error(err)
	}
	assert.Len(r, 1)
	assert.Equal(r[0].(Car).Color, "red")

	err = l.Close()
	if err != nil {
		t.Error(err)
	}
}
