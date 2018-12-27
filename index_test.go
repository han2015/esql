package esql_test

import (
	"testing"
	"time"
)

func TestParseStruct(t *testing.T) {
	type tweet struct {
		Name        string
		Age         int       `esql:"type:integer"`
		Joindate    time.Time `esql:"type:text;analyzer:english"`
		Description string
	}

	es.DB().Delete()
	ok, err := es.DB().IndexExists()
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Log(string(es.DB().ShowMapping().Response()))
		return
	}

	c := es.DB().AutoMapping(tweet{}).ShowMapping()
	if c.Error != nil {
		t.Error(err)
		return
	}

	t.Log(string(c.Response()))
}
