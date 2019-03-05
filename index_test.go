package esql_test

import (
	"testing"

	"github.com/han2015/esql"
)

func TestCreateIndex(t *testing.T) {
	set := esql.F{
		"settings": esql.F{
			"number_of_shards": 1,
		},
		"mappings": esql.F{
			"_doc": esql.F{
				"properties": esql.F{
					"Name": esql.F{"type": "keyword"},
				},
			},
		},
	}

	//switch sql to indextest
	if err := es.DB().Table("indextest").CreateIndex(set).Error; err != nil {
		t.Fatal(err)
	}

	if ok, err := es.DB().Table("indextest").IndexExists(); !ok || err != nil {
		t.Fatal("created, but not existed")
	}
	resp := es.DB().Table("indextest").ShowMapping().Response(nil)
	t.Log(string(resp))

	es.DB().Table("indextest").Delete()
	if ok, err := es.DB().Table("indextest").IndexExists(); ok || err != nil {
		t.Fatal("deleted, but still existed")
	}
}
