package esql_test

import (
	"os"
	"testing"

	"github.com/han2015/esql"
)

var es *esql.ElasticSearch

func TestMain(m *testing.M) {
	es = esql.NewElasticSearch("esql")
	os.Exit(m.Run())
}
