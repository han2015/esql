package esql_test

import (
	"os"
	"testing"
	"time"

	"github.com/han2015/esql"
)

var es *esql.ElasticSearch

func TestMain(m *testing.M) {
	es = esql.NewElasticSearch("esql")
	if err := es.DB().AutoMapping(employee{}).Error; err != nil {
		panic(err)
	}

	// resp := es.DB().ShowMapping().Response(nil)
	// fmt.Println(string(resp))

	os.Exit(m.Run())
}

type golang struct {
	First string
	Last  string `esql:"type:text"`
}
type as3 struct {
	Name string `esql:"type:keyword"`
}

type mysql struct {
	Number      int `esql:"type:keyword"`
	Level       int
	Name        string
	Description string
}

type employee struct {
	Name   string
	Number int `esql:"type:integer"`

	Gender      string `esql:"-"`
	Enable      golang `esql:"enabled:false"`
	Index       string `esql:"type:text;index:false"`
	Golang      golang `esql:"dynamic:strict"`
	As3         []as3  `esql:"type:nested"`
	Mysql       []mysql
	Age         int `esql:"type:integer"`
	JoinDate    time.Time
	Description string `esql:"type:keyword;ignore_above:500"`
}
