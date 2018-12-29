package esql_test

import (
	"testing"
	"time"
)

func TestAutoMapping(t *testing.T) {
	db := es.DB().Table("fuck")
	if err := db.AutoMapping(employee{}).Error; err != nil {
		t.Fatal(err)
	}
	t.Log(db.Template())
	resp := es.DB().Table("fuck").ShowMapping().Response()
	t.Log(string(resp))
}

type golang struct {
	First string
	Last  string `esql:"type:text"`
}
type as3 struct {
	Name string `esql:"type:keyword"`
}

type mysql struct {
	Number int `esql:"type:keyword"`
	Name   string
}

type employee struct {
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
