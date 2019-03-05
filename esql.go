package esql

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
)

//ServerAddr  It assumes that rawurl was received in an HTTP request.
//http://locahost:9200
var server *url.URL
var indexReg = regexp.MustCompile("[^0-9a-z+-_.]")
var defaultIndexSetting = F{
	"settings": F{
		"index": F{
			"number_of_shards":   1,
			"number_of_replicas": 1,
		},
	},
}

func init() {
	host := "http://localhost:9200"
	if v := os.Getenv("ELASTICSEARCH_HOST"); v != "" {
		host = v
	}
	_server, err := url.ParseRequestURI(host)
	if err != nil {
		panic("server address: " + err.Error())
	}
	http.DefaultClient.Timeout = 10 * time.Second
	server = _server
}

//NewElasticSearch   a convenient client for gloal
// concept: https://www.elastic.co/guide/en/elasticsearch/reference/6.2/_basic_concepts.html
// Cluster, Node, Index, Document, Shards & Replicas
// if index not set, it searchs all elasticsearch default
// https://localhost:9200/
func NewElasticSearch(indexs ...string) *ElasticSearch {
	return &ElasticSearch{indexs: strings.Join(indexs, ",")}
}

// DB allow to define yourself url
func DB(table string) *Client {
	var db Client
	db.hostDB = clone()
	db.method, db.hostDB.Path = "GET", path.Join(db.hostDB.Path, table)
	val := url.Values{}
	val.Set("timeout", "8s")
	db.queries = val
	db.search = F{}
	db.metrics = F{}
	db.groups = F{}
	db.aggregations = F{}
	return &db
}

// Response represents a boolean response sent back by the search egine
type Response struct {
	Acknowledged bool
	Error        interface{}
	Status       int
	Index        string
	Found        bool
}

type Setting interface {
	Append(F)
	Fields() []interface{}
}

//F <= Find
// convenicent tool for condtions; ideally, you just concentrate on Settings of Match.
// if you have multi Settings, should make F slice and call search APIs with it.
// Format  {"field": setting}
type F map[string]interface{}
type Not map[string]interface{}

//Fields make self as Settings
func (n Not) Fields() (arr []interface{}) {
	for k, v := range n {
		arr = append(arr, Not{k: v})
	}
	return
}

//Append copy arr's k/v struct to m
func (n Not) Append(s F) {
	for k, v := range s {
		n[k] = v
	}
}

//Fields make self as Settings
func (f F) Fields() (arr []interface{}) {
	for k, v := range f {
		arr = append(arr, F{k: v})
	}
	return
}

//Append copy arr's k/v struct to m
func (f F) Append(s F) {
	for k, v := range s {
		f[k] = v
	}
}

//ElasticSearch global DB
type ElasticSearch struct {
	indexs string
}

//DB a new db connection, concurrency with same index.
func (e ElasticSearch) DB() *Client {
	return DB(e.indexs)
}

// a new server for every Client instance
func clone() *url.URL {
	_url := *server
	return &_url
}

// if return true, the index is illegal
// https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
func checkIndexName(c *Client) bool {
	if indexReg.MatchString(c.hostDB.Path) {
		c.Error = fmt.Errorf("esql: The index name against index name limitations. Reset index with c.Table() firstly")
		return true
	}

	return false
}
