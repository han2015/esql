package esql

import (
	"net/url"
	"os"
	"path"
)

//ServerAddr  It assumes that rawurl was received in an HTTP request.
//http://locahost:9200
var server *url.URL

func init() {
	host := "http://locahost:9200"
	if v := os.Getenv("ELASTICSEARCH_HOST"); v != "" {
		host = v
	}
	_server, err := url.ParseRequestURI(host)
	if err != nil {
		panic("server address: " + err.Error())
	}

	server = _server
}

//NewElasticSearch   a convenient client for gloal
// concept: index = indexDB; type = table
// if indexDB not set, it searchs all elasticsearch
// https://localhost:9200/{indexDB}
func NewElasticSearch(db string) *ElasticSearch {
	return &ElasticSearch{indexDB: db}
}

// DB allow to define yourself url
func DB(uri string) *Client {
	var db Client
	db.hostDB = clone()
	db.hostDB.Path = path.Join(db.hostDB.Path, uri)
	val := url.Values{}
	val.Set("timeout", "5s")
	db.queries = val
	return &db
}

// Response represents a boolean response sent back by the search egine
type Response struct {
	Acknowledged bool
	Error        interface{}
	Status       int
	Index        string
}

//F <= Field
// convenicent tool for condtions; ideally, you just concentrate on conditions of Match.
// if you have multi conditions, should make F slice and call search APIs with it.
// Format  {"field": setting}
type F map[string]interface{}

//Append copy arr's k/v struct to m
func (f F) Append(arr ...F) {
	for _, v := range arr {
		for k, v := range v {
			f[k] = v
		}
	}
}

//Insert set arr's k/v struct under key field of m
func (f F) Insert(key string, arr ...F) {
	t := F{}
	for _, v := range arr {
		for k, v := range v {
			t[k] = v
		}
	}
	f[key] = t
}

//ElasticSearch global DB
type ElasticSearch struct {
	indexDB string
}

//DB a new db connection, concurrency with same index.
func (e ElasticSearch) DB() *Client {
	return DB(e.indexDB)
}

// a new server for every Client instance
func clone() *url.URL {
	_url := *server
	return &_url
}
