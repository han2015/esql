package esql

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
)

//Client the instance of request.
// you should use the tool esql.NewElasticSearch().DB() or esql.DB()
type Client struct {
	//hostDB format: server/dbname
	hostDB *url.URL
	// http head method
	method string
	// the body of http resonse
	response []byte
	//all Settings
	search F

	joins  F
	dismax F // similar with boolQuery, but is parent of bool
	bools  F // boolQuery

	must    []F   //where have to assigned
	should  []F   //or and where default match
	filter  []F   //where
	mustnot []Not //not

	Error    error
	queries  url.Values //query in path
	template string     //final json data
}

//Table  to reassign a new index for request.
// if you initialize elastic with many index, (e.g. blog_*,author,product), now you  temporarily just want
// a search on product, then use Table to reset it.
// or options with index and document.
// https://localhost:9200/{table}
func (c *Client) Table(table string) *Client {
	c.hostDB.Path = table
	return c
}

//Exec execute your prepared data directly. auto put url in query path when executing
// if not set, default exec on indexDB. (indexDB refer NewElasticSearch).
// data  json data.
// https://localhost:9200/index
func (c *Client) Exec(data string) *Client {
	c.clear()
	return c.exec(c.hostDB.String(), data)
}

//Source Allows to control how the _source field is returned with every hit.
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-request-source-filtering.html
// i = false
// i = "obj.*"
// i = [ "obj1.*", "obj2.*" ]
// i = F{ "includes": [ "obj1.*", "obj2.*" ], "excludes": [ "*.description" ]}
func (c *Client) Source(i interface{}) *Client {
	c.search["_source"] = i
	return c
}

// Timeout  10, 10ms ; or 5s & 5000 （5seconds）
func (c *Client) Timeout(out string) *Client {
	c.queries.Add("timeout", out)
	return c
}

//Method update http method
func (c *Client) Method(method string) *Client {
	c.method = method
	return c
}

//Template to get finnal query string
func (c *Client) Template() string {
	return c.template
}

//Response return db http response
func (c *Client) Response() []byte {
	return c.response
}

//Indices curl -X GET 'localhost:9200/_cat/indices?v'
func (c *Client) Indices() *Client {
	c.hostDB.Path = path.Join(c.hostDB.Path, "_cat/indices?v")
	return c.exec(c.hostDB.String())
}

//MakeQuery prepares query body
func (c *Client) MakeQuery() *Client {
	if c.template != "" {
		c.Error = fmt.Errorf("make-query process more than once")
		return c
	}

	_bool := F{}
	if len(c.must) > 0 {
		_bool["must"] = c.must
	}

	if len(c.should) > 0 {
		_bool["should"] = c.should
	}

	if len(c.mustnot) > 0 {
		_bool["mustnot"] = c.mustnot
	}
	if len(c.filter) > 0 {
		_bool["filter"] = c.filter
	}

	//https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-dis-max-query.html
	if len(c.dismax) > 0 {
		c.dismax["queries"] = F{"bool": _bool}
		c.search["query"] = F{"dis_max": c.dismax}
	} else {
		c.search["query"] = F{"bool": _bool}
	}

	data, err := json.Marshal(c.search)
	c.template, c.Error = string(data), err
	//clear memory, that also means the Settings of client have been locked.
	c.clear()
	return c
}

func (c *Client) exec(uri string, data ...string) *Client {
	if c.Error != nil {
		return c
	}

	if len(data) == 0 {
		data = []string{""}
	}

	req, err := http.NewRequest(c.method, uri, strings.NewReader(data[0]))
	if err != nil {
		c.Error = err
		return c
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		c.Error = err
		return c
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.Error = err
		return c
	}

	var errs Error
	json.Unmarshal(body, &errs)
	if errs.Status >= http.StatusBadRequest {
		c.Error = fmt.Errorf("%s", body)
		return c
	}

	c.response = body
	return c
}

func (c *Client) clear() *Client {
	c.dismax, c.bools, c.joins = nil, nil, nil
	c.must, c.mustnot, c.should, c.filter = nil, nil, nil, nil
	return c
}
