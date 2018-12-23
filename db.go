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
	//todo: currently not use; if set table, should match on it
	//if not, reflect struct
	table string
	// the body of http resonse
	response []byte
	source   interface{}

	joins  F
	order  F // same lever with query
	limit  F // same lever with query
	dismax F // similar with boolQuery
	bools  F // boolQuery

	must       []F //where
	mustnot    []F //not
	should     []F //or
	filter     []F //where
	match      []F //where (full text)
	ranges     []F //exact search
	terms      []F //exact search in
	filterbool F   //where

	Error    error
	queries  url.Values //query in path
	template string     //final json data
}

//Path reset entire path of hostDB
// https://localhost:9200/{path}
func (c *Client) Path(path string) *Client {
	c.table = ""
	c.hostDB.Path = path
	return c
}

//Table update doc type, auto put in query path when searching
// if not set, default search on indexDB. (indexDB refer NewElasticSearch)
// 'table' maybe to the indexDB, all depend on what the setting of indexDB
// https://localhost:9200/{indexDB}/{table}
func (c *Client) Table(table string) *Client {
	c.table = table
	return c
}

//Exec execute your prepared data directly. auto put url in query path when executing
// if not set, default exec on indexDB. (indexDB refer NewElasticSearch).
// 'url' maybe to the indexDB, all depend on what the setting of indexDB.
// data  json data.
// https://localhost:9200/{indexDB}/{url}
func (c *Client) Exec(url, data string) *Client {
	c.hostDB.Path = path.Join(c.hostDB.Path, url)
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
	c.source = i
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
	c.method = "GET"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_cat/indices?v")
	return c.exec(c.hostDB.String())
}

//MakeQuery to prepare request body
func (c *Client) MakeQuery() *Client {
	if c.must == nil {
		c.Error = fmt.Errorf("make-query process more than once")
		return c
	}

	final := F{}
	c.filter = append(c.filter, c.ranges...)
	c.filter = append(c.filter, c.terms...)
	if c.filterbool["fbool"] != nil {
		c.filter = append(c.filter, F{"bool": c.filterbool})
	}

	if len(c.must)+len(c.mustnot)+len(c.should)+len(c.bools)+len(c.dismax) > 0 || len(c.match) > 1 {
		_m := F{}
		c.must = append(c.must, c.match...)
		if len(c.must) > 0 {
			_m["must"] = c.must
		}
		if len(c.mustnot) > 0 {
			_m["mustnot"] = c.mustnot
		}
		if len(c.should) > 0 {
			_m["should"] = c.should
		}
		if len(c.filter) > 0 {
			_m["filter"] = c.filter
		}
		//https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-dis-max-query.html
		if len(c.dismax) > 0 {
			c.dismax["bool"] = _m
			final["query"] = F{"dis_max": c.dismax}
		} else {
			final["query"] = F{"bool": _m}
		}
	} else if len(c.filter)+len(c.filterbool) > 1 {
		final["constant_score"] = F{"filter": c.filter}
	} else if len(c.ranges) == 1 {
		final["query"] = c.ranges[0]
	} else if len(c.terms) == 1 {
		final["query"] = c.terms[0]
	} else if len(c.match) == 1 {
		final["query"] = c.match[0]
	}

	if len(c.joins) > 0 {
		c.joins["query"] = final
		f := F{"query": c.joins, "sort": c.order, "from": c.limit["from"], "size": c.limit["size"]}
		if c.source != nil {
			f["_source"] = c.source
		}
		data, err := json.Marshal(f)
		c.template, c.Error = string(data), err
		c.clear()
		return c
	}

	final["sort"], final["from"], final["size"] = c.order, c.limit["from"], c.limit["size"]
	if c.source != nil {
		final["_source"] = c.source
	}

	data, err := json.Marshal(final)
	c.template, c.Error = string(data), err
	//clear memory, that also means the conditions of client have been locked.
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
	c.order, c.limit, c.dismax, c.bools, c.filterbool, c.joins = nil, nil, nil, nil, nil, nil
	c.must, c.mustnot, c.should, c.filter, c.match, c.ranges, c.terms = nil, nil, nil, nil, nil, nil, nil
	return c
}
