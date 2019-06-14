package esql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"reflect"
	"strings"
)

// CreateIndex initalize  i the setting of index
// {
//     "settings" : {
//         "number_of_shards" : 1
//     },
//     "mappings" : {
//         "_doc" : {
//             "properties" : {
//                 "field1" : { "type" : "text" }
//             }
//         }
//     }
// }
// https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/getting-started-create-index.html
// Advocate to create settings section ofindex in ElasticSearch directly, just setting mappings
// by AutoMapping. That means you'd better prepare a exsit index, instead use createIndex manaully.
func (c *Client) CreateIndex(i F) *Client {
	if checkIndexName(c) {
		return c
	}
	c.method = "PUT"
	data, _ := json.Marshal(i)
	return c.exec(c.hostDB.String(), string(data))
}

//Delete index
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-delete-index.html
func (c *Client) Delete() *Client {
	if checkIndexName(c) {
		return c
	}
	c.method = "DELETE"
	return c.exec(c.hostDB.String())
}

// IndexExists allows to check if the index exists or not.
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-exists.html
func (c *Client) IndexExists() (bool, error) {
	if checkIndexName(c) {
		return false, c.Error
	}

	_client := &http.Client{}
	resp, err := _client.Head(c.hostDB.String())
	if err != nil {
		c.Error = err
		return false, err
	}

	return resp.StatusCode == http.StatusOK, nil
}

//type:
//integer field is not analyzed
// string, text
//keyword field is not analyzed, as a single unit even if they contain multiple words
//date field is not analyzed too

//ShowMapping show index's mapping
func (c *Client) ShowMapping() *Client {
	if checkIndexName(c) {
		return c
	}
	c.hostDB.Path = path.Join(c.hostDB.Path, "_mapping")
	return c.exec(c.hostDB.String())
}

/*
https://www.elastic.co/guide/en/elasticsearch/painless/6.5/painless-context-examples.html
types of elasticsearch

type golang struct {
	First string
	Last  string
}
type as3 struct {
	Name string
}

type mysql struct {
	Number int
	Name   string
}
type employee struct {
	// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/object.html
	Golang golang `esql:"dynamic:strict;enabled:false"`

	// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-nested-query.html
	As3 []as3 `esql:"type:nested"`

	//https://www.elastic.co/guide/en/elasticsearch/reference/6.5/array.html
	Mysql       []mysql
	Gender      string    `esql:"-"`
	Age         int       `esql:"type:integer"`
	Location    []float64  `esql:"type:geo_point"`
	JoinDate    time.Time
	//fields config: https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
	Description string `esql:"type:text;analyzer:english;ignore_above:500;fields:name,type,analyzer"`
}
*/

//AutoMapping set up index's mapping by a assigned struct
//struct or *struct directly and mapping json directly
//https://www.elastic.co/guide/en/elasticsearch/reference/6.5/mapping.html
func (c *Client) AutoMapping(i interface{}) *Client {
	defer func() {
		if r := recover(); r != nil {
			c.Error = fmt.Errorf("panic error: %v", r)
		}
	}()
	if checkIndexName(c) {
		return c
	}

	ok, err := c.IndexExists()
	if err != nil {
		return c
	}

	if !ok && c.CreateIndex(defaultIndexSetting).Error != nil {
		return c
	}

	var mapStr string
	t := reflect.TypeOf(i)
	switch t.Kind() {
	case reflect.Ptr:
		t = t.Elem()
		fallthrough
	case reflect.Struct:
		props := parseStruct(t)
		str, _ := json.Marshal(F{"properties": props})
		mapStr = fmt.Sprintf(`%s`, str)
	default:
		c.Error = fmt.Errorf("only support struct or &struct")
		return c
	}

	c.method = "PUT"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_mapping/_doc")
	c.template = mapStr
	return c.exec(c.hostDB.String(), mapStr)
}

func parseStruct(t reflect.Type) F {
	maps := F{}
	for i := 0; i < t.NumField(); i++ {
		fd := t.Field(i)
		ft := fd.Type.Kind()
		tags := fd.Tag.Get("esql")
		if tags == "-" {
			maps[fd.Name] = F{"enabled": false}
			continue
		}

		_set := parseTags(tags)
		if fd.Type.Name() == "Duration" || strings.Contains(fd.Type.Name(), "Time") {
			if _set["type"] == nil {
				_set["type"] = "date"
			}
			maps[fd.Name] = _set
			continue
		}

		switch ft {
		case reflect.Map:
			if _set["type"] == nil {
				_set["type"] = "object"
			}
		case reflect.Ptr:
			fd.Type = fd.Type.Elem()
			fallthrough
		case reflect.Struct:
			_set["properties"] = parseStruct(fd.Type)
		case reflect.Slice:
			if _set["type"] == nil {
				//here will not set it's properties, caused es only index nested array!
				_set["type"] = "object"
			} else if _set["type"] == "geo_point" {
				_set["type"] = "geo_point"
			} else {
				//should indicate exactly to nested
				_set["properties"] = parseStruct(fd.Type.Elem())
			}
		case reflect.String:
			if _set["type"] == nil {
				_set["type"] = "text"
			}
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int8, reflect.Uint, reflect.Uint32, reflect.Uint64:
			if _set["type"] == nil {
				_set["type"] = "integer"
			}
		case reflect.Float32, reflect.Float64:
			if _set["type"] == nil {
				_set["type"] = "double"
			}
		case reflect.Bool:
			if _set["type"] == nil {
				_set["type"] = "boolean"
			}
		default:
			if _set["type"] == nil {
				_set["type"] = "keyword"
			}
		}
		maps[fd.Name] = _set
	}

	return maps
}

func parseTags(str string) F {
	_m := F{}
	if str == "" {
		return _m
	}
	// fields:name,type,analyzer"
	for _, v := range strings.Split(str, ";") {
		a := strings.Split(v, ":")
		if a[0] == "fields" {
			_n := F{}
			fls := strings.Split(a[1], ",")
			_n["type"] = fls[1]
			if fls[1] == "text" {
				_n["analyzer"] = fls[2]
			}
			_m["fields"] = F{fls[0]: _n}
			continue
		}
		_m[a[0]] = a[1]
	}
	return _m
}
