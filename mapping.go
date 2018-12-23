package esql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"reflect"
	"strings"
)

// IndexExists allows to check if the index exists or not.
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-exists.html
func (c *Client) IndexExists() (bool, error) {
	_client := &http.Client{}
	resp, err := _client.Head(c.hostDB.String())
	if err != nil {
		return false, err
	}

	return resp.StatusCode == http.StatusOK, nil
}

//type:
//integer field is not analyzed
// string, text
//keyword field is not analyzed, as a single unit even if they contain multiple words
//date field is not analyzed too

// curl -X PUT "localhost:9200/shakespeare" -H 'Content-Type: application/json' -d'
// {
//  "mappings": {
//   "doc": {
//    "properties": {
//     "speaker": {"type": "keyword"},
//     "play_name": {"type": "keyword"},
//     "line_id": {"type": "integer"},
//     "speech_number": {"type": "integer"}
//    }
//   }
//  }
// }
// '

//curl -X GET 'localhost:9200/shakespeare/_mapping'
//curl -X GET "localhost:9200/gb/_mapping/tweet"
func (c *Client) ShowMapping(table string) *Client {
	c.method = "GET"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_mapping", table)
	return c.exec(c.hostDB.String())
}

//AutoMapping set up mapping for a table
//struct or *struct directly
//mapping json directly
//todo: 1) how to set 倒排索引
//      2) support mapping structs AutoMapping(&a{},&bb{},&cd{},&e{})
func (c *Client) AutoMapping(i interface{}) *Client {
	var mapStr string
	t := reflect.TypeOf(i)
	switch t.Kind() {
	case reflect.Ptr:
		t = t.Elem()
		props := parseStruct(t)
		str, _ := json.Marshal(F{"properties": props})
		mapStr = fmt.Sprintf(`{"mappings":{"%s":%s}}`, t.Name(), str)
	case reflect.Struct:
		parseStruct(t)
		props := parseStruct(t)
		str, _ := json.Marshal(F{"properties": props})
		mapStr = fmt.Sprintf(`{"mappings":{"%s": %s}}`, t.Name(), str)
	case reflect.String:
		mapStr = i.(string)
	}
	c.method = "PUT"

	return c.exec(c.hostDB.String(), mapStr)
}

// type Emplloy struct {
// 	Name        string
// 	Age         int       `esql:"type:integer"`
// 	JoinDate    time.Time `esql:"type:text;index:analyzed;analyzer:english"`
// 	Description string
// }
func parseStruct(t reflect.Type) F {
	maps := F{}
	for i := 0; i < t.NumField(); i++ {
		fd := t.Field(i)
		ft := fd.Type.Kind()

		if fd.Type.Name() == "Duration" || strings.Contains(fd.Type.Name(), "Time") {
		} else if ft == reflect.Struct {
			maps[fd.Name] = parseStruct(fd.Type)
			continue
		} else if ft == reflect.Ptr {
			maps[fd.Name] = parseStruct(fd.Type.Elem())
			continue
		}

		tag := fd.Tag.Get("esql")
		if tag == "" {
			continue
		}

		_m := F{}
		for _, v := range strings.Split(tag, ";") {
			a := strings.Split(v, ":")
			_m[a[0]] = a[1]
		}
		maps[fd.Name] = _m
	}

	return maps
}
