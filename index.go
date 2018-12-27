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

/* news research:
1)put have to exact doc id
2)post used to auto create new doc
*/

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

//AutoMapping set up index's mapping by a assigned struct
//struct or *struct directly and mapping json directly
//https://www.elastic.co/guide/en/elasticsearch/reference/6.5/mapping.html
//todos: 1) how to set 倒排索引
//       2) support mapping structs AutoMapping(&a{},&bb{},&cd{},&e{})
//		 3) dyncmic https://www.elastic.co/guide/cn/elasticsearch/guide/current/dynamic-mapping.html
// 		 4) support slice https://www.elastic.co/guide/cn/elasticsearch/guide/current/complex-core-fields.html#object-arrays
func (c *Client) AutoMapping(i interface{}) *Client {
	if checkIndexName(c) {
		return c
	}

	ok, err := c.IndexExists()
	if err != nil {
		return c
	}

	if !ok {
		if err := c.CreateIndex(F{
			"settings": F{
				"index": F{
					"number_of_shards":   5,
					"number_of_replicas": 1,
				},
			},
		}).Error; err != nil {
			return c
		}
	}

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
	c.hostDB.Path = path.Join(c.hostDB.Path, "_mapping/_doc")
	return c.exec(c.hostDB.String(), mapStr)
}

// type Emplloy struct {
// 	Name        string
// 	Age         int       `esql:"type:integer"`
// 	JoinDate    time.Time `esql:"type:text;index:analyzed;analyzer:english;ignore_above:500"`
// 	Description string
// }
func parseStruct(t reflect.Type) F {
	maps := F{}
	for i := 0; i < t.NumField(); i++ {
		fd := t.Field(i)
		ft := fd.Type.Kind()

		if fd.Type.Name() == "Duration" || strings.Contains(fd.Type.Name(), "Time") {
		} else if ft == reflect.Struct {
			// 需要重新考虑结构解析
			//bug! https://www.elastic.co/guide/cn/elasticsearch/guide/current/complex-core-fields.html#_%E5%86%85%E9%83%A8%E5%AF%B9%E8%B1%A1%E7%9A%84%E6%98%A0%E5%B0%84
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
