//Package esql  search api implements
package esql

import (
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"strings"
)

// Find  makes search querry, then start query, and scan the result to i.
// i must be the reflect.Ptr.  e.g &F, &struct{}
// this should be the last chain when you do any search.
// es.DB().Where(F{}).Match(F{}).Not(F{}).Or(F{}).Between(F{}).In(F{}).Range(F{}).Term(F{}).Order(F{}).Limit(5).Find(&Response{})
func (c *Client) Find(i interface{}) *Client {
	c.Serialize()
	c.hostDB.Path = path.Join(c.hostDB.Path, "_search")
	if i == nil {
		return c.exec(c.hostDB.String(), c.template)
	}

	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		c.Error = fmt.Errorf("source should be a reflect.Ptr")
		return c
	}

	if err := c.exec(c.hostDB.String(), c.template).Error; err != nil {
		return c
	}

	c.Error = json.Unmarshal(c.response, i)
	return c
}

//Dismax F{"tie_breaker" : 1, "boost" : 1}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-dis-max-query.html
func (c *Client) Dismax(i Setting) *Client {
	c.dismax = i.(F)
	return c
}

//Bool  just add some speciall setting for bool query, exclude must must_not should and filter.
// F{"minimum_should_match" : 1, "boost" : 1.0 }
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Bool(i Setting) *Client {
	c.bools = i.(F)
	return c
}

// func (c *Client) parseParams(i ...string) *Client {
// 	for _, v := range i {
// 		c.mustnot = append(c.mustnot, F{"exists": v})
// 	}
// 	return c
// }

// Where  as must {"match" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-match-query.html
func (c *Client) Where(i ...F) *Client {
	c.must = append(c.must, c.reflect("match", i)...)
	return c
}

//Not same as MustNot  https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Not(i ...Not) *Client {
	c.reflect("match", i)
	return c
}

//Or same as should https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Or(i ...F) *Client {
	c.should = append(c.should, c.reflect("match", i)...)
	return c
}

// In as Terms { "terms": { "field": [ "name1", "name2", "name3"]}}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-terms-query.html
func (c *Client) In(i ...Setting) *Client {
	return c.Terms(i...)
}

//Missing as Null {"missing" : {"field" : "name"}}
// just put name of fields
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-exists-query.html#_literal_missing_literal_query
func (c *Client) Missing(i ...string) *Client {
	for _, v := range i {
		c.mustnot = append(c.mustnot, Not{"exists": F{"field": v}})
	}
	return c
}

//NotNil as Exists {"exists" : {"field" : "name"}}
// just put name of fields
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-exists-query.html#query-dsl-exists-query
func (c *Client) NotNil(i ...string) *Client {
	for _, v := range i {
		c.filter = append(c.filter, F{"exists": F{"field": v}})
	}
	return c
}

// Between same as Range { "range": { "field": interface}}
// F{ "field": interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-range-query.html
func (c *Client) Between(i ...Setting) *Client {
	c.filter = append(c.filter, c.reflect("range", i)...)
	return c
}

//Order  https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-request-sort.html
// [
// 	{ "post_date" : {"order" : "asc"}},
// 	"user",
// 	{ "name" : "desc" },
// 	{ "age" : "desc" },
// 	"_score"
// ]
func (c *Client) Order(i ...F) *Client {
	arr := []interface{}{}
	for _, v := range i {
		arr = append(arr, v.Fields()...)
	}
	c.search["sort"] = arr
	return c
}

//Limit from to
// len(n)=1 0->n
// len(n)>1 n[0]->n[1]
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-request-from-size.html
func (c *Client) Limit(n ...int) *Client {
	if l := len(n); l == 1 {
		c.search["size"] = n[0]
	} else if l > 1 {
		c.search["from"], c.search["size"] = n[0], n[1]
	}
	return c
}

//StringQuery Perform the query on all fields detected in the mapping that can be queried. Will be used by default when the _all field is disabled and no default_field is specified (either in the index settings or in the request body) and no fields are specified.
// {"query_string" : { "query": "value string", "fields" : ["name1", "name2"], "other" : interface}}
// F{ "query": "value string", "fields" : ["name1", "name2"], "other" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-query-string-query.html#
func (c *Client) StringQuery(i ...F) *Client {
	c.must = append(c.must, c.reflect("query_string", i)...)
	return c
}

//SimpleStringSelect Perform the query on all fields detected in the mapping that can be queried. Will be used by default when the _all field is disabled and no default_field is specified (either in the index settings or in the request body) and no fields are specified.
// {"simple_query_string" : { "query": "value string", "fields" : ["name1", "name2"], "other" : interface}}
// F{ "query": "value string", "fields" : ["name1", "name2"], "other" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-simple-query-string-query.html
func (c *Client) SimpleStringSelect(i ...F) *Client {
	c.must = append(c.must, c.reflect("simple_query_string", i)...)
	return c
}

//Phrase as MatchPhrase {"match_phrase" : {"field" : interface}}
// F{ "field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-match-query-phrase.html
func (c *Client) Phrase(i ...F) *Client {
	c.must = append(c.must, c.reflect("match_phrase", i)...)
	return c
}

//Must as Where:  the clause (query) must appear in matching documents and will contribute to the score.
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Must(i ...F) *Client {
	c.must = append(c.must, c.reflect("match", i)...)
	return c
}

//MustNot as not  https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) MustNot(i ...Not) *Client {
	c.reflect("match", i)
	return c
}

//Should as or https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
// types e.g(match,term,terms,range,fuzzy...)
func (c *Client) Should(i ...F) *Client {
	c.should = append(c.should, c.reflect("match", i)...)
	return c
}

//Filter https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Filter(i ...F) *Client {
	c.filter = append(c.filter, c.reflect("term", i)...)
	return c
}

//Match  {"match" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-match-query.html
func (c *Client) Match(i ...Setting) *Client {
	c.must = append(c.must, c.reflect("match", i)...)
	return c
}

//Multy as  MultiMatch { "multi_match": { "query":interface, "field": [ "name1", "name2", "name3"] }}
// F{"field": [ "name1", "name2", "name3" ], "query":interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-multi-match-query.html
func (c *Client) Multy(i ...Setting) *Client {
	c.must = append(c.must, c.reflect("multi_match", i)...)
	return c
}

// Range { "range": { "field": interface}}
// F{ "field": interface}
// gt: > （greater than）
// lt: < （less than）
// gte: >= (greater than or equal to）
// lte: <=（less than or equal to）
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-range-query.html
func (c *Client) Range(i ...Setting) *Client {
	c.filter = append(c.filter, c.reflect("range", i)...)
	return c
}

// Term { "term": { "field": interface}}
// F{ "field": interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-term-query.html
func (c *Client) Term(i ...Setting) *Client {
	c.filter = append(c.filter, c.reflect("term", i)...)
	return c
}

// Terms as In { "terms": { "field": [ "name1", "name2", "name3"]}}
// F{ "field": [ "name1", "name2", "name3"]}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-terms-query.html
func (c *Client) Terms(i ...Setting) *Client {
	c.filter = append(c.filter, c.reflect("terms", i)...)
	return c
}

//Regexp {"regexp" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-regexp-query.html#query-dsl-regexp-query
func (c *Client) Regexp(i ...Setting) *Client {
	c.filter = append(c.filter, c.reflect("regexp", i)...)
	return c
}

//Fuzzy {"fuzzy" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-fuzzy-query.html
func (c *Client) Fuzzy(i ...Setting) *Client {
	c.filter = append(c.filter, c.reflect("fuzzy", i)...)
	return c
}

//Wildcard {"wildcard" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-wildcard-query.html
func (c *Client) Wildcard(i ...Setting) *Client {
	c.filter = append(c.filter, c.reflect("wildcard", i)...)
	return c
}

//GeoBox https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-geo-bounding-box-query.html
// e.g  GeoBox("address.location", 40.73, -74.1, 40.01, -71.12)
func (c *Client) GeoBox(fieldName string, top, left, bottom, right float64) *Client {
	geo := F{}
	geo[fieldName] = F{"top": top, "left": left, "bottom": bottom, "right": right}
	c.filter = append(c.filter, F{"geo_bounding_box": geo})
	return c
}

//GeoDistance https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-geo-distance-query.html
//dis-units: https://www.elastic.co/guide/en/elasticsearch/reference/6.5/common-options.html#distance-units
// e.g  GeoDistance("address.location", "200m" 40.73, -74.1)
func (c *Client) GeoDistance(fieldName, dis string, lat, lon float64) *Client {
	geo := F{}
	geo["geo_distance"] = F{"distance": dis, fieldName: F{"lat": lat, "lon": lon}}
	c.filter = append(c.filter, geo)
	return c
}

// Scroll search
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-request-scroll.html
// size The 'size' parameter allows you to configure the maximum number of hits to be returned with each batch of results.
// expires: https://www.elastic.co/guide/en/elasticsearch/reference/6.5/common-options.html#time-units
func (c *Client) Scroll(size int, expires string) *Client {
	c.search["size"] = size
	c.queries.Set("scroll", expires)
	return c
}

// GetScroll The result from the above request includes a _scroll_id, which should be passed to the scroll API in order to retrieve the next batch of results.
// it starts a query directly, should use alone.
func (c *Client) GetScroll(scrollID, expires string) *Client {
	if expires == "" {
		expires = "1m"
	}
	data, _ := json.Marshal(F{"scroll": expires, "scroll_id": scrollID})
	host := strings.Split(c.hostDB.String(), "/"+c.hostDB.Path)[0]
	return c.exec(host+"/_search/scroll", string(data))
}

//Joins elasticsearch-join different with sql's action
// 'i' is a setting of Joins, should only one, don't use it as conditions
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-nested-query.html
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-has-child-query.html
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-has-parent-query.html
// {
//     "query": {
//         "has_child" : {
//             "type" : "blog_tag",
//             "score_mode" : "min",
//             "query" : {
//                 "term" : {
//                     "tag" : "something"
//                 }
//             }
//         }
//     }
// }
func (c *Client) Joins(types, on string, i ...F) *Client {
	_set := F{}
	for _, v := range i {
		_set.Append(v)
	}
	switch types {
	case "nested":
		_set["path"] = on
	case "has_parent":
		_set["parent_type"] = on
	case "has_child":
		_set["type"] = on
	}
	c.joinType, c.joins = types, _set
	return c
}

// MatchAll { "match_all": {}}
// F{"field": interface} or nil
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-match-all-query.html
func (c *Client) MatchAll(i F) *Client {
	str := `{ "match_all": {}}`
	if len(i) > 0 {
		data, _ := json.Marshal(F{"match_all": i})
		str = string(data)
	}

	c.exec(c.hostDB.String(), str)
	return c
}

// ValidateQuery i to get doc type
func (c *Client) ValidateQuery() *Client {
	c.hostDB.Path = path.Join(c.hostDB.Path, "_validate/query?explain")
	return c.Serialize().exec(c.hostDB.String(), c.template)
}

// divide Setting into correct query
func (c *Client) reflect(types string, i interface{}) (arr []F) {
	tt := reflect.ValueOf(i)
	l := tt.Len()
	switch reflect.TypeOf(i).Elem().Name() {
	case "Setting":
		cons := i.([]Setting)
		for n := 0; n < l; n++ {
			if tt.Index(n).Elem().Type().Name() == "Not" {
				c.mustnot = append(c.mustnot, Not{types: cons[n]})
				continue
			}
			arr = append(arr, F{types: cons[n]})
		}
	case "F":
		fs := i.([]F)
		for n := 0; n < l; n++ {
			arr = append(arr, F{types: fs[n]})
		}
	case "Not":
		fs := i.([]Not)
		for n := 0; n < l; n++ {
			c.mustnot = append(c.mustnot, Not{types: fs[n]})
		}
	}

	return
}
