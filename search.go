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
	c.MakeQuery()
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

// In as Terms { "terms": { "field": [ "name1", "name2", "name3"], "other": interface }}
// F{ "field": [ "name1", "name2", "name3"], "other": interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-terms-query.html
func (c *Client) In(field string, values []interface{}, i Setting) *Client {
	return c.Terms(field, values, i)
}

//Missing as Null {"missing" : {"field" : "name"}}
// just put name of fields
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-exists-query.html#_literal_missing_literal_query
func (c *Client) Missing(i ...string) *Client {
	for _, v := range i {
		c.mustnot = append(c.mustnot, Not{"exists": v})
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

//StringQuery {"query_string" : { "query": "value string", "fields" : ["name1", "name2"], "other" : interface}}
// F{ "query": "value string", "fields" : ["name1", "name2"], "other" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-query-string-query.html#
func (c *Client) StringQuery(i ...F) *Client {
	c.must = append(c.must, F{"query_string": i})
	return c
}

//SimpleStringSelect {"simple_query_string" : { "query": "value string", "fields" : ["name1", "name2"], "other" : interface}}
// F{ "query": "value string", "fields" : ["name1", "name2"], "other" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-simple-query-string-query.html
func (c *Client) SimpleStringSelect(i ...F) *Client {
	c.must = append(c.must, F{"simple_query_string": i})
	return c
}

//Phrase as MatchPhrase {"match_phrase" : {"field" : interface}}
// F{ "field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-match-query-phrase.html
func (c *Client) Phrase(i ...Setting) *Client {
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
func (c *Client) Should(i ...F) *Client {
	c.should = append(c.should, c.reflect("match", i)...)
	return c
}

//Filter https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Filter(i ...F) *Client {
	c.filter = append(c.filter, c.reflect("term", i)...)
	return c
}

//Match as where {"match" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-match-query.html
func (c *Client) Match(i ...Setting) *Client {
	c.should = append(c.should, c.reflect("match", i)...)
	return c
}

//Multy as  MultiMatch { "multi_match": { "query":interface, "field": [ "name1", "name2", "name3"] }}
// F{"field": [ "name1", "name2", "name3" ], "query":interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-multi-match-query.html
func (c *Client) Multy(fields []string, s Setting) *Client {
	s.Append(F{"fields": fields})
	c.must = append(c.must, c.reflect("multi_match", []Setting{s})...)
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

// Terms as In { "terms": { "field": [ "name1", "name2", "name3"], "other": interface }}
// F{ "field": [ "name1", "name2", "name3"], "other": interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-terms-query.html
func (c *Client) Terms(field string, values []interface{}, i Setting) *Client {
	i.Append(F{field: values})
	c.filter = append(c.filter, c.reflect("terms", []Setting{i})...)
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
	return c.MakeQuery().exec(c.hostDB.String(), c.template)
}

// divide Setting into correct query
func (c *Client) reflect(types string, i interface{}) (arr []F) {
	tt := reflect.ValueOf(i)
	l := tt.Len()
	switch reflect.TypeOf(i).Elem().Name() {
	case "Setting":
		cons := i.([]Setting)
		for n := 0; n < l; n++ {
			not := tt.Index(n).Elem().Type().Name()
			for _, f := range cons[n].Fields() {
				if not == "Not" { //Note: must reflect on i(v),
					c.mustnot = append(c.mustnot, Not{types: f})
					continue
				}
				arr = append(arr, F{types: f})
			}
		}
	case "F":
		fs := i.([]F)
		for n := 0; n < l; n++ {
			for _, f := range fs[n].Fields() {
				arr = append(arr, F{types: f})
			}
		}
	case "Not":
		fs := i.([]Not)
		for n := 0; n < l; n++ {
			for _, f := range fs[n].Fields() {
				c.mustnot = append(c.mustnot, Not{types: f})
			}
		}
	}

	return
}

///////////////////todos//////////////////////
//Joins elasticsearch-join different with sql's action
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
func (c *Client) Joins(parentOrChild, on string, i F) *Client {
	if strings.Contains(parentOrChild, "parent") {
		i["parent_type"] = on
		c.joins = F{"has_parent": i}
		return c
	}

	i["type"] = on
	c.joins = F{"has_child": i}
	return c
}

//Group todo https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics.html
func (c *Client) Group(i F) *Client {
	return c
}

//Boosting todo https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-boosting-query.html
func (c *Client) Boosting(i F) *Client {
	return c
}

//Select todo https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-boosting-query.html
func (c *Client) Select(i F) *Client {
	return c
}

//Geo queries
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/geo-queries.html

//Request Body Search
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-request-body.html
// bitset store
// https://www.elastic.co/guide/cn/elasticsearch/guide/current/filter-caching.html
