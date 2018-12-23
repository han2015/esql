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
	c.method = "GET"
	c.hostDB.Path = path.Join(c.hostDB.Path, c.table, "_search")
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
func (c *Client) Dismax(i F) *Client {
	c.dismax = i
	return c
}

//Bool  just add some speciall setting for bool query, exclude must must_not should and filter.
// F{"minimum_should_match" : 1, "boost" : 1.0 }
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Bool(i F) *Client {
	c.bools = i
	return c
}

// Where  as Match {"match" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-match-query.html
func (c *Client) Where(i ...F) *Client {
	for _, v := range i {
		c.match = append(c.match, F{"match": v})
	}
	return c
}

//Not same as MustNot  https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Not(i ...F) *Client {
	c.mustnot = append(c.mustnot, i...)
	return c
}

//Or same as should https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Or(i ...F) *Client {
	c.should = append(c.should, i...)
	return c
}

// In as Terms { "terms": { "field": [ "name1", "name2", "name3"], "other": interface }}
// F{ "field": [ "name1", "name2", "name3"], "other": interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-terms-query.html
func (c *Client) In(field string, values []interface{}, i ...F) *Client {
	return c.Terms(field, values, i...)
}

//Null as Missing {"missing" : {"field" : "name"}}
// just put name of fields
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-exists-query.html#_literal_missing_literal_query
func (c *Client) Null(i ...string) *Client {
	for _, v := range i {
		c.mustnot = append(c.mustnot, F{"exists": v})
	}
	return c
}

//NotNil as Exists {"exists" : {"field" : "name"}}
// just put name of fields
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-exists-query.html#query-dsl-exists-query
func (c *Client) NotNil(i ...string) *Client {
	for _, v := range i {
		c.match = append(c.match, F{"exists": v})
	}
	return c
}

// Between same as Range { "range": { "field": interface}}
// F{ "field": interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-range-query.html
func (c *Client) Between(i ...F) *Client {
	for _, v := range i {
		c.ranges = append(c.ranges, F{"range": v})
	}
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
	c.order = F{"sort": i}
	return c
}

//Limit from to
// default 0->10
// len(n)=1 0->n
// len(n)>1 n[0]->n[1]
func (c *Client) Limit(n ...int) *Client {
	l := len(n)
	c.limit = F{"from": 0, "size": 10}
	if l == 1 {
		c.limit["size"] = n[0]
	} else if l > 1 {
		c.limit["size"], c.limit["size"] = n[0], n[1]
	}
	return c
}

//SelectString as StringQuery {"query_string" : { "query": "value string", "fields" : ["name1", "name2"], "other" : interface}}
// F{ "query": "value string", "fields" : ["name1", "name2"], "other" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-query-string-query.html#
func (c *Client) SelectString(i F) *Client {
	c.match = append(c.match, F{"query_string": i})
	return c
}

//SimpleSelectString as StringSimpleQuery {"simple_query_string" : { "query": "value string", "fields" : ["name1", "name2"], "other" : interface}}
// F{ "query": "value string", "fields" : ["name1", "name2"], "other" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-simple-query-string-query.html
func (c *Client) SimpleSelectString(i F) *Client {
	c.match = append(c.match, F{"simple_query_string": i})
	return c
}

//Phrase as MatchPhrase {"match_phrase" : {"field" : interface}}
// F{ "field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-match-query-phrase.html
func (c *Client) Phrase(i F) *Client {
	c.match = append(c.match, F{"match_phrase": i})
	return c
}

//Must as where the clause (query) must appear in matching documents and will contribute to the score.
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Must(i ...F) *Client {
	c.must = append(c.must, i...)
	return c
}

//MustNot as not  https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) MustNot(i ...F) *Client {
	c.mustnot = append(c.mustnot, i...)
	return c
}

//Should as or https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Should(i ...F) *Client {
	c.should = append(c.should, i...)
	return c
}

//Filter https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) Filter(i ...F) *Client {
	c.filter = append(c.filter, i...)
	return c
}

// "filter" : {
// 	"bool" : {
// 	  "must" : [
// 		 { "term" : {"price" : 20}},
// 		 { "term" : {"productID" : "XHDK-A-1293-#fJ3"}}
// 	  ],
// 	  "must_not" : {
// 		 "term" : {"price" : 30}
// 	  }
//    }
// }

//FilterMust   https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) FilterMust(i ...F) *Client {
	c.filterbool["must"] = i
	return c
}

//FilterShould   https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) FilterShould(i ...F) *Client {
	c.filterbool["should"] = i
	return c
}

//FilterMustNot   https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-bool-query.html#query-dsl-bool-query
func (c *Client) FilterMustNot(i ...F) *Client {
	c.filterbool["must_not"] = i
	return c
}

// {
//     "bool": {
//         "must": { "match":   { "email": "business opportunity" }},
//         "should": [
//             { "match":       { "starred": true }},
//             { "bool": {
//                 "must":      { "match": { "folder": "inbox" }},
//                 "must_not":  { "match": { "spam": true }}
//             }}
//         ],
//         "minimum_should_match": 1
//     }
// }

//Match as where {"match" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-match-query.html
func (c *Client) Match(i ...F) *Client {
	for _, v := range i {
		c.match = append(c.match, F{"match": v})
	}
	return c
}

//Multy as  MultiMatch { "multi_match": { "query":interface, "field": [ "name1", "name2", "name3"] }}
// F{"field": [ "name1", "name2", "name3" ], "query":interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-multi-match-query.html
func (c *Client) Multy(fields []string, is ...F) *Client {
	h := F{"fields": fields}
	h.Append(is...)
	c.match = append(c.match, F{"multi_match": h})
	return c
}

// Range { "range": { "field": interface}}
// F{ "field": interface}
// gt: > （greater than）
// lt: < （less than）
// gte: >= (greater than or equal to）
// lte: <=（less than or equal to）
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-range-query.html
func (c *Client) Range(i ...F) *Client {
	for _, v := range i {
		c.ranges = append(c.ranges, F{"range": v})
	}
	return c
}

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

// Term { "term": { "field": interface}}
// F{ "field": interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-term-query.html
func (c *Client) Term(i ...F) *Client {
	for _, v := range i {
		c.terms = append(c.terms, F{"term": v})
	}
	return c
}

// Terms as In { "terms": { "field": [ "name1", "name2", "name3"], "other": interface }}
// F{ "field": [ "name1", "name2", "name3"], "other": interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-terms-query.html
func (c *Client) Terms(field string, values []interface{}, i ...F) *Client {
	m := F{field: values}
	m.Append(i...)
	c.terms = append(c.terms, F{"terms": m})
	return c
}

//Regexp {"regexp" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-regexp-query.html#query-dsl-regexp-query
func (c *Client) Regexp(i ...F) *Client {
	for _, v := range i {
		c.match = append(c.match, F{"regexp": v})
	}
	return c
}

//Fuzzy {"fuzzy" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-fuzzy-query.html
func (c *Client) Fuzzy(i ...F) *Client {
	for _, v := range i {
		c.match = append(c.match, F{"fuzzy": v})
	}
	return c
}

//Wildcard {"wildcard" : {"field" : interface}}
// F{"field" : interface}
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-wildcard-query.html
func (c *Client) Wildcard(i ...F) *Client {
	for _, v := range i {
		c.match = append(c.match, F{"wildcard": v})
	}
	return c
}

// MatchAll { "match_all": {}}
// F{"field": interface} or nil
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/query-dsl-match-all-query.html
func (c *Client) MatchAll(i F) *Client {
	c.match = append(c.match, F{"match_all": i})
	return c
}

// ValidateQuery i to get doc type
func (c *Client) ValidateQuery(i interface{}) *Client {
	c.method = "GET"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_validate/query?explain")
	if i == nil {
		return c.exec(c.hostDB.String(), c.template)
	}

	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return c.exec(c.hostDB.String(), c.template)
}

///////////////////todos//////////////////////

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
