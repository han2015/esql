package esql_test

import (
	"testing"

	"github.com/han2015/esql"
)

// {
//     "query": {
//         "dis_max" : {
//             "queries" : {
//                 "bool":{
//                     "should": [
//                         {"match" : {
//                             "name": "John"
//                         }},
//                         {"match" : {
//                             "name": "jane"
//                         }}
//                     ]
//                 }
//             }
//         }
//     }
// }
func TestDisMax(t *testing.T) {
	result := `{"query":{"bool":{"filter":[{"term":{"tag":"tech"}}],"must":[{"match":{"name":"John"}}],"mustnot":[{"range":{"age":{"gte":10,"let":20}}}],"should":[{"match":{"name":"elasticsearch"}},{"match":{"tag":"wow"}}]}}}`
	c := es.DB()
	if err := c.Dismax(esql.F{"boost": 1}).Match(esql.F{"name": "John"}).Match(esql.F{"name": "John"}).
		MakeQuery().Error; err != nil {
		t.Fatal(err)
	}
	if s := c.Template(); s == result {
		t.Log(c.Template())
		return
	}

	t.Fatal(c.Template())
}

// {
//   "query": {
//     "bool" : {
//       "must" : {
//         "match" : { "user" : "John" }
//       },
//       "filter": {
//         "term" : { "tag" : "tech" }
//       },
//       "must_not" : {
//         "range" : {
//           "age" : { "gte" : 10, "lte" : 20 }
//         }
//       },
//       "should" : [
//         { "match" : { "tag" : "wow" } },
//         { "match" : { "name" : "elasticsearch" } }
//       ],
//       "minimum_should_match" : 1,
//       "boost" : 1.0
//     }
//   }
// }
func TestBool(t *testing.T) {
	result := `{"query":{"bool":{"filter":[{"term":{"tag":"tech"}}],"must":[{"match":{"name":"John"}}],"mustnot":[{"range":{"age":{"gte":10,"lte":20}}}],"should":[{"match":{"name":"elasticsearch"}}]}}}`
	c := es.DB()
	if err := c.Bool(esql.F{"boost": 1}).
		Must(esql.F{"name": "John"}).
		Should(esql.F{"name": "elasticsearch"}).
		Term(esql.F{"tag": "tech"}).
		Range(esql.Not{"age": esql.F{"gte": 10, "lte": 20}}).
		MakeQuery().Error; err != nil {
		t.Fatal(err)
	}
	if s := c.Template(); s == result {
		t.Log(c.Template())
		return
	}

	t.Fatal(c.Template())
}
