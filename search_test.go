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
	c := es.DB()
	if err := c.Dismax(esql.F{"boost": 1}).Match(esql.F{"name": "John"}).Match(esql.F{"name": "John"}).
		Serialize().Error; err != nil {
		t.Fatal(err)
	}

	t.Log(c.Template())
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
	c := es.DB()
	if err := c.Must(esql.F{"name": "John"}).
		Should(esql.F{"name": "elasticsearch"}).
		Term(esql.F{"tag": "tech"}).
		Range(esql.Not{"age": esql.F{"gte": 10, "lte": 20}}, esql.F{"tag": "tech"}).
		Limit(3, 4).
		Serialize().
		Error; err != nil {
		t.Fatal(err)
	}

	t.Log(c.Template())
}

func TestJoins(t *testing.T) {
	arr := []string{"nested", "has_parent", "has_child"}
	for _, v := range arr {
		c := es.DB()
		if err := c.Joins(v, "blog").Must(esql.F{"name": "John"}).
			Term(esql.F{"tag": "tech"}).
			Limit(3, 4).
			Serialize().
			Error; err != nil {
			t.Fatal(err)
		}

		t.Log(c.Template())
	}

}
