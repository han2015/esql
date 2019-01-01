package esql_test

import "testing"

/*
{
    "aggs": {
        "group_name": {
            "terms": {
                "field": "name"
            }
        },
        "metric_name": {
            "avg": {
                "field": "name"
            }
        }
    }
}
*/
func TestGroupTerms(t *testing.T) {
	c := es.DB()
	if err := c.GroupTerms("name").Avg("name", "*").Serialize().Error; err != nil {
		t.Fatal(err)
	}

	t.Log(c.Template())
}

/*
{
    "aggs": {
        "group_name": {
			"terms": {
                "field": "name"
            },
			"aggs": {
                "metric_name": {
                    "avg": {
                        "field": "name"
                    }
                }
            }
        }
    }
}
*/
func TestMetricsOnGroupTerms(t *testing.T) {
	c := es.DB()
	if err := c.GroupTerms("name").Avg("name", "group_name").Serialize().Error; err != nil {
		t.Fatal(err)
	}

	t.Log(c.Template())
}
