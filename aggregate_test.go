package esql_test

import (
	"testing"

	"github.com/han2015/esql"
)

/*
{
    "aggs": {
        "metric_name": {
            "avg": {
                "field": "name"
            }
        }
    },
    "size": 0
}
*/
func TestAvgAggration(t *testing.T) {
	c := es.DB()
	if err := c.Avg("name").Limit(0).Serialize().Error; err != nil {
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
	if err := c.GroupTerms("name").Avg("name").Serialize().Error; err != nil {
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

/*
{
    "aggs": {
        "group_name": {
            "aggs": {
                "metric_name": {
                    "avg": {
                        "field": "name",
                        "missing": 10,
                        "other": "other setting"
                    }
                }
            },
            "terms": {
                "field": "name"
            }
        }
    }
}
*/
func TestMetricsSettingOnGroupTerms(t *testing.T) {
	c := es.DB()
	if err := c.GroupTerms("name").Avg("name", "group_name", esql.F{"missing": 10,
		"other": "other setting"}).Serialize().Error; err != nil {
		t.Fatal(err)
	}

	t.Log(c.Template())
}
