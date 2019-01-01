package esql

import (
	"fmt"
	"reflect"
)

/*
TODOS:
	1)TopHits  https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-top-hits-aggregation.html
*/

//Group same as GroupTerms https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-bucket-terms-aggregation.html
//"terms" : { "field" : "genre" }
func (c *Client) Group(field string, i ...F) *Client {
	return c.setGroups("terms", field, i)
}

// GroupTerms same as Group https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-bucket-terms-aggregation.html
//"terms" : { "field" : "genre" }
func (c *Client) GroupTerms(field string, i ...F) *Client {
	return c.setGroups("terms", field, i)
}

// GroupDateHistogram https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-bucket-datehistogram-aggregation.html
// "date_histogram" : {
// 	"field" : "date",
// 	"interval" : "month"
// }
func (c *Client) GroupDateHistogram(field string, i ...F) *Client {
	return c.setGroups("date_histogram", field, i)
}

// GroupHistogram https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-bucket-histogram-aggregation.html
// "histogram" : {
// 	"field" : "price",
// 	"interval" : 50
// }
func (c *Client) GroupHistogram(field string, i ...F) *Client {
	return c.setGroups("histogram", field, i)
}

// GroupDateRange https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-bucket-daterange-aggregation.html
// "date_range": {
// 	"field": "date",
// 	"format": "MM-yyy",
// 	"ranges": [
// 		{ "to": "now-10M/M" },
// 		{ "from": "now-10M/M" }
// 	]
// }
func (c *Client) GroupDateRange(field string, i ...F) *Client {
	return c.setGroups("date_range", field, i)
}

//GroupIPRange https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-bucket-iprange-aggregation.html
// "ip_range" : {
// 	"field" : "ip",
// 	"ranges" : [
// 		{ "mask" : "10.0.0.0/25" },
// 		{ "mask" : "10.0.0.127/25" }
// 	]
// }
func (c *Client) GroupIPRange(field string, i ...F) *Client {
	return c.setGroups("ip_range", field, i)
}

//GroupRange https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-bucket-range-aggregation.html
// "range" : {
// 	"field" : "price",
// 	"ranges" : [
// 		{ "to" : 100.0 },
// 		{ "from" : 100.0, "to" : 200.0 },
// 		{ "from" : 200.0 }
// 	]
// }
func (c *Client) GroupRange(field string, i ...F) *Client {
	return c.setGroups("range", field, i)
}

//GroupGeoDistance https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-bucket-geodistance-aggregation.html
// "geo_distance" : {
// 	"field" : "location",
// 	"origin" : "52.3760, 4.894",
// 	"unit" : "km",
// 	"ranges" : [
// 		{ "to" : 100 },
// 		{ "from" : 100, "to" : 300 },
// 		{ "from" : 300 }
// 	]
// }
func (c *Client) GroupGeoDistance(field, origin string, i ...F) *Client {
	_set := F{}
	_set["field"] = field
	_set["origin"] = origin
	if len(i) > 0 {
		i[0].Append(_set)
	}

	return c.setGroups("geo_distance", field, i)
}

//Avg A single-value metrics aggregation that computes the average of numeric values that are extracted from the aggregated documents.
//https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-avg-aggregation.html
//"avg" : { "field" : "grade" }
func (c *Client) Avg(field string, i ...interface{}) *Client {
	return c.setMetrics("avg", field, i)
}

// Cardinality https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-cardinality-aggregation.html
// cardinality" : {
// 	"field" : "_doc",
// 	"precision_threshold": 100
// }
func (c *Client) Cardinality(field string, i ...interface{}) *Client {
	return c.setMetrics("cardinality", field, i)
}

//ExtendedStats https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-extendedstats-aggregation.html
func (c *Client) ExtendedStats(field string, i ...interface{}) *Client {
	return c.setMetrics("extended_stats", field, i)
}

//GeoBounds https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-geobounds-aggregation.html
func (c *Client) GeoBounds(field string, i ...interface{}) *Client {
	return c.setMetrics("geo_bounds", field, i)
}

//GeoCentroid https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-geocentroid-aggregation.html
func (c *Client) GeoCentroid(field string, i ...interface{}) *Client {
	return c.setMetrics("geo_centroid", field, i)
}

//Max https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-max-aggregation.html
func (c *Client) Max(field string, i ...interface{}) *Client {
	return c.setMetrics("max", field, i)
}

//Min https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-min-aggregation.html
func (c *Client) Min(field string, i ...interface{}) *Client {
	return c.setMetrics("min", field, i)
}

//Percentiles https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-percentile-aggregation.html
func (c *Client) Percentiles(field string, i ...interface{}) *Client {
	return c.setMetrics("percentiles", field, i)
}

//PercentileRanks https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-percentile-rank-aggregation.html
// "percentile_ranks" : {
// 	"field" : "fieldName",
// 	"values" : [500, 600],
//	"keyed": false
// }
func (c *Client) PercentileRanks(field string, i ...interface{}) *Client {
	return c.setMetrics("percentile_ranks", field, i)
}

//Stats https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-stats-aggregation.html
func (c *Client) Stats(field string, i ...interface{}) *Client {
	return c.setMetrics("stats", field, i)
}

//Sum https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-sum-aggregation.html
func (c *Client) Sum(field string, i ...interface{}) *Client {
	return c.setMetrics("sum", field, i)
}

//Count https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-valuecount-aggregation.html
func (c *Client) Count(field string, i ...interface{}) *Client {
	return c.setMetrics("count", field, i)
}

//ScriptedMetric https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-scripted-metric-aggregation.html
func (c *Client) ScriptedMetric(i F) *Client {
	return c.setMetrics("scripted_metric", "", []interface{}{i})
}

//WeightedAvg https://www.elastic.co/guide/en/elasticsearch/reference/6.5/search-aggregations-metrics-weight-avg-aggregation.html
func (c *Client) WeightedAvg(field, weight string, i ...interface{}) *Client {
	_set := F{}
	_set["value"] = F{"field": field}
	_set["weight"] = F{"field": weight}
	i = append(i, _set)
	return c.setMetrics("weighted_avg", "", i)
}

func (c *Client) setGroups(types, field string, i []F) *Client {
	_set := F{}
	if len(i) > 0 {
		_set = i[0]
	}
	if field != "" {
		_set["field"] = field
	}
	c.groups["group_"+field] = F{
		types: _set,
	}
	return c
}

func (c *Client) setMetrics(types, field string, i []interface{}) *Client {
	var onGroup string
	_set := F{}
	for _, v := range i {
		switch reflect.TypeOf(v).Kind() {
		case reflect.String:
			onGroup = v.(string)
		case reflect.Map:
			_set.Append(v.(F))
		}
	}

	if field != "" {
		_set["field"] = field
	}

	if onGroup == "*" || onGroup == "" {
		c.metrics["metric_"+field] = F{
			types: _set,
		}
		return c
	}
	// add aggregation on group aggregation
	if c.groups[onGroup] == nil {
		c.Error = fmt.Errorf("metrics on %s, but it is not existed", onGroup)
		return c
	}

	g := c.groups[onGroup].(F)
	if g["aggs"] == nil {
		g["aggs"] = F{}
	}

	g["aggs"].(F)["metric_"+field] = F{
		types: _set,
	}

	return c
}
