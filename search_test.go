package esql_test

import (
	"testing"
	"time"

	"github.com/han2015/esql"
)

func TestBool(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must", Level: 1, Number: 1},
		mysql{Name: "this is must a test", Level: 2, Number: 1},
		mysql{Name: "this is a test", Level: 3, Number: 1},
		mysql{Name: "this is a test", Level: 4, Number: 1},
		mysql{Name: "this is a test", Level: 5, Number: 1},
	)

	cases := []struct {
		setting esql.F
		must    bool
		filter  bool
		want    int
	}{
		{setting: esql.F{"Name": "must"}, want: 5, must: true},
		{setting: esql.F{"Name": "anything"}, want: 1, filter: true},

		{setting: esql.F{"Name": "anything"}, want: 0},
		{setting: esql.F{"Name": "must"}, want: 2},
		{setting: esql.F{"Name": "this"}, want: 5},
	}

	for _, v := range cases {
		got := esql.F{}
		client := es.DB()
		if v.must {
			client.Match(esql.F{"Name": "this is"})
		}
		if v.filter {
			client.Term(esql.F{"Level": 5})
		}
		client.Bool(esql.F{"minimum_should_match": 1, "boost": 1.0}).Should(v.setting).Limit(0).Count("Number").Find(&got)
		t.Log(client.Template())
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}

func getAggre(field string, got esql.F) int {
	c := got["aggregations"].(map[string]interface{})["metric_"+field].(map[string]interface{})["value"].(float64)
	return int(c)
}

func initRecords(i ...interface{}) {
	for _, v := range i {
		es.DB().AutoIndexDoc(v)
	}

	time.Sleep(3 * time.Second) //wait to valid
}

func TestMatch(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	if err := es.DB().AutoIndexDoc(mysql{Name: "this is a test", Number: 1}).Error; err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)
	cases := []struct {
		setting esql.F
		want    int
	}{
		{setting: esql.F{"Name": esql.F{"query": "this is a test", "fuzziness": "AUTO"}}, want: 1},
		{setting: esql.F{"Name": "match"}, want: 0},
	}

	for _, v := range cases {
		got := esql.F{}
		es.DB().Match(v.setting).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}

func TestMust(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must", Number: 1},
		mysql{Name: "this is a test", Number: 1},
	)
	cases := []struct {
		setting esql.F
		want    int
	}{
		{setting: esql.F{"Name": "must"}, want: 1},
		{setting: esql.F{"Name": "test"}, want: 2},
	}

	for _, v := range cases {
		got := esql.F{}
		es.DB().Match(esql.F{"Name": "this is"}).Must(v.setting).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}
func TestMustNot(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must not", Number: 1},
		mysql{Name: "this is a test", Number: 1},
	)

	cases := []struct {
		setting esql.Not
		want    int
	}{
		{setting: esql.Not{"Name": "must"}, want: 1},
		{setting: esql.Not{"Name": "test"}, want: 0},
		{setting: esql.Not{"Name": "anything"}, want: 2},
	}

	for _, v := range cases {
		got := esql.F{}
		es.DB().Match(esql.F{"Name": "this is"}).MustNot(v.setting).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}
func TestShould(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a must not", Number: 1},
		mysql{Name: "this is a test", Number: 1, Level: 5},
	)

	cases := []struct {
		setting esql.F
		must    bool
		filter  bool
		want    int
	}{
		{setting: esql.F{"Name": "not"}, want: 2, must: true},
		{setting: esql.F{"Name": "anything"}, want: 1, filter: true},
		{setting: esql.F{"Name": "anything"}, want: 1, filter: true, must: true},

		{setting: esql.F{"Name": "anything"}, want: 0},
		{setting: esql.F{"Name": "must"}, want: 1},
		{setting: esql.F{"Name": "this"}, want: 2},
	}

	for _, v := range cases {
		got := esql.F{}
		client := es.DB()
		if v.must {
			client.Match(esql.F{"Name": "this is"})
		}
		if v.filter {
			client.Term(esql.F{"Level": 5})
		}
		client.Should(v.setting).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}
func TestIn(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must not", Level: 1, Number: 1},
		mysql{Name: "this is a test", Level: 2, Number: 1},
	)

	cases := []struct {
		setting esql.Setting
		want    int
	}{
		{setting: esql.F{"Level": []int{1, 2}}, want: 2},
		{setting: esql.F{"Level": []int{5}}, want: 0},
		{setting: esql.Not{"Level": []int{1}}, want: 1},
		{setting: esql.Not{"Level": []int{5}}, want: 2},
	}

	for _, v := range cases {
		got := esql.F{}
		es.DB().In(v.setting).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}

func TestMissing(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must not", Level: 1, Number: 1},
		mysql{Level: 0, Number: 1},
	)

	cases := []struct {
		values string
		want   int
	}{
		{values: "Name", want: 0},
		{values: "kk", want: 2},
	}

	for _, v := range cases {
		got := esql.F{}
		es.DB().Missing(v.values).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}
func TestNotNil(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must not", Level: 1, Number: 1},
		mysql{Level: 0, Number: 1},
	)

	cases := []struct {
		values string
		want   int
	}{
		{values: "Name", want: 2},
		{values: "kk", want: 0},
	}

	for _, v := range cases {
		got := esql.F{}
		es.DB().NotNil(v.values).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}
func TestBetween(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must not", Level: 1, Number: 1},
		mysql{Name: "this is a test", Level: 2, Number: 1},
		mysql{Name: "this is a test", Level: 3, Number: 1},
		mysql{Name: "this is a test", Level: 4, Number: 1},
		mysql{Name: "this is a test", Level: 5, Number: 1},
	)

	cases := []struct {
		setting esql.Setting
		want    int
	}{
		{setting: esql.F{"Level": esql.F{"gt": 1}}, want: 4},
		{setting: esql.F{"Level": esql.F{"gt": 1, "lte": 4}}, want: 3},
		{setting: esql.Not{"Level": esql.F{"gt": 2}}, want: 2},
		{setting: esql.Not{"Level": esql.F{"gt": 1, "lte": 4}}, want: 2},
	}

	for _, v := range cases {
		got := esql.F{}
		es.DB().Between(v.setting).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}

func TestStringQuery(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must not", Level: 1, Number: 1},
		mysql{Name: "this is a test", Level: 2, Number: 1},
		mysql{Name: "this is a test", Level: 3, Number: 1},
		mysql{Name: "this is a test", Level: 4, Number: 1},
		mysql{Name: "this is a test", Level: 5, Number: 1},
	)

	cases := []struct {
		setting esql.F
		want    int
	}{
		{setting: esql.F{"query": "must not not", "fields": []string{"Name"}}, want: 1},
		{setting: esql.F{"query": "this is a test", "fields": []string{"Name"}}, want: 5},
	}

	for _, v := range cases {
		got := esql.F{}
		c := es.DB().StringQuery(v.setting).Limit(0).Count("Number").Find(&got)
		t.Log(c.Template())
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}
func TestSimpleStringSelect(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must not", Level: 1, Number: 1},
		mysql{Name: "this is a test", Level: 2, Number: 1},
		mysql{Name: "this is a test", Level: 3, Number: 1},
		mysql{Name: "this is not a test", Level: 4, Number: 1},
		mysql{Name: "a test", Level: 5, Number: 1},
	)

	cases := []struct {
		setting []esql.F
		want    int
	}{
		{setting: []esql.F{{"query": "not must", "fields": []string{"Name"}}}, want: 2},
		{setting: []esql.F{{"query": "test", "fields": []string{"Name"}}}, want: 5},
	}

	for _, v := range cases {
		got := esql.F{}
		es.DB().SimpleStringSelect(v.setting...).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}
func TestPhrase(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must cool not", Level: 1, Number: 1},
		mysql{Name: "this is a test", Level: 2, Number: 1},
		mysql{Name: "this is a test", Level: 3, Number: 1},
		mysql{Name: "this is not a test", Level: 4, Number: 1},
		mysql{Name: "a test", Level: 5, Number: 1},
	)

	cases := []struct {
		setting []esql.F
		want    int
	}{
		{setting: []esql.F{{"Name": esql.F{"query": "must cool"}}}, want: 1},
		{setting: []esql.F{{"Name": esql.F{"query": "not must"}}}, want: 0},
		{setting: []esql.F{{"Name": esql.F{"query": "is a test"}}}, want: 3},
	}

	for _, v := range cases {
		got := esql.F{}
		es.DB().Phrase(v.setting...).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want, getAggre("Number", got))
		}
	}
}
func TestMulty(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must cool not", Number: 1},
		mysql{Name: "this is a test", Number: 1},
		mysql{Name: "this is a test", Number: 1},
		mysql{Name: "this is not a test", Number: 1},
		mysql{Name: "a test", Number: 1},
	)

	cases := []struct {
		setting []esql.Setting
		want    int
	}{
		{
			setting: []esql.Setting{
				esql.F{"fields": []string{"Name"}, "query": "must cool"},
				esql.F{"fields": []string{"Gender"}, "query": "must"}},
			want: 0,
		},
		{setting: []esql.Setting{esql.F{"fields": []string{"Name"}, "query": "must cool"}}, want: 1},
		{setting: []esql.Setting{esql.F{"fields": []string{"Name"}, "query": "not must"}}, want: 2},
		{setting: []esql.Setting{esql.F{"fields": []string{"Name"}, "query": "is a test"}}, want: 5},
	}

	for _, v := range cases {
		got := esql.F{}
		c := es.DB().Multy(v.setting...).Limit(0).Count("Number").Find(&got)
		t.Log(c.Template())
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want, getAggre("Number", got))
		}
	}
}
func TestTerm(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Level: 1, Number: 1},
		mysql{Level: 2, Number: 1},
		mysql{Level: 3, Number: 1},
		mysql{Level: 4, Number: 1},
		mysql{Name: "term with text", Number: 1},
	)

	cases := []struct {
		setting []esql.Setting
		want    int
	}{
		{
			setting: []esql.Setting{
				esql.F{"Level": 1},
				esql.F{"Number": 1}},
			want: 1,
		},
		{
			setting: []esql.Setting{
				esql.F{"Level": 6},
				esql.F{"Number": 1}},
			want: 0,
		},
		{setting: []esql.Setting{esql.Not{"Level": 2}, esql.F{"Number": 1}}, want: 4},
		{setting: []esql.Setting{esql.Not{"Level": 6}}, want: 5},
		{setting: []esql.Setting{esql.F{"Name": "term with text"}}, want: 0}, //not support text field
	}

	for _, v := range cases {
		got := esql.F{}
		es.DB().Term(v.setting...).Limit(0).Count("Number").Find(&got)
		if getAggre("Number", got) != v.want {
			t.Log(got)
			t.Fatal(v.want)
		}
	}
}
func TestTerms(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Level: 1, Number: 1},
		mysql{Level: 2, Number: 1},
		mysql{Level: 3, Number: 1},
		mysql{Level: 4, Number: 1},
	)

	cases := []struct {
		setting []esql.Setting
		want    int
	}{
		{
			setting: []esql.Setting{
				esql.F{"Level": []int{1, 2, 3}},
				esql.F{"Number": []int{1}}},
			want: 3,
		},
		{
			setting: []esql.Setting{
				esql.F{"Level": []int{1, 2, 3}},
				esql.F{"Number": []int{2}}},
			want: 0,
		},
		{setting: []esql.Setting{esql.F{"Level": []int{6}}}, want: 0},
		{setting: []esql.Setting{esql.Not{"Level": []int{1, 2}}}, want: 2},
	}

	for _, v := range cases {
		got := esql.F{}
		c := es.DB().Terms(v.setting...).Limit(0).Count("Number").Find(&got)
		t.Log(c.Template())
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want)
		}
	}
}
func TestRegexp(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this is a test must cool not", Number: 1},
		mysql{Name: "this is a test", Description: "test regexp", Number: 1},
		mysql{Name: "this is a test", Description: "12345560", Number: 1},
		mysql{Name: "this is not a test", Description: "aaabbb", Number: 1},
		mysql{Name: "many a test", Description: "has_many", Number: 1},
	)

	cases := []struct {
		setting []esql.Setting
		want    int
	}{
		{
			setting: []esql.Setting{
				esql.F{"Name": "must *"}},
			want: 1,
		},
		{setting: []esql.Setting{esql.F{"Name": "this *"}, esql.F{"Description": "[0-9]+"}}, want: 1},
		{setting: []esql.Setting{esql.F{"Description": "has_.*"}}, want: 1},
		{setting: []esql.Setting{esql.F{"Description": "a+b+"}}, want: 1},
	}

	for _, v := range cases {
		got := esql.F{}
		c := es.DB().Regexp(v.setting...).Limit(0).Count("Number").Find(&got)
		t.Log(c.Template())
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want, getAggre("Number", got))
		}
	}
}

func TestWildcard(t *testing.T) {
	es.DB().Term(esql.F{"Number": 1}).DeleteByQuerry()
	initRecords(
		mysql{Name: "this musdfsst cool not", Number: 1},
		mysql{Name: "this0000is a test", Description: "test regexp", Number: 1},
		mysql{Name: "thisasdfasdfis a test", Description: "12345560", Number: 1},
	)

	cases := []struct {
		setting []esql.Setting
		want    int
	}{
		{
			setting: []esql.Setting{
				esql.F{"Name": "mu*st"}},
			want: 1,
		},
		{setting: []esql.Setting{esql.F{"Name": "this*is"}, esql.F{"Description": "12*0"}}, want: 1},
		{setting: []esql.Setting{esql.F{"Description": "te*exp"}}, want: 1},
	}

	for _, v := range cases {
		got := esql.F{}
		c := es.DB().Wildcard(v.setting...).Limit(0).Count("Number").Find(&got)
		t.Log(c.Template())
		if getAggre("Number", got) != v.want {
			t.Fatal(v.want, getAggre("Number", got))
		}
	}
}
