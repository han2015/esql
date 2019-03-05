package esql_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/han2015/esql"
)

func TestGetDocWithID(t *testing.T) {
	model := mysql{Name: "struct name"}
	var res esql.Response
	got := struct {
		ID string `json:"_id"`
	}{}
	es.DB().IndexDoc("test1", model).Response(&got)
	es.DB().GetDocWithID(got.ID).Response(&res)
	if !res.Found {
		t.Fatal("TestGetDocWithID: not find record with id")
	}
}

func TestUpdateDoc(t *testing.T) {
	model := mysql{Name: "struct name"}
	doc := esql.F{}
	got := struct {
		ID string `json:"_id"`
	}{}

	es.DB().IndexDoc("test1", model).Response(&got)
	es.DB().UpdateDoc(got.ID, mysql{Name: "mysql", Number: 8})
	es.DB().GetDocWithID(got.ID).Response(&doc)

	data := doc["_source"].(map[string]interface{})
	if fmt.Sprint(data["Name"]) != "mysql" {
		t.Fatal("")
	}
}

func TestAutoIndexDocs(t *testing.T) {
	got := esql.F{}
	var count float64
	models := []interface{}{mysql{Name: "autoindex", Number: 1}, mysql{Name: "autoindex"}, mysql{Name: "autoindex"}, mysql{Name: "autoindex"}}
	for _, v := range models {
		if err := es.DB().AutoIndexDoc(v).Error; err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(3 * time.Second) //wait to valid
	client := es.DB().Where(esql.F{"Name": "autoindex"}).Count("Number").Find(&got)
	if client.Error != nil {
		t.Fatal(client.Error)
	}

	count = (got["aggregations"].(map[string]interface{})["metric_Number"]).(map[string]interface{})["value"].(float64)
	if int(count) != 4 {
		t.Fatal(count, ":match != 4")
	}

	es.DB().Where(esql.F{"Name": "autoindex"}).DeleteByQuerry().Response(&got)
	count = got["deleted"].(float64) + got["total"].(float64)
	if int(count) != 8 {
		t.Fatal(count, ":delete !=8")
	}
}

func TestDeleteDoc(t *testing.T) {
	model := mysql{Name: "struct name"}
	var res esql.Response
	got := struct {
		ID string `json:"_id"`
	}{}

	es.DB().IndexDoc("test1", model).Response(&got)
	es.DB().DeleteDoc(got.ID)
	es.DB().GetDocWithID(got.ID).Response(&res)
	if res.Found {
		t.Fatal("")
	}
}
