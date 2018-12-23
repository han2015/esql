package esql

import (
	"encoding/json"
	"fmt"
	"path"
	"reflect"
)

// GET twitter/_doc/0?_source=false
func (c *Client) GetDocWithID(tableAndId string) *Client {
	c.method = "GET"
	c.hostDB.Path = path.Join(c.hostDB.Path, tableAndId)
	return c.exec(c.hostDB.String())
}

// InsertDocument adds or updates a typed JSON document in a specific index, making it searchable
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html
func (c *Client) UpdateDoc(id string, i interface{}) *Client {
	t := reflect.TypeOf(i)

	if t.Kind() == reflect.Slice {
		c.Error = fmt.Errorf("don't support index-with-id on slice")
		return c
	}

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	data, _ := json.Marshal(i)
	c.method = "PUT"
	c.hostDB.Path = path.Join(c.hostDB.Path, t.Name(), id, "_update")
	return c.exec(c.hostDB.String(), string(data))
}

//UpdatePartialDoc  passing a partial document, which will be merged into the existing document
//https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html#_updates_with_a_partial_document
func (c *Client) UpdatePartialDoc(tableAndId string, i interface{}) *Client {
	data, _ := json.Marshal(i)
	c.method = "POST"
	c.hostDB.Path = path.Join(c.hostDB.Path, tableAndId, "_update")
	return c.exec(c.hostDB.String(), string(data))
}

// DocAutoIndex The index operation automatically creates an index if it has not been created before
// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-creation
func (c *Client) AutoDocIndex(i interface{}) *Client {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	data, _ := json.Marshal(i)
	c.method = "POST"
	c.hostDB.Path = path.Join(c.hostDB.Path, t.Name())
	return c.exec(c.hostDB.String(), string(data))
}

// Delete deletes an existing index.
// delete indexDB /
// delete table   /table
// delete doc  	  /table/id
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-delete-index.html
func (c *Client) Delete(str string) *Client {
	c.method = "DELETE"
	c.hostDB.Path = path.Join(c.hostDB.Path, str)
	return c.exec(c.hostDB.String())
}

//DeleteByQuerry todo: to support
//https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html
func (c *Client) DeleteByQuerry() *Client {
	c.method = "DELETE"
	return c
}
