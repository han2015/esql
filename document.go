package esql

import (
	"encoding/json"
	"fmt"
	"path"
)

// GetDocWithID twitter/_doc/0?_source=false
func (c *Client) GetDocWithID(id string) *Client {
	if checkIndexName(c) {
		return c
	}
	c.hostDB.Path = path.Join(c.hostDB.Path, "_doc", id)
	return c.exec(c.hostDB.String())
}

// UpdateDoc adds or updates a typed JSON document in a specific index, making it searchable
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html
func (c *Client) UpdateDoc(id string, i interface{}) *Client {
	if checkIndexName(c) {
		return c
	}
	data, _ := json.Marshal(i)
	c.method = "POST"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_doc", id, "_update")
	return c.exec(c.hostDB.String(), string(data))
}

// IndexDoc https://www.elastic.co/guide/en/elasticsearch/reference/6.5/getting-started-query-document.html
func (c *Client) IndexDoc(id string, i interface{}) *Client {
	if checkIndexName(c) {
		return c
	}
	data, _ := json.Marshal(i)
	c.method = "PUT"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_doc", id)
	return c.exec(c.hostDB.String(), string(data))
}

//UpdatePartialDoc  passing a partial document, which will be merged into the existing document
//https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html#_updates_with_a_partial_document
func (c *Client) UpdatePartialDoc(id string, i interface{}) *Client {
	if checkIndexName(c) {
		return c
	}
	data, _ := json.Marshal(i)
	c.method = "POST"
	c.hostDB.Path = path.Join(c.hostDB.Path, id, "_update")
	return c.exec(c.hostDB.String(), string(data))
}

// AutoIndexDocs The index operation automatically creates an index if it has not been created before
// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-creation
// curl -X POST "localhost:9200/esql/_doc/_bulk?pretty" -H 'Content-Type: application/json' -d'
// {"index":{}}
// {"name": "John Doe" }
// {"index":{}}
// {"name": "Jane Doe" }
// '
func (c *Client) AutoIndexDocs(i ...interface{}) *Client {
	var str string
	for _, v := range i {
		data, _ := json.Marshal(v)
		str += fmt.Sprintf(`{"index":{}}\n%s\n`, data)
	}

	return c.Bulk(str)
}

// DeleteDoc deletes an existing index.
// default delete table
// delete doc
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/getting-started-delete-documents.html
func (c *Client) DeleteDoc(id string) *Client {
	if checkIndexName(c) {
		return c
	}
	c.method = "DELETE"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_doc", id)
	return c.exec(c.hostDB.String())
}

// Bulk it possible to perform many index/delete operations in a single API call.
// This can greatly increase the indexing speed.
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/docs-bulk.html
func (c *Client) Bulk(data string) *Client {
	if checkIndexName(c) {
		return c
	}
	c.method = "POST"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_doc/_bulk?pretty")
	return c.exec(c.hostDB.String())
}

//DeleteByQuerry todo: to support
//https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html
func (c *Client) DeleteByQuerry() *Client {
	return c
	// c.method = "DELETE"
	// c.hostDB.Path = path.Join(c.hostDB.Path, "_delete_by_query")
	// return c.exec(c.hostDB.String())
}
