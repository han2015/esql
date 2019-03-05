package esql

import (
	"encoding/json"
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
	c.method = "PUT"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_doc", id)
	return c.exec(c.hostDB.String(), string(data))
}

// IndexDoc https://www.elastic.co/guide/en/elasticsearch/reference/6.5/docs-index_.html
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
	c.hostDB.Path = path.Join(c.hostDB.Path, "_doc", id, "_update")
	return c.exec(c.hostDB.String(), string(data))
}

// AutoIndexDocs The index operation automatically creates an index if it has not been created before
// https://www.elastic.co/guide/en/elasticsearch/reference/6.5/docs-index_.html#_automatic_id_generation
func (c *Client) AutoIndexDoc(i interface{}) *Client {
	data, _ := json.Marshal(i)
	c.method = "POST"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_doc")
	return c.exec(c.hostDB.String(), string(data))
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

//DeleteByQuerry todo: to support
//https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html
func (c *Client) DeleteByQuerry() *Client {
	c.method = "POST"
	c.hostDB.Path = path.Join(c.hostDB.Path, "_delete_by_query")
	c.Serialize()
	return c.exec(c.hostDB.String(), c.template)
}
