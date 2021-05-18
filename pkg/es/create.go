package es

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/esapi"
)

// auto generate _id
func CreateInBatches(index string, documents []interface{}, esClient *elasticsearch.Client) error {
	requestBody, err := constructRequestBody(index, documents)
	if err != nil {
		return err
	}
	return PerformESBulk(index, requestBody, esClient)
}

// 用 action index 自动生成 _id
func constructRequestBody(index string, documents []interface{}) (string, error) {
	var bodyBuf bytes.Buffer
	for _, document := range documents {
		createHeader :=
			map[string]interface{}{
				"index": map[string]interface{}{
					"_index": index,
					"_type":  "_doc",
				},
			}
		header, err := json.Marshal(createHeader)
		if err != nil {
			return "", err
		}
		bodyBuf.Write(header)
		bodyBuf.WriteByte('\n')
		content, err := json.Marshal(document)
		if err != nil {
			return "", err
		}
		bodyBuf.Write(content)
		bodyBuf.WriteByte('\n')
	}
	return bodyBuf.String(), nil
}

func PerformESBulk(index string, requestBody string, esClient *elasticsearch.Client) error {
	// Set up the request object.
	req := esapi.BulkRequest{
		Index:   index,
		Body:    strings.NewReader(requestBody),
		Refresh: "false",
		Pretty:  false,
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return err
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return err
	}
	return nil
}
