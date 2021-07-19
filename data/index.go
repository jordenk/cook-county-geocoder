package data

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"log"
	"os"
	"strings"
	"time"
	"cook-county-geocoder/shared/mapping"
)

// Receive normalized structs and index ES.
func BuildEsClient(hosts []string) *elasticsearch.Client {
	retryBackoff := backoff.NewExponentialBackOff()

	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:     hosts,
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},
		MaxRetries: 5,
	})
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	return es
}

func DoesIndexExist(es *elasticsearch.Client, indexName string) bool {
	res, err := es.Indices.Exists(
		[]string{indexName},
	)
	if err != nil {
		log.Printf("Error calling index exists API: %s\n", err)
	}
	return res.StatusCode == 200
}

func CreateIndex(es *elasticsearch.Client, filePath string, indexName string) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading index file: %s", err)
	}
	stripped := strings.Join(strings.Fields(string(file)), "")
	body := strings.NewReader(stripped)
	res, err := es.Indices.Create(
		indexName,
		es.Indices.Create.WithBody(body),
		es.Indices.Create.WithWaitForActiveShards("1"),
	)
	if err != nil {
		log.Fatalf("Cannot create index- request creation error: %s", err)
	}
	if res.IsError() {
		log.Fatalf("Cannot create index- response error: %s", res)
	}
	log.Printf("Created index %s\n", indexName)
	_ = res.Body.Close()
}

func DeleteIndex(es *elasticsearch.Client, indexName string) {
	res, err := es.Indices.Delete([]string{indexName})

	if err != nil {
		log.Fatalf("Cannot delete index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("Cannot delete index: %s", res)
	}
	log.Printf("Deleted index %s\n", indexName)
	_ = res.Body.Close()
}

const (
	WORKERS = 5
)

func BulkIndexEs(es *elasticsearch.Client, indexName string, esAddresses []mapping.EsAddress) esutil.BulkIndexerStats {
	bulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      indexName,
		Client:     es,
		NumWorkers: WORKERS,
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	start := time.Now().UTC()

	for _, esAddress := range esAddresses {
		// Encode article to JSON
		data, err := json.Marshal(esAddress)
		if err != nil {
			log.Fatalf("Cannot encode address document %v: %s", esAddress, err)
		}

		err = bulkIndexer.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action:    "index",
				Body:      bytes.NewReader(data),
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {},
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Printf("ERROR: %s", err)
					} else {
						log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			log.Fatalf("Unexpected error: %s", err)
		}
	}

	if err := bulkIndexer.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	biStats := bulkIndexer.Stats()
	dur := time.Since(start)

	if biStats.NumFailed > 0 {
		log.Fatalf(
			"Indexed [%d] documents with [%d] errors in %s (%d docs/sec)",
			int64(biStats.NumIndexed),
			int64(biStats.NumFailed),
			dur.Truncate(time.Millisecond),
			int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed)),
		)
	} else {
		log.Printf(
			"Sucessfuly indexed [%d] documents in %s (%d docs/sec)",
			int64(biStats.NumIndexed),
			dur.Truncate(time.Millisecond),
			int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed)),
		)
	}
	return biStats
}
