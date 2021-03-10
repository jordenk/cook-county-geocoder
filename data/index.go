package data

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"log"
	"time"
)

// Receive normalized structs and index ES.
func BuildEsClient() *elasticsearch.Client {
	retryBackoff := backoff.NewExponentialBackOff()

	es, err := elasticsearch.NewClient(elasticsearch.Config{
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

// TODO create index if it doesn't exist.
func HandleIndex() {
	// Check if index exists

	//res, err = es.Indices.Create(indexName)
	//if err != nil {
	//	log.Fatalf("Cannot create index: %s", err)
	//}
	//if res.IsError() {
	//	log.Fatalf("Cannot create index: %s", res)
	//}
	//res.Body.Close()
}

const (
	WORKERS = 5
)

func BulkIndexEs(es *elasticsearch.Client, indexName string, esAddresses *[]EsAddress) {
	bulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      indexName,
		Client:     es,
		NumWorkers: WORKERS,
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	start := time.Now().UTC()

	for _, esAddress := range *esAddresses {
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
}
