package data

import (
	"bytes"
	"cook-county-geocoder/shared/mapping"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/elastic"
	"log"
	"os"
	"testing"
	"time"
)

var (
	client *elasticsearch.Client
)

const (
	addressIndex = "address_test"
)

func TestMain(m *testing.M) {
	log.Println("Setting up Elasticsearch.")

	endpoint, ciMode := os.LookupEnv("IT_ES_ENDPOINT")
	if ciMode {
		log.Println("Using ES service for CI.")
		client = BuildEsClient([]string{fmt.Sprintf("http://%s", endpoint)})
	} else {
		es := elastic.Preset(
			elastic.WithVersion("7.9.0"),
		)

		container, err := gnomock.Start(es)
		if err != nil {
			log.Fatal(err)
		}

		defer func() { _ = gnomock.Stop(container) }()

		client = BuildEsClient([]string{fmt.Sprintf("http://%s", container.DefaultAddress())})
	}

	exitVal := m.Run()

	os.Exit(exitVal)
}

func TestBulkIndex(t *testing.T) {
	beforeEach()

	var addresses []mapping.EsAddress
	for i := 100; i < 10100; i++ {
		addresses = append(addresses, buildTestEsAddress(i))
	}

	stats := BulkIndexEs(client, addressIndex, &addresses)

	// Check the stats for successful indexed count
	indexedCount := stats.NumIndexed
	expectedIndexedCount := uint64(len(addresses))

	if indexedCount != expectedIndexedCount {
		t.Errorf("Indexed document count (%d) does not equal expected indexed document count (%d)", indexedCount, expectedIndexedCount)
	}

	// Perform an actual query to make sure stats are accurate. This isn't necessary, it's a sanity check.
	time.Sleep(3 * time.Second)
	if getResultCount() != expectedIndexedCount {
		t.Errorf("Document count found in ES (%d) does not equal expected document count (%d)", indexedCount, expectedIndexedCount)
	}
}

// Helper test functions
func beforeEach() {
	if DoesIndexExist(client, addressIndex) {
		DeleteIndex(client, addressIndex)
	}
	CreateIndex(client, "../shared/mapping/es_index_v_0_1.json", addressIndex)
}

func buildTestEsAddress(number int) mapping.EsAddress {
	return mapping.EsAddress{
		Number:       number,
		StreetPrefix: "W",
		Street:       "STREET NAME",
		StreetSuffix: "AVE",
		City:         "CHICAGO",
		State:        "IL",
		Zip5:         "60606",
		ZipLast4:     "1234",
		LatLong: mapping.LatLong{
			Longitude: 15.45,
			Latitude:  50.55,
		},
	}
}

func getResultCount() uint64 {
	var buf bytes.Buffer

	queryString := `{"query":{"match_all":{}}}`
	b, err := json.Marshal(queryString)
	if err != nil {
		log.Fatalf("Error marshalling ES query json %s", err)
	}
	_, _ = buf.Read(b)

	res, err := client.Search(
		client.Search.WithIndex(addressIndex),
		client.Search.WithBody(&buf),
		client.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		log.Fatalf("Error querying ES %s", err)
	}

	respBuffer := new(bytes.Buffer)
	_, _ = respBuffer.ReadFrom(res.Body)

	var summary EsSummary

	err = json.Unmarshal(respBuffer.Bytes(), &summary)
	if err != nil {
		log.Fatalf("Error unmarshalling ES result %s", err)
	}

	return summary.Hits.Total.ResultCount
}

type EsSummary struct {
	Hits struct {
		Total struct {
			ResultCount uint64 `json:"value"`
		} `json:"total"`
	} `json:"hits"`
}
