package data

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// Represents an unvalidated, csv row.
type RawData struct {
	number       string
	streetPrefix string
	street       string
	streetSuffix string
	city         string
	state        string
	zip5         string
	zipLast4     string
	longitude    string
	latitude     string
}

// Represents a validated address.
type Address struct {
	number       int
	streetPrefix string
	street       string
	streetSuffix string
	city         string
	state        string
	zip5         string
	zipLast4     string
	// Rounding errors are not a problem for coordinates
	longitude float32
	latitude  float32
}

func CsvReader(fileName string) {
	csvFile, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Could not read CSV file: ", fileName, err)
	}
	reader := csv.NewReader(csvFile)

	// Check header
	headers, err := reader.Read()
	if err != nil {
		log.Fatal(err)
	}
	rawHeader := buildCookCountyRaw(headers)
	err = checkCookCountyHeaders(&rawHeader)
	if err != nil {
		log.Fatal(err)
	}

	for {
		// Read each record from csv
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		rawCsv := buildCookCountyRaw(record)
		err = checkRequiredFields(&rawCsv)
		if err != nil {
			log.Println(err)
			continue
		}
		// TODO validate address number, lat, long and transform
	}
}

// Validation functions for all data sources.
func checkRequiredFields(data *RawData) error {
	missingFields := make([]string, 0, 7)
	if isEmptyString(data.number) {
		missingFields = append(missingFields, "number")
	}
	if isEmptyString(data.street) {
		missingFields = append(missingFields, "street")
	}
	if isEmptyString(data.city) {
		missingFields = append(missingFields, "city")
	}
	if isEmptyString(data.state) {
		missingFields = append(missingFields, "state")
	}
	if isEmptyString(data.zip5) {
		missingFields = append(missingFields, "zip5")
	}
	if isEmptyString(data.longitude) {
		missingFields = append(missingFields, "longitude")
	}
	if isEmptyString(data.latitude) {
		missingFields = append(missingFields, "latitude")
	}
	if len(missingFields) > 0 {
		missing := fmt.Sprintf("missing required fields- %s raw data struct- %v", strings.Join(missingFields, ","), data)
		return errors.New(missing)
	}
	return nil
}

func isEmptyString(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

// Data source specific data extraction functions. Could be generic with data source specific parameters- cross that bridge
// when adding new source.
func checkCookCountyHeaders(data *RawData) error {
	expected := RawData{
		number:       "ADDRNOCOM",
		streetPrefix: "STNAMEPRD",
		street:       "STNAME",
		streetSuffix: "STNAMEPOT",
		city:         "USPSPN",
		state:        "USPSST",
		zip5:         "ZIP5",
		zipLast4:     "ZIP4",
		longitude:    "XPOSITION",
		latitude:     "YPOSITION",
	}
	if expected == *data {
		return nil
	}
	return fmt.Errorf("error mapping header columns. expected: %v actual :%v", expected, *data)
}

// This function must be called on both the header and non-header row to ensure data integrity.
func buildCookCountyRaw(row []string) RawData {
	return RawData{
		number:       row[3],
		streetPrefix: row[4],
		street:       row[5],
		streetSuffix: row[6],
		city:         row[10],
		state:        row[12],
		zip5:         row[13],
		zipLast4:     row[14],
		longitude:    row[21],
		latitude:     row[22],
	}
}
