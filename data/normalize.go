package data

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
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
	longitude float64
	latitude  float64
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

		_, err = transformRawToAddress(&rawCsv)
		if err != nil {
			log.Println(err)
			continue
		}

		// TODO send address to channel. May need to also send a complete signal with valid address count.
	}
}

// Validation functions for all data sources.

// checkRequiredFields inspects required fields and combines missing fields into a single error message.
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

// transformRawToAddress converts RawData strings to the desired data type, eagerly returning errors. If all validation
// is passed, then an Address is returned.
func transformRawToAddress(raw *RawData) (Address, error) {
	const MaxLocation = 90.0
	const MinLocation = -90.0

	// TODO clean number for letters, .5, 1/2
	num, err := strconv.Atoi(raw.number)
	if err != nil {
		return Address{}, fmt.Errorf("could not parse address number to int. number- %s full struct- %v", raw.number, raw)
	}

	long, err := strconv.ParseFloat(raw.longitude, 64)
	if err != nil {
		return Address{}, fmt.Errorf("could not parse address longitude to float64. longitude- %s full struct- %v", raw.longitude, raw)
	}

	if long > MaxLocation || long < MinLocation {
		return Address{}, fmt.Errorf("longitude is outside of logical range. longitude- %f full struct- %v", long, raw)
	}

	lat, err := strconv.ParseFloat(raw.latitude, 64)
	if err != nil {
		return Address{}, fmt.Errorf("could not parse address latitude to float64. latitude- %s full struct- %v", raw.latitude, raw)
	}

	if lat > MaxLocation || lat < MinLocation {
		return Address{}, fmt.Errorf("latitude is outside of logical range. latitude- %f full struct- %v", lat, raw)
	}

	validAddress := Address{
		number:       num,
		streetPrefix: raw.streetPrefix,
		street:       raw.street,
		streetSuffix: raw.streetSuffix,
		city:         raw.city,
		state:        raw.state,
		zip5:         raw.zip5,
		zipLast4:     raw.zipLast4,
		longitude:    long,
		latitude:     lat,
	}
	return validAddress, nil
}

func isEmptyString(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

// Data source specific data extraction functions. Could be generic with data source specific parameters- cross that bridge
// when adding new source.

// checkCookCountyHeaders is called on the header row of the Cook County CSV to make sure columns are correctly mapped.
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

// buildCookCountyRaw contains the column mapping for the Cook County CSV data. It must be called on both the header
// and non-header row to ensure data integrity.
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
