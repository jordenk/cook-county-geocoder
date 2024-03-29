package main

import (
	"cook-county-geocoder/data"
	"cook-county-geocoder/shared/mapping"
	"fmt"
	"os"
)

func main() {
	// TODO Use ENV VARs or CLI to toggle
	//dataModule()
	apiModule()
}

func apiModule() {

}

func dataModule() {
	// TODO will need to read from s3
	fileName := "data/Address_Points.csv"
	normalizedChannel := make(chan data.Address)
	errorChannel := make(chan string)
	completeChannel := make(chan bool)

	go data.CsvReader(fileName, normalizedChannel, errorChannel, completeChannel)

	// TODO write to a configurable output. Local file or S3.
	errors, err := os.OpenFile("data/normalize_errors.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}

	// Quick hack. TODO use a channel instead of building a huge slice.
	bigSlice := make([]mapping.EsAddress, 2341113)

	completeChannelOpen := true
	for completeChannelOpen {
		select {
		case n := <-normalizedChannel:
			esDoc := data.ToEsAddress(n)
			bigSlice = append(bigSlice, esDoc)
		case e := <-errorChannel:
			str := fmt.Sprint(e)
			if _, err := errors.WriteString(str + "\n"); err != nil {
				panic(err)
			}
		case completeChannelOpen = <-completeChannel:
		}
	}

	_ = errors.Close()

	// TODO Requires index to be manually created, for now.
	client := data.BuildEsClient([]string{"localhost:9200"})
	data.BulkIndexEs(client, "address", bigSlice)
}
