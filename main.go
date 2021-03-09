package main

import (
	"cook-county-geocoder/data"
	"fmt"
	"os"
)

func main() {
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
	bigSlice := make([]data.EsAddress, 2341113)

	completeChannelOpen := true
	for completeChannelOpen {
		select {
		case n := <-normalizedChannel:
			esDoc := data.Address.ToEsAddress(n)
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
	client := data.BuildEsClient()
	data.BulkIndexEs(client, "address", &bigSlice)
	// TODO log or collect errors 2021/03/09 08:01:28 Indexed [3,625,463] documents with [56,765] errors in 46.814s (77443 docs/sec)
}
