package main

import (
	"cook-county-geocoder/data"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	fileName := "data/Address_Points.csv"
	normalizedChannel := make(chan data.Address)
	errorChannel := make(chan string)
	completeChannel := make(chan bool)

	go data.CsvReader(fileName, normalizedChannel, errorChannel, completeChannel)

	// TODO wire to ES module instead of writing files.
	validAddress, err := os.OpenFile("data/valid.jsonl", 1, 0666)
	if err != nil {
		panic(err)
	}
	errors, err := os.OpenFile("data/errors.jsonl", 1, 0666)
	if err != nil {
		panic(err)
	}

	completeChannelOpen := true
	for completeChannelOpen {
		select {
		case n := <-normalizedChannel:
			esDoc := data.Address.ToEsAddress(n)
			b, _ := json.Marshal(esDoc)
			if _, err := validAddress.WriteString(string(b) + "\n"); err != nil {
				panic(err)
			}
		case e := <-errorChannel:
			str := fmt.Sprint(e)
			if _, err := errors.WriteString(str + "\n"); err != nil {
				panic(err)
			}
		case completeChannelOpen = <-completeChannel:
		}
	}

	_ = validAddress.Close()
	_ = errors.Close()
}
