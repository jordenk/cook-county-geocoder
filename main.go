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

	// TODO wire to ES module instead of writing files.
	go data.CsvReader(fileName, normalizedChannel, errorChannel, completeChannel)

	validAddress, err := os.Create("valid.jsonl")
	if err != nil {
		panic(err)
	}
	errors, err := os.Create("errors.jsonl")
	if err != nil {
		panic(err)
	}

	completeChannelOpen := true
	for completeChannelOpen {
		select {
		case n := <-normalizedChannel:
			str := fmt.Sprint(n)
			if _, err := validAddress.WriteString(str + "\n"); err != nil {
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
