package main

import "cook-county-geocoder/data"

func main() {
	fileName := "data/Address_Points.csv"
	data.CsvReader(fileName)
}