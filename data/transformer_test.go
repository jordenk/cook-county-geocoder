package data

import "testing"

func TestToEsAddressTransformsAnAddress(t *testing.T) {
	valid := Address{
		Number:       1234,
		StreetPrefix: "streetPrefix",
		Street:       "street",
		StreetSuffix: "streetSuffix",
		City:         "city",
		State:        "state",
		Zip5:         "zip5",
		ZipLast4:     "zipLast4",
		Longitude:    57.684512,
		Latitude:     -15.24568,
	}
	actual := ToEsAddress(valid)
	expected := EsAddress{
		Number:       1234,
		StreetPrefix: "streetPrefix",
		Street:       "street",
		StreetSuffix: "streetSuffix",
		City:         "city",
		State:        "state",
		Zip5:         "zip5",
		ZipLast4:     "zipLast4",
		LatLong:      LatLong{Latitude: -15.24568, Longitude: 57.684512},
	}
	if actual != expected {
		t.Errorf("Error transforming Address to EsAddress. actual: %v expected: %v", actual, expected)
	}
}
