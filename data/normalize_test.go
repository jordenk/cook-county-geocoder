package data

import (
	"testing"
)

func TestCheckRequiredFieldsWithValidInput(t *testing.T) {
	validRawInput := RawData{
		number:       "non empty string",
		streetPrefix: "",
		street:       "non empty string",
		streetSuffix: "",
		city:         "non empty string",
		state:        "non empty string",
		zip5:         "non empty string",
		zipLast4:     "",
		longitude:    "non empty string",
		latitude:     "non empty string",
	}
	err := checkRequiredFields(&validRawInput)
	if  err != nil {
		t.Errorf("Expected no errors when required fields are non empty strings. Found %v", err)
	}
}

func TestCheckRequiredFieldsWithMissingFields(t *testing.T) {
	inputMissingRequired := RawData{
		number:       "",
		streetPrefix: "",
		street:       "",
		streetSuffix: "",
		city:         "",
		state:        "",
		zip5:         "",
		zipLast4:     "",
		longitude:    "",
		latitude:     "",
	}
	if checkRequiredFields(&inputMissingRequired) == nil {
		t.Errorf("Expected error when missing required fields. No error returned.")
	}
}

func TestCookCountyHeadersWithValidInput(t *testing.T) {
	validHeaders := RawData{
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
	err := checkCookCountyHeaders(&validHeaders)
	if  err != nil {
		t.Errorf("Expected no errors when headers are in the correct location. Found %v", err)
	}
}

func TestCookCountyHeadersInvalidHeaderLocation(t *testing.T) {
	inputMissingRequired := RawData{
		number:       "numberInvalid",
		streetPrefix: "streetPrefixInvalid",
		street:       "streetInvalid",
		streetSuffix: "streetSuffixInvalid",
		city:         "cityInvalid",
		state:        "stateInvalid",
		zip5:         "zip5Invalid",
		zipLast4:     "zipLast4Invalid",
		longitude:    "longitudeInvalid",
		latitude:     "latitudeInvalid",
	}
	if checkCookCountyHeaders(&inputMissingRequired) == nil {
		t.Errorf("Expected error when passed unexpected headers. No error returned.")
	}
}

func TestBuildCookCountryRow(t *testing.T) {
	input := []string{
		"0",
		"1",
		"2",
		"number",
		"streetPrefix",
		"street",
		"streetSuffix",
		"7",
		"8",
		"9",
		"city",
		"11",
		"state",
		"zip5",
		"zipLast4",
		"15",
		"16",
		"17",
		"18",
		"19",
		"20",
		"longitude",
		"latitude",
		"23",
		"24",
	}
	expected := RawData{
		number:       "number",
		streetPrefix: "streetPrefix",
		street:       "street",
		streetSuffix: "streetSuffix",
		city:         "city",
		state:        "state",
		zip5:         "zip5",
		zipLast4:     "zipLast4",
		longitude:    "longitude",
		latitude:     "latitude",
	}

	actual := buildCookCountyRaw(input)
	if actual != expected {
		t.Errorf("Error building raw data struct. actual: %v expected: %v", actual, expected)
	}
}


