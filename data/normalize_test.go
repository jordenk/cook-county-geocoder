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
	if err != nil {
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
	if err != nil {
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

func TestTransformRawToAddressWithValidInput(t *testing.T) {
	validRawInput := buildRawData("1234", "57.684512", "-15.24568")

	expected := Address{
		number:       1234,
		streetPrefix: "streetPrefix",
		street:       "street",
		streetSuffix: "streetSuffix",
		city:         "city",
		state:        "state",
		zip5:         "zip5",
		zipLast4:     "zipLast4",
		longitude:    57.684512,
		latitude:     -15.24568,
	}


	actual, err := transformRawToAddress(&validRawInput)
	if err != nil {
		t.Errorf("Expected no errors with valid input. Found %v", err)
	}

	if actual != expected {
		t.Errorf("Error building address data struct. actual: %v expected: %v", actual, expected)
	}
}


func TestTransformRawToAddressWithNumericParsingErrors(t *testing.T) {
	rawDataBadNumber := buildRawData("bad number", "57.684512", "-15.24568")
	_, err := transformRawToAddress(&rawDataBadNumber)

	if err == nil {
		t.Errorf("Expected error when passed a RawData struct with an invalid number value. No error returned.")
	}

	rawDataInvalidLongitude := buildRawData("1234", "bad longitude", "-15.24568")
	_, err = transformRawToAddress(&rawDataInvalidLongitude)

	if err == nil {
		t.Errorf("Expected error when passed a RawData struct with a invalid longitude value. No error returned.")
	}

	rawDataInvalidLatitude := buildRawData("bad number", "57.684512", "-15.24.56.8")
	_, err = transformRawToAddress(&rawDataInvalidLatitude)

	if err == nil {
		t.Errorf("Expected error when passed a RawData struct with a invalid latitude value. No error returned.")
	}
}

func TestTransformRawToAddressWithOutOfRangeLatLongValues(t *testing.T) {
	rawDataLowLatitude := buildRawData("1234", "57.684512", "-90.24568")
	_, err := transformRawToAddress(&rawDataLowLatitude)

	if err == nil {
		t.Errorf("Expected error when passed a RawData struct with lower out of range latitude. No error returned.")
	}

	rawDataHighLatitude := buildRawData("1234", "57.684512", "90.24568")
	_, err = transformRawToAddress(&rawDataHighLatitude)

	if err == nil {
		t.Errorf("Expected error when passed a RawData struct with higher out of range latitude. No error returned.")
	}

	rawDataLowLongitude := buildRawData("1234", "-90.0001", "-15.24568")
	_, err = transformRawToAddress(&rawDataLowLongitude)

	if err == nil {
		t.Errorf("Expected error when passed a RawData struct with lower out of range longitude. No error returned.")
	}

	rawDataHighLongitude := buildRawData("1234", "90.0001", "15.24568")
	_, err = transformRawToAddress(&rawDataHighLongitude)

	if err == nil {
		t.Errorf("Expected error when passed a RawData struct with higher out of range longitude. No error returned.")
	}
}

func buildRawData(number string, longitude string, latitude string) RawData {
	return RawData{
		number:       number,
		streetPrefix: "streetPrefix",
		street:       "street",
		streetSuffix: "streetSuffix",
		city:         "city",
		state:        "state",
		zip5:         "zip5",
		zipLast4:     "zipLast4",
		longitude:    longitude,
		latitude:     latitude,
	}
}
