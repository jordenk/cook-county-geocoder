package data

// Address is a normalized address with the minimum required fields.
type Address struct {
	Number       int
	StreetPrefix string
	Street       string
	StreetSuffix string
	City         string
	State        string
	Zip5         string
	ZipLast4     string
	Longitude    float64
	Latitude     float64
}
