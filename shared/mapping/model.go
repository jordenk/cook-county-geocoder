package mapping

// EsAddress is the representation of the data in ElasticSearch document form
type EsAddress struct {
	Number       int     `json:"number"`
	StreetPrefix string  `json:"street_prefix"`
	Street       string  `json:"street"`
	StreetSuffix string  `json:"street_suffix"`
	City         string  `json:"city"`
	State        string  `json:"state"`
	Zip5         string  `json:"zip_5"`
	ZipLast4     string  `json:"zip_last_4"`
	LatLong      LatLong `json:"lat_long"`
}

type LatLong struct {
	Longitude float64 `json:"lon"`
	Latitude  float64 `json:"lat"`
}

