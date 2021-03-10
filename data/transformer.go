package data

// Transformer is a simple file for now. This layer is separated to house more complex scoring logic and combining
// data from different sources.
func ToEsAddress(address Address) EsAddress {
	return EsAddress{
		Number:       address.Number,
		StreetPrefix: address.StreetPrefix,
		Street:       address.Street,
		StreetSuffix: address.StreetSuffix,
		City:         address.City,
		State:        address.State,
		Zip5:         address.Zip5,
		ZipLast4:     address.ZipLast4,
		LatLong:      LatLong{Latitude: address.Latitude, Longitude: address.Longitude},
	}
}
