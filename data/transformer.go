package data

// Transformer is a simple file for now. This layer is separated to house more complex scoring logic and combining
// data from different sources.
func (receiver Address) ToEsAddress() EsAddress {
	return EsAddress{
		Number:       receiver.Number,
		StreetPrefix: receiver.StreetPrefix,
		Street:       receiver.Street,
		StreetSuffix: receiver.StreetSuffix,
		City:         receiver.City,
		State:        receiver.State,
		Zip5:         receiver.Zip5,
		ZipLast4:     receiver.ZipLast4,
		LatLong:      LatLong{Latitude: receiver.Latitude, Longitude: receiver.Longitude},
	}
}
