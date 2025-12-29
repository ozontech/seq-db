package resources

var (
	BytesPool        = NewSizedPool[byte](16)
	Uint32SlicesPool = NewSizedPool[[]uint32](16)
	BytesSlicesPool  = NewSizedPool[[]byte](16)
)
