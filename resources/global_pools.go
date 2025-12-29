package resources

var (
	BytesPool        = NewSizedPool[byte](24)
	Uint32SlicesPool = NewSizedPool[[]uint32](24)
	BytesSlicesPool  = NewSizedPool[[]byte](24)
)
