package resources

const poolBuckets = 24

var (
	BytesPool        = NewSizedPool[byte](poolBuckets)
	Uint32SlicesPool = NewSizedPool[[]uint32](poolBuckets)
	BytesSlicesPool  = NewSizedPool[[]byte](poolBuckets)
)
