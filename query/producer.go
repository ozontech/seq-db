package query

type RecordProducer interface {
	Next() (*Record, *Metadata)
	Release()
}

type Metadata struct {
	Err error
	// TODO: some additional info like explain data, tracing data, etc
}

type BatchedRecordProducer interface {
	NextBatch() ([]*Record, *Metadata)
}
