package query

type RecordProducer interface {
	Next() (*Record, *Metadata)
	// TODO: we need a method to release all the resources down the producer call stack
	// Release()
}

type Metadata struct {
	Err error
	// TODO: some additional info like explain data, tracing data, etc
}

type BatchedRecordProducer interface {
	NextBatch() ([]*Record, *Metadata)
}
