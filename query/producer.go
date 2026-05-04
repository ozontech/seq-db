package query

type RecordProducer interface {
	// TODO: Next() (*Record, metadata) // meta has error plus some additional info
	Next() (*Record, bool) // TODO: record as interface (???)
	// TODO: we need a method to release all the resources down the producer call stack
}

type BatchedRecordProducer interface {
	NextBatch() ([]*Record, bool)
}
