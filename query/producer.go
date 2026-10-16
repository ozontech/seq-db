package query

type RecordProducer interface {
	// Next returns the next record, or nil when the stream is exhausted. Errors
	// that occur during production are not returned here; they are accumulated
	// internally and reported via Finalize.
	Next() *Record
	// Finalize releases resources held by the producer and returns the final
	// summary gathered during the stream. It must be called exactly once after the
	// producer is exhausted.
	Finalize() *Summary
}

type Summary struct {
	Err   error
	Total uint64
	// NotExists is the aggregation-level count of documents that have the
	// aggregated field but no group-by token; zero for document streams.
	NotExists uint64
}
