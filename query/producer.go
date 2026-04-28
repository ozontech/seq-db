package query

type RecordProducer interface {
	// TODO: Next() (*Record, metadata) // где в мете ошибка + какая-нибудь дополнительная инфа
	Next() (*Record, bool) // TODO: record as interface (???)
}

type BatchedRecordProducer interface {
	NextBatch() ([]*Record, bool)
}
