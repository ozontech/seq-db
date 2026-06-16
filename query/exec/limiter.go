package exec

import "github.com/ozontech/seq-db/query"

type Limiter struct {
	input query.RecordProducer

	limit    uint32
	produced uint32
}

func NewLimiter(
	input query.RecordProducer,
	limit uint32,
) *Limiter {
	return &Limiter{
		input: input,
		limit: limit,
	}
}

func (l *Limiter) Next() (*query.Record, *query.Metadata) {
	if l.produced >= l.limit {
		return nil, nil
	}

	r, meta := l.input.Next()
	if meta != nil {
		return nil, meta
	}
	if r == nil {
		return nil, nil
	}

	l.produced++

	return r, nil
}

func (l *Limiter) Release() {
	l.input.Release()
}
