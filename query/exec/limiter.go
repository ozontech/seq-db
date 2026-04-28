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

func (l *Limiter) Next() (*query.Record, bool) {
	if l.produced >= l.limit {
		return nil, false
	}

	r, has := l.input.Next()
	if !has {
		return nil, false
	}

	l.produced++

	return r, true
}
