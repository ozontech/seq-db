package exec

import "github.com/ozontech/seq-db/query"

type Limiter struct {
	input query.RecordProducer

	limit  uint32
	offset uint32

	produced uint32
	skipped  uint32
}

func NewLimiter(
	input query.RecordProducer,
	limit uint32,
	offset uint32,
) *Limiter {
	return &Limiter{
		input:  input,
		limit:  limit,
		offset: offset,
	}
}

func (l *Limiter) Next() *query.Record {
	for l.skipped < l.offset {
		r := l.input.Next()
		if r == nil {
			return nil
		}
		l.skipped++
	}

	// limit == 0 means no limit
	if l.limit != 0 && l.produced >= l.limit {
		return nil
	}

	r := l.input.Next()
	if r == nil {
		return nil
	}

	l.produced++

	return r
}

func (l *Limiter) Finalize() *query.Summary {
	return l.input.Finalize()
}
