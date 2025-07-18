package tracing

import (
	"context"
	"net/http"

	"go.opencensus.io/trace"

	"github.com/ozontech/seq-db/consts"
)

func isDebug(r *http.Request) bool {
	header := r.Header.Get(consts.DebugHeader)
	return header != ""
}

func HTTPSpan(r *http.Request, name string, coeff float64) (context.Context, *trace.Span) {
	ops := []trace.StartOption{}
	debug := isDebug(r)
	if debug {
		ops = append(ops, trace.WithSampler(trace.AlwaysSample()))
	} else {
		ops = append(ops, trace.WithSampler(trace.ProbabilitySampler(defaultProbability*coeff)))
	}

	ctx, span := trace.StartSpan(r.Context(), name, ops...)
	if debug {
		span.AddAttributes(trace.BoolAttribute(consts.JaegerDebugKey, true))
		ctx = context.WithValue(ctx, consts.JaegerDebugKey, true)
	}
	return ctx, span
}
