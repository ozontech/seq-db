package parser

import (
	"strings"
)

type Range struct {
	Field       string
	From        Term
	To          Term
	IncludeFrom bool
	IncludeTo   bool
}

func (n *Range) Dump(builder *strings.Builder) {
	builder.WriteString(n.Field)
	builder.WriteString(`:`)
	if n.IncludeFrom {
		builder.WriteByte('[')
	} else {
		builder.WriteByte('{')
	}
	n.From.Dump(builder)
	builder.WriteString(" TO ")
	n.To.Dump(builder)
	if n.IncludeTo {
		builder.WriteByte(']')
	} else {
		builder.WriteByte('}')
	}
}
