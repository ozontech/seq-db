package parser

import (
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"go4.org/netipx"
)

type IPRange struct {
	Field string
	From  Term
	To    Term
}

func (n *IPRange) Dump(builder *strings.Builder) {
	builder.WriteString(quoteTokenIfNeeded(n.Field))
	builder.WriteString(`:ip_range(`)

	n.From.Dump(builder)
	builder.WriteString(", ")
	n.To.Dump(builder)

	builder.WriteString(`)`)
}

func (n *IPRange) DumpSeqQL(b *strings.Builder) {
	b.WriteString(quoteTokenIfNeeded(n.Field))
	b.WriteString(`:ip_range(`)

	n.From.DumpSeqQL(b)
	b.WriteString(", ")
	n.To.DumpSeqQL(b)

	b.WriteString(`)`)
}

// parseFilterIPRange parses 'ip_range' filter.
// It supports only either 2 ip addresses or ip address in CIDR notation.
// Example queries:
//
//	host_addr:ip_range(192.168.0.1, 192.168.0.255)
//	host_addr:ip_range(192.168.0.0/24)
func parseFilterIPRange(lex *lexer, fieldName string) (*IPRange, error) {
	if !lex.IsKeyword("(") {
		return nil, fmt.Errorf("expected '(', got %q", lex.Token)
	}
	lex.Next()

	if lex.IsKeyword(")") {
		return nil, errors.New("empty 'ip_range' filter")
	}

	tok, err := parseCompositeToken(lex, '/')
	if err != nil {
		return nil, err
	}

	var r netipx.IPRange
	if strings.ContainsRune(tok, '/') {
		prefix, err := netip.ParsePrefix(tok)
		if err != nil {
			return nil, err
		}
		r = netipx.RangeOfPrefix(prefix)
	} else {
		from, err := netip.ParseAddr(tok)
		if err != nil {
			return nil, err
		}

		if !lex.IsKeywords(",") {
			return nil, fmt.Errorf("expected ',' keyword, got %q", lex.Token)
		}

		lex.Next()

		tok, err := parseCompositeToken(lex)
		if err != nil {
			return nil, err
		}

		to, err := netip.ParseAddr(tok)
		if err != nil {
			return nil, err
		}

		if from.Compare(to) >= 0 {
			return nil, fmt.Errorf("first ip %q is greater or equal than second ip %q", from, to)
		}

		r = netipx.IPRangeFrom(from, to)
	}

	if !lex.IsKeyword(")") {
		return nil, fmt.Errorf("expected ')', got %q", lex.Token)
	}

	lex.Next()

	return &IPRange{
		Field: fieldName,
		From:  newTextTerm(r.From().String()),
		To:    newTextTerm(r.To().String()),
	}, nil
}
