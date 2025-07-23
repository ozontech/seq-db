package parser

import (
	"fmt"
	"net/netip"
	"strings"

	"github.com/ozontech/seq-db/seq"
)

type IpRange struct {
	Field string
	From  Term
	To    Term
}

func (n *IpRange) Dump(builder *strings.Builder) {
	builder.WriteString(quoteTokenIfNeeded(n.Field))
	builder.WriteString(`:ip_range(`)

	n.From.DumpSeqQL(builder)
	builder.WriteString(", ")
	n.To.DumpSeqQL(builder)

	builder.WriteString(`)`)
}

func (n *IpRange) DumpSeqQL(b *strings.Builder) {
	b.WriteString(quoteTokenIfNeeded(n.Field))
	b.WriteString(`:ip_range(`)

	n.From.DumpSeqQL(b)
	b.WriteString(", ")
	n.To.DumpSeqQL(b)

	b.WriteString(`)`)
}

// parseFilterIpRange parses 'ip_range' filter.
// TODO
// Filter 'in' is a logical OR of multiple Literal.
// It supports all forms of seq-ql string literals.
// Example queries:
//
//	service:in(auth-api, api-gateway, clickhouse-shard-*)
//	phone:in(`+7 999 ** **`, '+995'*)
func parseFilterIpRange(lex *lexer, fieldName string, t seq.TokenizerType, caseSensitive bool) (*IpRange, error) {
	r := &IpRange{Field: fieldName}
	if !lex.IsKeyword("(") {
		return r, fmt.Errorf("expect '(', got %q", lex.Token)
	}
	lex.Next()

	if lex.IsKeyword(")") {
		return r, fmt.Errorf("empty 'ip_range' filter")
	}

	ipFrom, err := parseIpAddr(lex)
	if err != nil {
		return r, err
	}

	if !lex.IsKeywords(",") {
		return r, fmt.Errorf("expected ',' keyword, got %q", lex.Token)
	}

	lex.Next()

	ipTo, err := parseIpAddr(lex)
	if err != nil {
		return r, err
	}

	if ipFrom.Compare(ipTo) > 0 {
		return r, fmt.Errorf("first ip must be less then second")
	}

	r.From = newTextTerm(ipFrom.String())
	r.To = newTextTerm(ipTo.String())

	//ipPrefix, err := netip.ParsePrefix(tok)
	//if err != nil {
	//	return nil, fmt.Errorf("expected ip address or mask in cidr notation, got %q", tok)
	//}

	//fmt.Println(ipPrefix.Bits())

	// trying to parse ip address e.g. '192.168.1.1'
	//ipFrom, err := netip.ParseAddr(tok)
	//if err != nil {
	//	// if failed to parse ip address, trying to parse ip prefix in cidr notation e.g. '192.168.1.1/24'
	//
	//
	//	// todo
	//}

	if !lex.IsKeyword(")") {
		return r, fmt.Errorf("expect ')', got %q", lex.Token)
	}

	lex.Next()
	return r, nil
}

func parseIpAddr(lex *lexer) (netip.Addr, error) {
	ip, err := parseCompositeToken(lex)
	if err != nil {
		return netip.Addr{}, err
	}

	return netip.ParseAddr(ip)
}
