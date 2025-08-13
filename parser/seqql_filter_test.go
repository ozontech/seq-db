package parser

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/seq"
)

func TestLexer(t *testing.T) {
	test := func(q string, tokens []string) {
		t.Helper()

		lex := newLexer(q)
		lex.Next()
		i := 0
		for ; !lex.IsEnd(); i++ {
			require.Equalf(t, tokens[i], lex.Token, "unexpected token at position %d", i)
			lex.Next()
		}
		require.Equal(t, len(tokens), i)
	}

	// Simple cases.
	test("service:clickhouse AND level:3", []string{"service", ":", "clickhouse", "AND", "level", ":", "3"})
	test("service:clickhouse-?", []string{"service", ":", "clickhouse", "-", "?"})
	test("#", []string{})
	test("*", []string{string(wildcardRune)})
	test(`'\*'`, []string{"*"})
	test("?", []string{"?"})

	// Test quotes.
	test("service:`clickhouse*`", []string{"service", ":", "clickhouse*"})
	test(`service:clickhouse*`, []string{"service", ":", "clickhouse", string(wildcardRune)})
	test("service:'clickhouse*'", []string{"service", ":", "clickhouse" + string(wildcardRune)})
	test("service:'clickhouse\\*'", []string{"service", ":", "clickhouse*"})
}

func TestSeqQLAll(t *testing.T) {
	t.Parallel()

	mapping := seq.Mapping{
		"k8s_namespace":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"k8s_pod":         seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"service":         seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"level":           seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"message":         seq.NewSingleType(seq.TokenizerTypeText, "", 0),
		"text":            seq.NewSingleType(seq.TokenizerTypeText, "", 0),
		"keyword":         seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"уровень":         seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"x-forwarded-for": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"user-agent":      seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"#":               seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"*":               seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"m":               seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"OR":              seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}
	test := func(originalQuery, expected string) {
		t.Helper()
		parsedOriginal, err := ParseSeqQL(originalQuery, mapping)
		require.NoError(t, err)

		gotOriginal := parsedOriginal.SeqQLString()
		require.Equal(t, expected, gotOriginal)

		parsedGot, err := ParseSeqQL(gotOriginal, mapping)
		require.NoError(t, err)

		require.Equal(t, gotOriginal, parsedGot.SeqQLString())
		require.Equal(t, parsedOriginal, parsedGot)
	}

	test("*", "*")

	// Propagate "not".
	test(`NOT NOT text:a`, `text:a`)
	test(`text:a AND NOT NOT text:b`, `(text:a and text:b)`)
	test(`text:a AND NOT text:b`, `(not text:b and text:a)`)
	test(`NOT NOT text:a OR text:b`, `(text:a or text:b)`)
	test(`NOT text:a OR text:b`, `(not (not text:b and text:a))`)
	test(`NOT (NOT text:a AND NOT text:b)`, `(text:a or text:b)`)
	test(`NOT text:a OR text:b OR text:c OR text:d`, `(not (not text:d and (not text:c and (not text:b and text:a))))`)
	test(`text:a OR text:b OR text:c OR NOT text:d`, `(not (not ((text:a or text:b) or text:c) and text:d))`)
	test(`NOT text:a AND text:b AND text:c AND text:d`, `(((not text:a and text:b) and text:c) and text:d)`)
	test(`text:a AND text:b AND text:c AND NOT text:d`, `(not text:d and ((text:a and text:b) and text:c))`)
	test(`NOT ((NOT text:a OR (NOT text:b AND text:c)) AND (NOT text:d AND NOT text:e))`, `((not (not text:b and text:c) and text:a) or (text:d or text:e))`)

	// Test fulltext search filter.
	test(`service:some`, `service:some`)
	test(`service:"some text"`, `service:"some text"`)
	test(`text:"some text"`, `(text:some and text:text)`)
	test(`text:"some very long text"`, `(((text:some and text:very) and text:long) and text:text)`)
	test(`text:"a b" AND text:"c d f" or text:"e f"`, `(((text:a and text:b) and ((text:c and text:d) and text:f)) or (text:e and text:f))`)
	test(`service:some AND level:[1, 3] AND level:[3, 5]`, `((service:some and level:[1, 3]) and level:[3, 5])`)

	// Test wildcards and asterisks.
	test(`service:"some*"`, `service:some*`)
	test(`service:some*`, `service:some*`)
	test(`service:"some*thing"`, `service:some*thing`)
	test(`service:some*thing`, `service:some*thing`)
	test(`service:"some*thing*"`, `service:some*thing*`)
	test(`service:some*thing*`, `service:some*thing*`)
	test(`service:*thing*`, `service:*thing*`)
	test(`service:"*"`, `service:*`)
	test(`service:*`, `service:*`)
	test(`service:"cms"*"inter"*"api"`, `service:cms*inter*api`)

	// Test keyword wildcards.
	// In the old query language "**" is not allowed.
	test(`service:**`, `service:**`)
	test(`service:a**`, `service:a**`)
	test(`service:**b`, `service:**b`)
	test(`service:a**b`, `service:a**b`)

	// Test tokenization.
	test(`service:abc`, `service:abc`)
	test(`service:"quoted"`, `service:quoted`)
	test(`service:"quoted spaces"`, `service:"quoted spaces"`)
	test(`service:'"symbols"'`, `service:"\"symbols\""`)
	test(`service:"[1 TO 3]"`, `service:"[1 to 3]"`)
	test(`  service  :   hi  `, `service:hi`)
	test(`service:""`, `service:""`)

	// Test composite token.
	test(`keyword:'#''$'"^"`, `keyword:"#$^"`)
	test(`message:'#''$'"^"`, `message:""`)
	test(`'#':'#'`, `"#":"#"`)
	test(`"*":"*"`, `"\*":*`)
	test("`*`:`*`", `"\*":"\*"`)
	test(`m:a AND OR : r`, `(m:a and "OR":r)`)

	// Test range filter.
	test(`level:[1, 3]`, `level:[1, 3]`)
	test(`level:[*, 3]`, `level:[*, 3]`)
	test(`level:["*", 3]`, `level:[*, 3]`)
	test(`level:(1, "*"]`, `level:(1, *]`)
	test(`level:(1, *]`, `level:(1, *]`)
	test(`level:[1, 3] AND service:["*", "*"]`, `(level:[1, 3] and service:[*, *])`)
	test(`level:["from", "to"]`, `level:[from, to]`)
	test(`level:[from, to]`, `level:[from, to]`)
	test(`level:["a b c", "d e f"]`, `level:["a b c", "d e f"]`)
	test(`level:["hi", "ho"]`, `level:[hi, ho]`)
	test(`level:["-123", -456]`, `level:[-123, -456]`)
	test(`  level  :  [  1  ,  3  ]  `, `level:[1, 3]`)
	test(`level:["", "a\*b"]`, `level:["", "a\*b"]`)
	test(`level:["-3", 6) OR (service:"hel lo" AND level:[1, 3])`, `(level:[-3, 6) or (service:"hel lo" and level:[1, 3]))`)

	// Parsing AST.
	test(`service:"wms-svc-logistics-megasort" and level:""#`, `(service:wms-svc-logistics-megasort and level:"")`)
	test(`service: composer-api`, `service:composer-api`)
	test(`  service    : a   or   level     :   3  `, `(service:a or level:3)`)
	test(`service: a or level: 3 AND text:b`, `(service:a or (level:3 and text:b))`)
	test(`service: a or level: 3 or text:b`, `((service:a or level:3) or text:b)`)
	test(` not  service : a `, `(not service:a)`)
	test(`service:a or not service:b or service:c`, `(not (not service:c and (not service:a and service:b)))`)
	test(`not (service:a or service:c)`, `(not (service:a or service:c))`)
	test(`NOT Not service:a`, `service:a`)
	test(`service:*`, `service:*`)
	test(` service : * `, `service:*`)
	test(`service:a or service:b AND NOT service:c`, `(service:a or (not service:c and service:b))`)

	// Test comments.
	test(`#
# search by logistics-megasort service
service:"wms-svc-logistics-megasort" and level:"#"
# end of query`, `(service:wms-svc-logistics-megasort and level:"#")`)

	// Test text wildcards.
	test(`text:some*thing`, `text:some*thing`)
	test(`text:"a**b**"`, `text:a**b**`)
	test(`text:"some* weird* *cases"`, `((text:some* and text:weird*) and text:*cases)`)
	test(`text:"some *weird cases* hmm very*intrs"`, `((((text:some and text:*weird) and text:cases*) and text:hmm) and text:very*intrs)`)
	test(`text:"val*" AND text:"val\**"`, `(text:val* and text:"val\*"*)`)

	// Test complex search with wildcards.
	test(`text:"\*\**"`, `text:"\*\*"*`)
	test(`text:'value=*' AND text:'value="\*"*'`, `((text:value and text:*) and ((text:value and text:"\*") and text:*))`)
	test(`text:value'="\*\*"*' AND text:"\*\*"`, `(((text:value and text:"\*\*") and text:*) and text:"\*\*")`)
	test(`text:'value=*' AND text:'value="\*"*' AND text:'value="\*\*"*' AND text:"\*\*" AND text:"\*\**"`,
		`(((((text:value and text:*) and ((text:value and text:"\*") and text:*)) and ((text:value and text:"\*\*") and text:*)) and text:"\*\*") and text:"\*\*"*)`)

	// Test escape.
	test("keyword:`+7 995 28 07`", "keyword:\"+7 995 28 07\"")
	test("keyword:'+7 995 28 07'", "keyword:\"+7 995 28 07\"")
	test("keyword:`+7 995 ** **`", `keyword:"+7 995 \*\* \*\*"`)
	test("keyword:`+7 995 \\** **`", `keyword:"+7 995 \\\*\* \*\*"`)
	test("keyword:`\\t`", `keyword:"\\t"`)
	test(`keyword:"\t"`, `keyword:"\t"`)
	test(`keyword:"\\t"`, `keyword:"\\t"`)
	test(`keyword:"'\n\t'"`, `keyword:"'\n\t'"`)
	test(`keyword:"kafka_impl/producer.go:84"`, `keyword:"kafka_impl/producer.go:84"`)
	test(`keyword:"\/ready"`, `keyword:"\\/ready"`)
	test(`message:'7916\*\*\*\*\*79'`, `message:"7916\*\*\*\*\*79"`)
	test(`keyword:"a\*b"`, `keyword:"a\*b"`)
	test(`message:"a\*b"`, `message:"a\*b"`)
	test(`keyword:"\U0001F3CC"`, `keyword:"🏌"`)

	// Test UTF8.
	test(`text:"Произошла ошибка"`, `(text:произошла and text:ошибка)`)
	test("text:`Произошла ошибка: недостаточно места на диске`", `(((((text:произошла and text:ошибка) and text:недостаточно) and text:места) and text:на) and text:диске)`)
	test("уровень:'😖'", `уровень:"😖"`)

	// Test range.
	test(`level:[1, 3]`, `level:[1, 3]`)
	test(`level:(1, 3)`, `level:(1, 3)`)
	test(`level:["*", "*"]`, `level:[*, *]`)
	test(`level:[*, *]`, `level:[*, *]`)
	test(`level:[abc, cbd]`, `level:[abc, cbd]`)

	// Test separators without quotes.
	test(`service:clickhouse-shard-1`, `service:clickhouse-shard-1`)
	test(`x-forwarded-for: abc`, `x-forwarded-for:abc`)
	test(`user-agent:"ozondeliveryapp_ios_prod"`, `user-agent:ozondeliveryapp_ios_prod`)

	// Test filter 'in'.
	test(`service:in(auth-api, api-gateway, clickhouse-shard-*)`, `((service:auth-api or service:api-gateway) or service:clickhouse-shard-*)`)
	test(`service:in(*, *, *)`, `((service:* or service:*) or service:*)`)
	test(`service:in(*)`, `service:*`)
	test(`level:in(1)`, `level:1`)
	test(`level:in(1, '2', 'three')`, `((level:1 or level:2) or level:three)`)
	test(`level:in(1, '2', ''*3*"")`, `((level:1 or level:2) or level:*3*)`)
	test(`level:in(""''''"", ****','","****"*")`, `(level:"" or level:****",,"*****)`)
	test(`level:in(one, t,wo)`, `((level:one or level:t) or level:wo)`)
	test(`level:"in(one, t,wo)"`, `level:"in(one, t,wo)"`)
	test(`level:error and k8s_namespace:in(default, kube-system) and k8s_pod:in(kube-proxy-*, kube-apiserver-*, kube-scheduler-*)`,
		`((level:error and (k8s_namespace:default or k8s_namespace:kube-system)) and ((k8s_pod:kube-proxy-* or k8s_pod:kube-apiserver-*) or k8s_pod:kube-scheduler-*))`)

	// Test filter 'ip_range'.
	test(`keyword: ip_range(192.168.1.2, 192.168.1.3)`, `keyword:ip_range(192.168.1.2, 192.168.1.3)`)
	test(`keyword: ip_range(192.168.1.2/0)`, `keyword:ip_range(0.0.0.0, 255.255.255.255)`)
	test(`keyword: ip_range(192.168.1.2/8)`, `keyword:ip_range(192.0.0.0, 192.255.255.255)`)
	test(`keyword: ip_range(192.168.1.2/31)`, `keyword:ip_range(192.168.1.2, 192.168.1.3)`)
	test(`keyword:ip_range(10.0.0.1, 10.0.0.255) AND service:api`, `(keyword:ip_range(10.0.0.1, 10.0.0.255) and service:api)`)
	test(`service:api OR keyword:ip_range(192.168.0.1/16)`, `(service:api or keyword:ip_range(192.168.0.0, 192.168.255.255))`)
	test(`NOT keyword:ip_range(127.0.0.1/8)`, `(not keyword:ip_range(127.0.0.0, 127.255.255.255))`)
	test(`(keyword:ip_range(10.0.0.1, 10.0.0.255) OR keyword:ip_range(192.168.1.1, 192.168.1.10)) AND service:db`, `((keyword:ip_range(10.0.0.1, 10.0.0.255) or keyword:ip_range(192.168.1.1, 192.168.1.10)) and service:db)`)
	test(`keyword:ip_range(10.0.0.1, 10.0.0.255) OR (service:api AND level:3)`, `(keyword:ip_range(10.0.0.1, 10.0.0.255) or (service:api and level:3))`)
}

func TestSeqQLCaseSensitive(t *testing.T) {
	// This test cannot run in parallel because it uses conf.CaseSensitive.

	test := func(in, out string) {
		t.Helper()
		seqql, err := ParseSeqQL(in, nil)
		require.NoError(t, err)
		require.Equal(t, out, seqql.SeqQLString())
	}
	// Test case sensitivity.
	config.CaseSensitive = true
	test("service: AbCdEf", "service:AbCdEf")
	test("text: AbCdEf", "text:AbCdEf")
	test("_exists_: 'AbCdEf'", "_exists_:AbCdEf")
	config.CaseSensitive = false
	test("service: AbCdEf", "service:abcdef")
	test("text: AbCdEf", "text:abcdef")
	test("_exists_: `AbCdEf`", "_exists_:AbCdEf")
}

func TestParseSeqQLNotIndexed(t *testing.T) {
	t.Parallel()

	q, err := ParseSeqQL("not_indexed_field:value", seq.Mapping{})
	require.NotNilf(t, err, "%+v", q)
	require.Equal(t, `field "not_indexed_field" is not indexed`, err.Error())
}

func TestParseSeqQLError(t *testing.T) {
	t.Parallel()

	test := func(got, expected string) {
		t.Helper()
		q, err := ParseSeqQL(got, nil)
		require.NotNilf(t, err, "%+v", q)
		require.Equalf(t, expected, err.Error(), "%s != %s", expected, err.Error())
	}

	// Test missing quoted.
	test(`service:"some`, `parsing filter value for field "service": unexpected symbol "\""`)
	test(`service:some "`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:some"`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:some"service:clickhouse`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:"some"*"thing`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:"some"*thing"`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:"some"*"thing`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:"some" *"thing"`, `expected 'and', 'or', 'not', got: "\ue000"`)
	test(`service: some thing`, `expected 'and', 'or', 'not', got: "thing"`)
	test(`service:"some thing`, `parsing filter value for field "service": unexpected symbol "\""`)
	test(`service:    some"thing`, `expected 'and', 'or', 'not', got: "\""`)

	test(`service:"some text AND level:"3"`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:some text" AND level:"3"`, `expected 'and', 'or', 'not', got: "text"`)
	test(`AND`, `missing ':' after "AND"`)
	test(`NOT`, `unexpected end of query`)
	test(`NOT NOT`, `unexpected end of query`)
	test(`m:a NOT`, `expected 'and', 'or', 'not', got: "NOT"`)

	// Test composite token.
	test(`service: AND level: 3`, `expected 'and', 'or', 'not', got: "level"`)
	test(`service: some AND level:`, `missing filter value for field "level"`)
	test(`m:a AND OR m : b`, `missing ':' after "OR"`)
	test(`m:a NOT AND m:b`, `expected 'and', 'or', 'not', got: "NOT"`)
	test(`service: some thing"`, `expected 'and', 'or', 'not', got: "thing"`)
	test(`[1 TO 3]:some`, `parsing field name: unexpected symbol "["`)
	test(`service:a\*b`, `expected 'and', 'or', 'not', got: "\\"`)
	test(`message:a\*b`, `expected 'and', 'or', 'not', got: "\\"`)
	test(`*:*`, `expected 'and', 'or', 'not', got: ":"`)
	test(`service:"workflow-api"and message:"Found"`, `expected 'and', 'or', 'not', got: "message"`)

	// Test range filter.
	test(`level:[1 3]`, `parsing range for field "level": expected ',' keyword, got "3"`)
	test(`level:[1TO3]`, `parsing range for field "level": expected ',' keyword, got "]"`)
	test(`level:[1 TO 3`, `parsing range for field "level": range end not found`)
	test(`level:1 TO 3]`, `expected 'and', 'or', 'not', got: "TO"`)
	test(`level:[]`, `parsing range for field "level": unexpected symbol "]"`)
	test(`level:[1 TO [3]]`, `parsing range for field "level": unexpected symbol "["`)
	test(`level:[1 TO 3]]`, `expected 'and', 'or', 'not', got: "]"`)
	test(`level:[[1 TO 3]]`, `parsing range for field "level": unexpected symbol "["`)
	test(`level:[[1 TO 3]`, `parsing range for field "level": unexpected symbol "["`)
	test(`level:[1 TP 3]`, `parsing range for field "level": expected ',' keyword, got "TP"`)
	test(`level:[1 TO 3[`, `parsing range for field "level": range end not found`)
	test(`level:]1 TO 3]`, `parsing filter value for field "level": unexpected symbol "]"`)
	test(`level:[`, `parsing range for field "level": unexpected end of query`)
	test(`level:[1`, `parsing range for field "level": expected ',' keyword, got ""`)
	test(`level:[*`, `parsing range for field "level": expected ',' keyword, got ""`)
	test(`level:["1`, `parsing range for field "level": unexpected symbol "\""`)
	test(`level:[ 1 to`, `parsing range for field "level": unexpected end of query`)
	test(`level:[1 to`, `parsing range for field "level": unexpected end of query`)
	test(`level:[1 to *`, `parsing range for field "level": range end not found`)
	test(`level:[1 to 2`, `parsing range for field "level": range end not found`)
	test(`level:[1 to 2*`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:[1 to "2`, `parsing range for field "level": unexpected symbol "\""`)
	test(`level:[1 to "2"`, `parsing range for field "level": range end not found`)
	test(`level:[1]`, `parsing range for field "level": expected ',' keyword, got "]"`)
	test(`level:[*]`, `parsing range for field "level": expected ',' keyword, got "]"`)
	test(`level:[1 to "2]`, `parsing range for field "level": unexpected symbol "\""`)

	// Test wildcards in range filter.
	test(`level:[** TO 1]`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:[1 TO a*]`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:[1 TO a*b]`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:[1 TO *b]`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:["**" TO 1]`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:[1 TO "a*"]`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:[1, "a*b"]`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:[1, "*b"]`, `parsing range for field "level": only single wildcard is allowed`)

	// Test empty key or value.
	test(`:[1 TO 3]`, `parsing field name: unexpected symbol ":"`)
	test(`:some`, `parsing field name: unexpected symbol ":"`)
	test(`:"abc"`, `parsing field name: unexpected symbol ":"`)
	test(`service:`, `missing filter value for field "service"`)
	test(`"":value`, `empty field name`)
	test(`service:`, `missing filter value for field "service"`)

	// Test unexpected tokens.
	test(`(m:a`, `missing ')'`)
	test(`m:a)`, `expected 'and', 'or', 'not', got: ")"`)
	test(`m:a AND (`, `unexpected end of query`)
	test(`m:a (`, `expected 'and', 'or', 'not', got: "("`)
	test(`m:a )`, `expected 'and', 'or', 'not', got: ")"`)
	test(`m:a( AND m:a`, `expected 'and', 'or', 'not', got: "("`)
	test(`m:a (AND m:a)`, `expected 'and', 'or', 'not', got: "("`)
	test(`m:a) AND m:a`, `expected 'and', 'or', 'not', got: ")"`)
	test(`some field:abc`, `missing ':' after "some"`)
	test(`level service:abc`, `missing ':' after "level"`)
	test(`(level:3 AND level level:abc)`, `missing ':' after "level"`)
	test(`NOT (:"abc")`, `parsing field name: unexpected symbol ":"`)

	// Test filter 'in'.
	test(`service:in`, `parsing 'in' filter: expected '(', got ""`)
	test(`service:in()`, `parsing 'in' filter: empty 'in' filter`)
	test(`service:in(1,)`, `parsing 'in' filter: parsing filter value for field "service": unexpected symbol ")"`)
	test(`service:in)`, `parsing 'in' filter: expected '(', got ")"`)
	test(`service:in(1`, `parsing 'in' filter: expected ')', got ""`)
	test(`service:in(1,3^2)`, `parsing 'in' filter: expected ')', got "^"`)
	test(`in(1):in(2)`, `missing ':' after "in"`)
	test(`service:in(2, in(4, 8))`, `parsing 'in' filter: expected ')', got "("`)
	test(`service:'in'(2, in(4, 8))`, `expected 'and', 'or', 'not', got: "("`)

	// Test filter 'ip_range'.
	test(`keyword:ip_range(300.0.0.1, 10.0.0.255)`, `parsing 'ip_range' filter: ParseAddr("300.0.0.1"): IPv4 field has value >255`)
	test(`keyword:ip_range(10.0.0.1, 10.0.0.256)`, `parsing 'ip_range' filter: ParseAddr("10.0.0.256"): IPv4 field has value >255`)
	test(`keyword:ip_range(10.0.0.1, "invalid")`, `parsing 'ip_range' filter: ParseAddr("invalid"): unable to parse IP`)
	test(`keyword:ip_range(10.0.0.1/33)`, `parsing 'ip_range' filter: netip.ParsePrefix("10.0.0.1/33"): prefix length out of range`)
	test(`keyword:ip_range(10.0.0.1, 10.0.0.0)`, `parsing 'ip_range' filter: first ip "10.0.0.1" is greater or equal than second ip "10.0.0.0"`)
	test(`keyword:ip_range(192.168.1.2, 192.168.1.3`, `parsing 'ip_range' filter: expected ')', got ""`)
	test(`keyword:ip_range(192.168.1.2/24`, `parsing 'ip_range' filter: expected ')', got ""`)
	test(`keyword:ip_range(192.168.1.2 192.168.1.3)`, `parsing 'ip_range' filter: expected ',' keyword, got "192.168.1.3"`)
	test(`keyword:ip_range(192.168.1.2,)`, `parsing 'ip_range' filter: unexpected symbol ")"`)
	test(`keyword:ip_range(,192.168.1.3)`, `parsing 'ip_range' filter: unexpected symbol ","`)
	test(`keyword:ip_range(192.168.1.2,,192.168.1.3)`, `parsing 'ip_range' filter: unexpected symbol ","`)

	// Test pipes.
	test(`message:--||`, `unknown pipe: |`)
	test(`source_type:access* | fields message | fields except login:admin`, `parsing 'fields' pipe: unexpected symbol ":"`)
	test(`source_type:access* | fields message | fields login`, `multiple field filters is not allowed`)
	test(`* | fields event, `, `parsing 'fields' pipe: trailing comma not allowed`)
}

func TestSeqQLParserFuzz(t *testing.T) {
	t.Parallel()
	// test, that any permutation of these characters will be invalid
	// template must be <= 11 symbols, or test will be very long
	templates := []string{
		`m:a[]`,
		`m::a`,
		`m:::a`,
		`m:a("`,
		`m:()`,
		`m:"`,
		`:()""`,
		`m:a OR ()"`,
		`AND OR NOT`,
	}
	for _, template := range templates {
		if len(template) >= 12 {
			panic("template is too long")
		}
		for p := make([]int, len(template)); p[0] < len(p); nextPerm(p) {
			s := getPerm(p, template)

			_, err := ParseSeqQL(s, nil)
			require.Errorf(t, err, "query: %s", s)
		}
	}
}

func TestSeqQLParsingASTStress(t *testing.T) {
	t.Parallel()
	iterations := 50
	for i := 0; i < iterations; i++ {
		exp := &ASTNode{}
		for i := 0; i < 100; i++ {
			addOperator(exp, 2*i)
			checkSelf(t, exp)
		}
	}
}

func BenchmarkSeqQLParsing(b *testing.B) {
	var query SeqQLQuery
	var err error
	str := `service: "some service" AND level:1`
	for i := 0; i < b.N; i++ {
		query, err = ParseSeqQL(str, seq.TestMapping)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
	exp = query.Root
}

func BenchmarkSeqQLParsingLong(b *testing.B) {
	var query SeqQLQuery
	var err error
	str := `((NOT ((((m:19 OR m:20) OR m:18) AND m:16) OR ((NOT (m:25 OR m:26)) AND m:12))) OR (((NOT m:29) AND m:22) OR (((m:31 OR m:32) AND m:14) OR (m:27 AND m:28))))`
	for i := 0; i < b.N; i++ {
		query, err = ParseSeqQL(str, seq.TestMapping)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
	exp = query.Root
}
