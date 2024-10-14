package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/query"
)

func checkErr(t *testing.T, q string) {
	t.Run("error", func(t *testing.T) {
		_, err := ParseQuery(q, query.TestMapping)
		assert.Error(t, err)
	})
}

// error messages are actually readable
func TestParserErr(t *testing.T) {
	// 00
	checkErr(t, ``)
	checkErr(t, `service:`)
	checkErr(t, `service:"some`)
	checkErr(t, `service:some"`)
	checkErr(t, `service: some thing`)
	// 05
	checkErr(t, `service:"some thing`)
	checkErr(t, `service: some thing"`)
	checkErr(t, `AND`)
	checkErr(t, `NOT`)
	checkErr(t, `service: AND level: 3`)
	// 10
	checkErr(t, `service: some AND level:`)
	checkErr(t, `nosuchfieldinlist: some`)
	checkErr(t, `service:"some text AND level:"3"`)
	checkErr(t, `service:some text" AND level:"3"`)
	checkErr(t, `m:a AND OR m:b`)
	// 15
	checkErr(t, `m:a NOT AND m:b`)
	checkErr(t, `m:a NOT`)
	checkErr(t, `NOT NOT`)
	checkErr(t, `level:[1 3]`)
	checkErr(t, `level:[1TO3]`)
	// 20
	checkErr(t, `level:[1 TO 3`)
	checkErr(t, `level:1 TO 3]`)
	checkErr(t, `level:[]`)
	checkErr(t, `level:[1 TO [3]]`)
	checkErr(t, `level:[1 TO 3]]`)
	// 25
	checkErr(t, `level:[[1 TO 3]]`)
	checkErr(t, `level:[[1 TO 3]`)
	checkErr(t, `level:[1 TP 3]`)
	checkErr(t, `level:[1 TO 3[`)
	checkErr(t, `level:]1 TO 3]`)
	// 30
	checkErr(t, `:some`)
	checkErr(t, `:[1 TO 3]`)
	checkErr(t, `[1 TO 3]:some`)
	checkErr(t, `(m:a`)
	checkErr(t, `m:a)`)
	// 35
	checkErr(t, `m:a AND (`)
	checkErr(t, `m:a (`)
	checkErr(t, `m:a )`)
	checkErr(t, `m:a( AND m:a`)
	checkErr(t, `m:a (AND m:a)`)
	// 40
	checkErr(t, `m:a) AND m:a`)
	checkErr(t, `service:**`)
	checkErr(t, `service:a**`)
	checkErr(t, `service:**b`)
	checkErr(t, `service:a**b`)
	// 45
	checkErr(t, `some field:abc`)
	checkErr(t, `level service:abc`)
	checkErr(t, `(level:3 AND level level:abc)`)
	checkErr(t, `:"abc"`)
	checkErr(t, `NOT (:"abc")`)
	// 50
	checkErr(t, `message:--||`)
	checkErr(t, `level:[** TO 1]`)
	checkErr(t, `level:[1 TO a*]`)
	checkErr(t, `level:[1 TO a*b]`)
	checkErr(t, `level:[1 TO *b]`)
	// 55
	checkErr(t, `level:["**" TO 1]`)
	checkErr(t, `level:[1 TO "a*"]`)
	checkErr(t, `level:[1 TO "a*b"]`)
	checkErr(t, `level:[1 TO "*b"]`)
	checkErr(t, `level:[`)
	// 60
	checkErr(t, `level:[ `)
	checkErr(t, `level:[1`)
	checkErr(t, `level:[ 1`)
	checkErr(t, `level:[*`)
	checkErr(t, `level:[ *`)
	// 65
	checkErr(t, `level:["1"`)
	checkErr(t, `level:["1`)
	checkErr(t, `level:[ 1 to`)
	checkErr(t, `level:[1 to`)
	checkErr(t, `level:[1 to *`)
	// 70
	checkErr(t, `level:[1 to 2`)
	checkErr(t, `level:[1 to 2*`)
	checkErr(t, `level:[1 to "2`)
	checkErr(t, `level:[1 to "2"`)
	checkErr(t, `level:[1]`)
	// 75
	checkErr(t, `level:[*]`)
	checkErr(t, `level:[1 to "2]`)
}

func nextPerm(p []int) {
	for i := len(p) - 1; i >= 0; i-- {
		if i == 0 || p[i] < len(p)-i-1 {
			p[i]++
			return
		}
		p[i] = 0
	}
}

func getPerm(p []int, s string) string {
	res := []byte(s)
	for i, v := range p {
		res[i], res[i+v] = res[i+v], res[i]
	}
	return string(res)
}

func TestParserFuzz(t *testing.T) {
	// test, that any permutation of these characters will be invalid
	// template must be <= 11 symbols, or test will be very long
	templates := []string{
		`m:a[]`,
		`m::a`,
		`m:::a`,
		`m:a("`,
		`m:()`,
		`m:"`,
		`m:()\`,
		`:()""\`,
		`m:a OR ()"`,
		`AND OR NOT`,
	}
	for _, template := range templates {
		t.Run("test", func(t *testing.T) {
			if len(template) >= 12 {
				panic("template is too long")
			}
			if len(template) >= 10 && testing.Short() {
				t.Skip("skipping long template test")
			}
			for p := make([]int, len(template)); p[0] < len(p); nextPerm(p) {
				s := getPerm(p, template)

				_, err := ParseQuery(s, nil)
				require.Errorf(t, err, "query: %s", s)
			}
		})
	}
}

func TestAggregationFilter(t *testing.T) {
	token, err := ParseAggregationFilter("")
	assert.NoError(t, err, "empty query should be okay")
	assert.Nil(t, token)

	token, err = ParseAggregationFilter("message: hello* AND k8s_pod: a*")
	assert.Error(t, err, "no complex queries")
	assert.Nil(t, token)

	_, err = ParseAggregationFilter("()")
	assert.Error(t, err, "literals only")

	_, err = ParseAggregationFilter("level:[1 TO 3]")
	assert.Error(t, err, "no range queries allowed")

	_, err = ParseAggregationFilter("level:[1 TO 3")
	assert.Error(t, err, "incorrect range query")

	_, err = ParseAggregationFilter("(")
	assert.Error(t, err, "incorrect query")

	_, err = ParseAggregationFilter("message:")
	assert.Error(t, err, "incorrect query")

	_, err = ParseAggregationFilter(":text")
	assert.Error(t, err, "incorrect query")

	_, err = ParseAggregationFilter("blabla")
	assert.Error(t, err, "incorrect query")

	_, err = ParseAggregationFilter("(message:hello*")
	assert.Error(t, err, "no invalid queries")

	token, err = ParseAggregationFilter("message: service_1*")
	exp := &Literal{
		Field: "message",
		Terms: []Term{
			{Kind: TermText, Data: "service_1"},
			{Kind: TermSymbol, Data: "*"},
		},
	}
	assert.Equal(t, exp, token)
	assert.NoError(t, err, "no errors on simple queries")

	token, err = ParseAggregationFilter("message12: *service_2")
	exp = &Literal{
		Field: "message12",
		Terms: []Term{
			{Kind: TermSymbol, Data: "*"},
			{Kind: TermText, Data: "service_2"},
		},
	}
	assert.Equal(t, exp, token)
	assert.NoError(t, err, "no errors on simple queries")

}
