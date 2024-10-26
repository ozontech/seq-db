package parser

import (
	"testing"

	"github.com/ozontech/seq-db/seq"
)

var exp *ASTNode

func BenchmarkParsing(b *testing.B) {
	str := `service: "some service" AND level:1`
	for i := 0; i < b.N; i++ {
		exp, _ = ParseQuery(str, seq.TestMapping)
	}
}

func BenchmarkParsingLong(b *testing.B) {
	str := `((NOT ((((m:19 OR m:20) OR m:18) AND m:16) OR ((NOT (m:25 OR m:26)) AND m:12))) OR (((NOT m:29) AND m:22) OR (((m:31 OR m:32) AND m:14) OR (m:27 AND m:28))))`
	for i := 0; i < b.N; i++ {
		exp, _ = ParseQuery(str, seq.TestMapping)
	}
}
