package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"go.uber.org/zap"
)

type parserTest struct {
	_Name         string
	debug         bool
	query         string
	sourceOps     []string
	sourceOpsKind []Kind
	ops           []Kind
	err           bool
}

func TestParser(t *testing.T) {
	tests := []*parserTest{
		{
			_Name:     "simple 1",
			query:     "service:a AND service:b",
			sourceOps: []string{"service:a", "service:b"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindAnd},
		},
		{
			_Name:     "simple 2",
			query:     "service:a AND service:b OR service:c",
			sourceOps: []string{"service:a", "service:b", "service:c"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindAnd, KindOr, KindAnd, KindOr},
		},
		{
			_Name:     "simple 3",
			query:     "service:a",
			sourceOps: []string{"service:a"},
			ops:       []Kind{KindOr, KindAnd},
		},
		{
			_Name:     "simple 4",
			query:     "message:55555-r2",
			sourceOps: []string{"message:55555", "message:r2"},
			ops:       []Kind{KindOr, KindOr, KindAnd},
		},
		{
			_Name:     "simple 5",
			query:     "operation_name:55555-r2 ",
			sourceOps: []string{"operation_name:55555-r2"},
			ops:       []Kind{KindOr, KindAnd},
		},
		{
			_Name:     "quote 1",
			query:     "service:my\\-api",
			sourceOps: []string{"service:my-api"},
			ops:       []Kind{KindOr, KindAnd},
		},
		{
			_Name:     "wc 1",
			query:     "service   :    a    AND      service:b     ",
			sourceOps: []string{"service:a", "service:b"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindAnd},
		},
		{
			_Name:     "brackets 1",
			query:     "service:a AND (service:b OR service:c)",
			sourceOps: []string{"service:a", "service:b", "service:c"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd},
		},
		{
			_Name:     "brackets 2",
			query:     "operation_name:a-b-c AND (operation_name:b-c-d OR operation_name:c-d-e)",
			sourceOps: []string{"operation_name:a-b-c", "operation_name:b-c-d", "operation_name:c-d-e"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd},
		},
		{
			_Name:     "brackets 3",
			query:     "service:a AND (service:b OR service:c) AND service:d",
			sourceOps: []string{"service:a", "service:b", "service:c", "service:d"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindAnd},
		},
		{
			_Name:     "brackets 4",
			query:     "service:a AND (service:b OR (service:c AND service:d))",
			sourceOps: []string{"service:a", "service:b", "service:c", "service:d"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindAnd, KindOr, KindAnd},
		},
		{
			_Name:     "brackets 5",
			query:     "service:a AND (service:b OR service:c) AND (service:d OR service:e)",
			sourceOps: []string{"service:a", "service:b", "service:c", "service:d", "service:e"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd},
		},
		{
			_Name:     "quoted 1",
			query:     `message:   "service_a\"\\ "   AND      message:   "   service_b"`,
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindAnd},
			sourceOps: []string{"message:service_a", "message:service_b"},
		},
		{
			_Name:     "quoted AND 1",
			query:     `message:"a b c"`,
			ops:       []Kind{KindOr, KindOr, KindOr, KindAnd},
			sourceOps: []string{"message:a", "message:b", "message:c"},
		},
		{
			_Name:     "quoted AND 2",
			query:     `operation_name:"a-b-c b-c__d c__d__e__f"`,
			ops:       []Kind{KindOr, KindOr, KindOr, KindAnd},
			sourceOps: []string{"operation_name:a-b-c", "operation_name:b-c__d", "operation_name:c__d__e__f"},
		},
		{
			_Name:     "quoted AND 3",
			query:     `message:X OR (message:"a b c")`,
			ops:       []Kind{KindOr, KindAnd, KindOr, KindOr, KindOr, KindAnd, KindOr},
			sourceOps: []string{"message:x", "message:a", "message:b", "message:c"},
		},
		{
			_Name: "error 1",
			query: `service:X O  service:Y`,
			err:   true,
		},
		{
			_Name: "error 2",
			query: `service:X A   service:Y`,
			err:   true,
		},
		{
			_Name: "error 3",
			query: `service:X ANDservice:Y`,
			err:   true,
		},
		{
			_Name: "error 4",
			query: `service:X A`,
			err:   true,
		},
		{
			_Name: "error 5",
			query: `service:X AND `,
			err:   true,
		},
		{
			_Name: "error 6",
			query: `)`,
			err:   true,
		},
		{
			_Name: "error 7",
			query: `asd)`,
			err:   true,
		},
		{
			_Name: "error 8",
			query: `AND )`,
			err:   true,
		},
		{
			_Name: "error when searching not indexed field",
			query: `non-existent_field:"test"`,
			err:   true,
		},
		{
			_Name:     "should ok when searching _exists_",
			query:     `_exists_:"test"`,
			ops:       []Kind{KindOr, KindAnd},
			sourceOps: []string{"_exists_:test"},
		},
		{
			_Name:     "empty",
			query:     ``,
			sourceOps: []string{"_all_:"},
		},
		{
			_Name:     "not 1",
			query:     `NOT service:A OR NOT service:B`,
			sourceOps: []string{"service:a", "service:b"},
			ops:       []Kind{KindOr, KindAnd, KindNot, KindOr, KindAnd, KindNot, KindAnd}, // last op is converted to AND, since OR deals only with positive ids
		},
		{
			_Name:     "not 2",
			query:     `service:A OR (service:B AND NOT service:c)`,
			sourceOps: []string{"service:a", "service:b", "service:c"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindOr, KindAnd, KindNot, KindAnd, KindOr},
		},
		{
			_Name:     "not 3",
			query:     `NOT (NOT (service:a OR service:b))`,
			sourceOps: []string{"service:a", "service:b"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindOr, KindNot, KindNot},
		},
		{
			_Name:     "not 4",
			query:     `NOT (service:a OR service:b) AND (service:c)`,
			sourceOps: []string{"service:a", "service:b", "service:c"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindOr, KindNot, KindOr, KindAnd, KindAnd},
		},
		{
			_Name:     "not 5",
			query:     `service:a AND NOT span_id:"1 2 3" AND NOT span_id:"100"`,
			sourceOps: []string{"service:a", "span_id:1 2 3", "span_id:100"},
			ops:       []Kind{KindOr, KindAnd, KindOr, KindAnd, KindNot, KindAnd, KindOr, KindAnd, KindNot, KindAnd},
		},
		{
			_Name:     "composite 1",
			query:     `NOT span_id:"1 2 3"`,
			sourceOps: []string{"span_id:1 2 3"},
			ops:       []Kind{KindOr, KindAnd, KindNot},
		},
		{
			_Name:         "range 1",
			query:         `service:[a to b]`,
			sourceOps:     []string{"service:[a to b]"},
			sourceOpsKind: []Kind{KindSourceRange},
			ops:           []Kind{KindOr, KindAnd},
		},
		{
			_Name:         "range 2",
			query:         `service:{a to b}`,
			sourceOps:     []string{"service:{a to b}"},
			sourceOpsKind: []Kind{KindSourceRange},
			ops:           []Kind{KindOr, KindAnd},
		},
		{
			_Name:         "range 3",
			query:         `service:[a to b}`,
			sourceOps:     []string{"service:[a to b}"},
			sourceOpsKind: []Kind{KindSourceRange},
			ops:           []Kind{KindOr, KindAnd},
		},
		{
			_Name:         "range 4",
			query:         `service:[* to b]`,
			sourceOps:     []string{"service:[* to b]"},
			sourceOpsKind: []Kind{KindSourceRange},
			ops:           []Kind{KindOr, KindAnd},
		},
		{
			_Name:         "range 5",
			query:         `service:[* to *]`,
			sourceOps:     []string{"service:[* to *]"},
			sourceOpsKind: []Kind{KindSourceRange},
			ops:           []Kind{KindOr, KindAnd},
		},
		{
			_Name:         "range 6",
			query:         `service:[a to *] AND service:{* to b}`,
			sourceOps:     []string{"service:[a to *]", "service:{* to b}"},
			sourceOpsKind: []Kind{KindSourceRange, KindSourceRange},
			ops:           []Kind{KindOr, KindAnd, KindOr, KindAnd, KindAnd},
		},
		{
			_Name:         "range 7",
			query:         `service:a AND service:[3 to 5] OR service:{123 to 345}`,
			sourceOps:     []string{"service:a", "service:[3 to 5]", "service:{123 to 345}"},
			sourceOpsKind: []Kind{KindSourceMatch, KindSourceRange, KindSourceRange},
			ops:           []Kind{KindOr, KindAnd, KindOr, KindAnd, KindAnd, KindOr, KindAnd, KindOr},
		},
		{
			_Name:         "range 8",
			query:         `service:["some string" to "other string"]`,
			sourceOps:     []string{`service:["some string" to "other string"]`},
			sourceOpsKind: []Kind{KindSourceRange},
			ops:           []Kind{KindOr, KindAnd},
		},
	}

	for _, test := range tests {
		t.Run(test._Name, func(t *testing.T) {
			var plan *Plan
			var err error
			require.NotPanics(t, func() {
				iterator := NewParser(TestMapping, consts.DefaultMaxTokenSize)
				plan, _, err = iterator.Parse([]byte(test.query))
			})
			if err != nil && test.err {
				return
			}
			if err == nil && test.err {
				assert.FailNow(t, "", "where should be error")
				panic("_")
			}
			if err != nil {
				assert.FailNow(t, "", "error while parsing query: %s", err.Error())
				panic("_")
			}
			if plan == nil {
				assert.FailNow(t, "", "error while parsing query: %s")
				panic("_")
			}

			if test.debug {
				logger.Info("ops", zap.Int("count", len(plan.Ops)))
				for _, op := range plan.Ops {
					logger.Info("op",
						zap.String("kind", op.Kind.String()),
						zap.Int("pos_childs", op.PosChilds),
						zap.Int("neg_childs", op.NegChilds),
					)
				}
				logger.Info("source ops", zap.Int("count", len(plan.SourceOps)))
				for _, op := range plan.SourceOps {
					logger.Info("source op",
						zap.String("kind", op.Kind.String()),
						zap.Int("pos_childs", op.PosChilds),
						zap.Int("neg_childs", op.NegChilds),
						zap.String("val", op.Token.HR()),
					)
				}
			}

			if len(plan.Ops)-1 != len(test.ops) {
				assert.FailNow(t, "", "wrong ops count exp=%d, actual=%d", len(test.ops), len(plan.Ops)-1)
			}

			if len(plan.SourceOps) != len(test.sourceOps) {
				assert.FailNow(t, "", "wrong source ops count exp=%d, actual=%d", len(test.ops), len(plan.SourceOps))
			}

			for i, testOp := range test.ops {
				planOp := plan.Ops[i]

				assert.Equal(t, testOp.String(), planOp.Kind.String(), "wrong op index=%d", i)
			}

			for i, testOp := range test.sourceOps {
				planOp := plan.SourceOps[i]

				assert.Equal(t, testOp, planOp.Token.HR(), "wrong source op")
			}

			if len(test.sourceOps) == len(test.sourceOpsKind) {
				for i, testOp := range test.sourceOpsKind {
					planOp := plan.SourceOps[i]
					assert.Equal(t, testOp, planOp.Kind, "wrong source op kind")
				}
			}
		})
	}
}
