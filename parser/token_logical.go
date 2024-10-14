package parser

type logicalKind int

const (
	LogicalOr logicalKind = iota
	LogicalAnd
	LogicalNot
	LogicalNAnd // created in `process`
)

type Logical struct {
	Operator logicalKind
}
