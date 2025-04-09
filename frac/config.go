package frac

type Config struct {
	Search SearchConfig
}

type SearchConfig struct {
	AggLimits AggLimits
}

type AggLimits struct {
	MaxFieldTokens     int // MaxFieldTokens max AggQuery.Field uniq values to parse.
	MaxGroupTokens     int // MaxGroupTokens max AggQuery.GroupBy unique values.
	MaxTIDsPerFraction int // MaxTIDsPerFraction max number of tokens per fraction.
}
