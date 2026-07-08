package frac

type Config struct {
	Search SearchConfig

	SkipSortDocs bool
	KeepMetaFile bool
}

type SearchConfig struct {
	AggLimits         AggLimits
	QueryOptimization QueryOptimizationConfig
}

type AggLimits struct {
	MaxFieldTokens     int // MaxFieldTokens max AggQuery.Field uniq values to parse.
	MaxFieldValues     int // MaxFieldValues max AggQuery.Field uniq values to hold per aggregation request.
	MaxGroupTokens     int // MaxGroupTokens max AggQuery.GroupBy unique values.
	MaxTIDsPerFraction int // MaxTIDsPerFraction max number of tokens per fraction.
}

type QueryOptimizationConfig struct {
	// BatchIterationCostThreshold is the minimum estimated non-batched iteration
	// cost required to enable batch-at-a-time query evaluation.
	BatchIterationCostThreshold int
}
