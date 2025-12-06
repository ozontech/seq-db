package active2

import (
	"math"
)

// sizeTiers splits the entire space of integers into successive ranges [A(n) ; B(n)] where:
//   - A(n+1) = B(n) + 1
//   - (B(n) - A(n)) / A(n) ~ deltaPercent
//
// Example: for newSizeTiers(100, 200, 10) we will have:
//
// Tier   0: [     0;   100 ] delta: 0.00%
// Tier   1: [   101;   106 ] delta: 6.00%
// Tier   2: [   107;   117 ] delta: 10.00%
// Tier   3: [   118;   129 ] delta: 10.00%
// Tier   4: [   130;   142 ] delta: 10.00%
// Tier   5: [   143;   156 ] delta: 10.00%
// Tier   6: [   157;   171 ] delta: 10.00%
// Tier   7: [   172;   189 ] delta: 10.00%
// Tier   8: [   190;   207 ] delta: 9.00%
// Tier   9: [   208;   228 ] delta: 10.00%
// Tier  10: [   229;   251 ] delta: 10.00%
// Tier  11: [   252;   276 ] delta: 10.00%
// Tier  12: [   277;   304 ] delta: 10.00%
// Tier  13: [   305;   334 ] delta: 10.00%
// Tier  14: [   335;   368 ] delta: 10.00%
// Tier  15: [   369;   405 ] delta: 10.00%
// Tier  16: [   406;   445 ] delta: 10.00%
// Tier  17: [   446;   490 ] delta: 10.00%
// Tier  18: [   491;   539 ] delta: 10.00%
// Tier  19: [   540;   593 ] delta: 10.00%
// Tier  20: [   594;   652 ] delta: 10.00%
// Tier  21: [   653;   717 ] delta: 10.00%
// Tier  22: [   718;   789 ] delta: 10.00%
// Tier  23: [   790;   868 ] delta: 10.00%
// Tier  24: [   869;   955 ] delta: 10.00%
// Tier  25: [   956;  1051 ] delta: 10.00%
// Tier  26: [  1052;  1156 ] delta: 10.00%
//
//	etc.
//
// So, sizeTiers returns us the tier (the number of range) for any integer value.
type sizeTiers struct {
	firstMax uint32
	maxTier  int
	deltaK   float64
	offset   float64
}

// newSizeTiers creates a calculator that determines the ordinal number of the range (size tier) a given integer value falls into.
//
// Parameters:
//
//	firstMax      - The upper bound of the initial (first) range. Range #0 is always [0, firstMax].
//	maxTier       - The maximum number of ranges (tiers) to create. For the last range (#maxTier),
//	                the upper bound is considered to be positive infinity (+inf).
//	deltaPercent  - The defined growth percentage. The ratio of the difference between the upper bounds
//	                of two adjacent ranges to the lower bound of the next range is approximately equal
//	                to this value. Formula for tier #n: (B_n - B_{n-1}) / B_{n-1} ≈ deltaPercent / 100
func newSizeTiers(firstMax uint32, maxTier, deltaPercent int) sizeTiers {
	deltaK := 1 / math.Log(1+float64(deltaPercent)/100)
	return sizeTiers{
		maxTier:  maxTier,
		firstMax: firstMax,
		deltaK:   deltaK,
		offset:   math.Floor(deltaK*(math.Log(float64(firstMax)))) - 1,
	}
}

func (t sizeTiers) Calc(size uint32) int {
	if size <= t.firstMax {
		return 0
	}
	tier := int(math.Floor(t.deltaK*(math.Log(float64(size)))) - t.offset)
	if tier > t.maxTier {
		return t.maxTier
	}
	return tier
}
