package blockedbloom

import "math"

// Estimate the FPR at the mean block load.
// RocksDB averages the rates of an underloaded and
// an overloaded block at one standard deviation from the mean:
// https://github.com/meta/rocksdb/blob/41faa8846821c7451c9a78b62a61d9b296e79a02/util/bloom_impl.h#L42-L58
func estimateBloomParameters(fpr float64) (load float64, probes int) {
	for k := 1; k <= maxBloomProbes; k++ {
		if blockFalsePositiveRate(load, k) >= fpr {
			continue
		}

		lowLoad, highLoad := load, max(1, 2*load)
		for blockFalsePositiveRate(highLoad, k) < fpr {
			lowLoad = highLoad
			highLoad *= 2
		}

		// Binary search works because, for a fixed number of probes, the FPR
		// increases monotonically with block load. More keys per block increase
		// the expected fraction of set bits, making false positives more likely.
		for range 42 {
			mid := (lowLoad + highLoad) / 2
			if blockFalsePositiveRate(mid, k) <= fpr {
				lowLoad = mid
			} else {
				highLoad = mid
			}
		}

		load, probes = lowLoad, k
	}

	return load, probes
}

// A block with B bits, n keys, and k probes per key receives k*n independent,
// uniformly distributed probes.
//
//   - A single probe hits a given bit with probability 1/B and misses it
//     with probability 1 - 1/B.
//   - The bit stays unset if all k*n probes miss it. This happens with
//     probability (1 - 1/B)^(k*n).
//
// Let X be the number of set bits. By linearity of expectation,
// E[X] = B * (1 - (1 - 1/B)^(k*n)).
//
// An absent key produces a false positive if all k of its probes hit set bits.
// We estimate this probability as (E[X]/B)^k.
func blockFalsePositiveRate(load float64, probes int) float64 {
	const missedBit = 1 - 1.0/bloomBlockBits
	setBit := 1 - math.Pow(missedBit, load*float64(probes))
	return math.Pow(setBit, float64(probes))
}
