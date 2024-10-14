package metric

// RollingAverage can compute average in a rolling window.
type RollingAverage struct {
	sum      int
	elements []int
	cursor   int
	filled   bool
}

func NewRollingAverage(sampleSize int) *RollingAverage {
	return &RollingAverage{
		elements: make([]int, sampleSize),
	}
}

// Append the value to the sample.
func (r *RollingAverage) Append(value int) {
	prev := r.swapAt(r.cursor, value)

	r.sum -= prev
	r.sum += value

	r.cursor++
	if r.cursor == len(r.elements) {
		r.filled = true
		r.cursor = 0
	}
}

// Get returns a rolling average.
func (r *RollingAverage) Get() float32 {
	if r.filled {
		return float32(r.sum) / float32(len(r.elements))
	}

	if r.cursor == 0 {
		return 0
	}
	// sample is not filled, e.g.
	// [50, 30, 20, 0, 0, 0, 0, 0]
	//              ^
	//              |
	//            cursor
	// we can return the average filled elements
	return float32(r.sum) / float32(r.cursor)
}

// Filled returns true if the sample is filled.
func (r *RollingAverage) Filled() bool {
	return r.filled
}

func (r *RollingAverage) swapAt(idx, newVal int) int {
	prev := r.elements[idx]
	r.elements[idx] = newVal
	return prev
}
