package bench

func ConstSource[V any](value V) func() V {
	return func() V {
		return value
	}
}

func MixSources[V any](share float32, in1, in2 func() V) func() V {
	var balance float32
	return func() V {
		balance += share
		if balance > 0 {
			balance -= 1.0
			return in1()
		}
		return in2()
	}
}

func Flood[V any](cleaner *Cleaner, in func() V) <-chan V {
	out := make(chan V)
	go func() {
		defer close(out)
		for !cleaner.TeardownStart.Load() {
			out <- in()
		}
	}()
	return out
}

func FloodFixedAmount[V any](size int, in func() V) <-chan V {
	out := make(chan V)
	go func() {
		defer close(out)
		for i := 0; i < size; i++ {
			out <- in()
		}
	}()
	return out
}

func Empty[V any]() <-chan V {
	out := make(chan V)
	defer close(out)
	return out
}
