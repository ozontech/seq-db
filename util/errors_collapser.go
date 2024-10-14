package util

import (
	"strconv"

	"github.com/ozontech/seq-db/bytespool"
)

type unwrapper interface {
	// Unwrap returns slice of errors, it uses in the errors.Is and errors.As stdlib functions.
	Unwrap() []error
}

// check errsCollapser implements unwrapper interface.
var _ unwrapper = (*errsCollapser)(nil)

// errsCollapser implements Unwrap interface.
// Same as errors.Join, but deduplicates errors.
type errsCollapser struct {
	errs        []error
	withRepeats bool
}

// CollapseErrors combines errors and writes repeats of each.
func CollapseErrors(errs []error) error {
	return collapseErrors(errs, true)
}

// DeduplicateErrors deduplicates errors.
func DeduplicateErrors(errs []error) error {
	return collapseErrors(errs, false)
}

func collapseErrors(errs []error, withRepeats bool) error {
	// shift not nil
	i := 0
	for _, err := range errs {
		if err == nil {
			continue
		}
		errs[i] = err
		i++
	}
	errs = errs[:i]

	if len(errs) == 0 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}

	return &errsCollapser{errs: errs, withRepeats: withRepeats}
}

func (e *errsCollapser) Error() string {
	if len(e.errs) == 0 {
		panic("BUG: errsCollapser is empty")
	}

	const gotNTimesExample = " (got 42 times); "

	// group
	m := make(map[string]int, len(e.errs))
	errorLength := 0
	for _, err := range e.errs {
		str := err.Error()
		m[str]++
		repeats := m[str]

		if repeats == 1 {
			// count str
			errorLength += len(str)
		}
		if e.withRepeats && repeats == 2 {
			// count "got N times"
			errorLength += len(gotNTimesExample)
		}
	}

	buf := bytespool.AcquireReset(errorLength)
	defer bytespool.Release(buf)

	first := true
	for str, repeats := range m {
		if first {
			first = false
		} else {
			const errorsSeparator = "; "
			buf.B = append(buf.B, errorsSeparator...)
		}

		buf.B = append(buf.B, str...)

		if e.withRepeats && repeats > 1 {
			buf.B = append(buf.B, " (got "...)
			buf.B = strconv.AppendInt(buf.B, int64(repeats), 10)
			buf.B = append(buf.B, " times)"...)
		}
	}

	return string(buf.B)
}

func (e *errsCollapser) Unwrap() []error {
	return e.errs
}
