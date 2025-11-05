package util

import (
	"fmt"
	"runtime/debug"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ozontech/seq-db/logger"
)

type panicWrapper struct {
	e error
}

func (p *panicWrapper) Unwrap() error {
	return p.e
}

func (p *panicWrapper) Error() string {
	return p.e.Error()
}

func Recover(metric prometheus.Counter, err error) {
	metric.Inc()
	logger.Error(err.Error())
	debug.PrintStack()
}

func RecoverToError(panicData any, metric prometheus.Counter) error {
	err := fetchError(panicData)
	if err != nil {
		Recover(metric, err)
		err = &panicWrapper{e: err}
	}
	return err
}

func fetchError(panicData any) error {
	if panicData != nil {
		if err, ok := panicData.(error); ok {
			return err
		}
		return fmt.Errorf("%v", panicData)
	}
	return nil
}
