package circuitbreaker

import (
	"context"
	"fmt"
	"time"

	"github.com/cep21/circuit/v3"
	"github.com/cep21/circuit/v3/closers/hystrix"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"go.uber.org/zap"
)

var (
	_ circuit.RunMetrics = promMetrics{}
	_ circuit.Metrics    = promMetrics{}

	manager = circuit.Manager{} // used to manage circuits with unique names
)

type CircuitBreaker struct {
	*circuit.Circuit
}

type Config struct {
	// Timeout for the execution context.
	Timeout time.Duration
	// MaxConcurrent the maximum number of requests allowed to be executed at one time.
	MaxConcurrent int64

	// NumBuckets the number of buckets the rolling window is divided into.
	NumBuckets int

	// BucketWidth used to compute the duration of the statistical rolling window.
	BucketWidth time.Duration

	// RequestVolumeThreshold number of requests in the rolling window that will trigger
	// a check on the total number of requests as a percentage (ErrorThresholdPercentage).
	// For example, if the value is 20, then if only 19 requests are received in the rolling window
	// the circuit will not trigger percentage check to open the circuit.
	// Set 1 to disable and always trigger percentage check.
	RequestVolumeThreshold int64

	// TotalThresholdPercent ratio in the rolling window of the number of failed requests to the total number of requests.
	// For example, if the value is 10, if total requests in the rolling window is 100 when failed requests is 10,
	// then 10/100 >= 10% -> the circuit will be open.
	ErrorThresholdPercentage int64

	// SleepWindow how long to deny requests before allowing attempts
	// again to determine if the chain should be closed again.
	SleepWindow time.Duration
}

const (
	// defaultHalfOpenAttempts how many requests are allowed to be sent in the config.SleepWindow.
	// 1 is default in is default value in the cep21/circuit.
	defaultHalfOpenAttempts = 1
	// defaultRequiredConcurrentSuccessful how many requests must be passed to open the breaker.
	// 1 is default in is default value in the cep21/circuit.
	defaultRequiredConcurrentSuccessful = 1
)

func New(name string, config Config) *CircuitBreaker {
	breaker := manager.GetCircuit(name)
	if breaker != nil {
		return &CircuitBreaker{
			Circuit: breaker,
		}
	}

	metricCollector := promMetrics{name: name}
	metricCollector.Closed(time.Now())

	if config.Timeout == 0 {
		config.Timeout = time.Minute
	}

	breaker = manager.MustCreateCircuit(name, circuit.Config{
		Execution: circuit.ExecutionConfig{
			Timeout:               config.Timeout,
			MaxConcurrentRequests: config.MaxConcurrent,
		},
		General: circuit.GeneralConfig{
			OpenToClosedFactory: hystrix.CloserFactory(hystrix.ConfigureCloser{
				SleepWindow:                  config.SleepWindow,
				HalfOpenAttempts:             defaultHalfOpenAttempts,
				RequiredConcurrentSuccessful: defaultRequiredConcurrentSuccessful,
			}),
			ClosedToOpenFactory: hystrix.OpenerFactory(hystrix.ConfigureOpener{
				RequestVolumeThreshold:   config.RequestVolumeThreshold,
				ErrorThresholdPercentage: config.ErrorThresholdPercentage,
				NumBuckets:               config.NumBuckets,
				RollingDuration:          time.Duration(config.NumBuckets) * config.BucketWidth,
			}),
		},
		Metrics: circuit.MetricsCollectors{
			Run: []circuit.RunMetrics{
				metricCollector,
			},
			Circuit: []circuit.Metrics{
				metricCollector,
			},
		},
	})

	return &CircuitBreaker{
		Circuit: breaker,
	}
}

func (cb *CircuitBreaker) Execute(ctx context.Context, callback func(context.Context) error) error {
	err := cb.Circuit.Execute(ctx, func(ctx context.Context) error {
		return callback(ctx)
	}, nil)

	if err != nil {
		return fmt.Errorf("circuit breaker execute: %w", err)
	}

	return err
}

type promMetrics struct {
	name string
}

func (p promMetrics) Closed(_ time.Time) {
	metric.CircuitBreakerState.WithLabelValues(p.name).Set(0)
	logger.Error("circuit breaker closed", zap.String("name", p.name))
}

func (p promMetrics) Opened(_ time.Time) {
	metric.CircuitBreakerState.WithLabelValues(p.name).Set(1)
	logger.Error("circuit breaker opened", zap.String("name", p.name))
}

func (p promMetrics) Success(_ time.Time, _ time.Duration) {
	metric.CircuitBreakerSuccess.WithLabelValues(p.name).Inc()
}

func (p promMetrics) ErrFailure(_ time.Time, _ time.Duration) {
	metric.CircuitBreakerErr.WithLabelValues(p.name, "failure").Inc()
}

func (p promMetrics) ErrTimeout(_ time.Time, _ time.Duration) {
	metric.CircuitBreakerErr.WithLabelValues(p.name, "timeout").Inc()
}

func (p promMetrics) ErrBadRequest(_ time.Time, _ time.Duration) {
	metric.CircuitBreakerErr.WithLabelValues(p.name, "bad_request").Inc()
}

func (p promMetrics) ErrInterrupt(_ time.Time, _ time.Duration) {
	metric.CircuitBreakerErr.WithLabelValues(p.name, "interrupt").Inc()
}

func (p promMetrics) ErrConcurrencyLimitReject(_ time.Time) {
	metric.CircuitBreakerErr.WithLabelValues(p.name, "concurrency_limit").Inc()
}

func (p promMetrics) ErrShortCircuit(_ time.Time) {
	metric.CircuitBreakerErr.WithLabelValues(p.name, "short_circuit").Inc()
}
