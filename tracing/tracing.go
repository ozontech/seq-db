package tracing

import (
	"fmt"
	"net"
	"os"

	"contrib.go.opencensus.io/exporter/jaeger"
	"go.opencensus.io/trace"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/buildinfo"
	"github.com/ozontech/seq-db/logger"
)

var (
	defaultProbability = 0.01
)

func Start(probability float64) error {
	defaultProbability = probability

	appName := "seqdb"
	if v := os.Getenv("TRACING_SERVICE_NAME"); v != "" {
		appName = v
	}
	host := "127.0.0.1"
	if v := os.Getenv("TRACING_AGENT_HOST"); v != "" {
		host = v
	}
	port := "6831"
	if v := os.Getenv("TRACING_AGENT_PORT"); v != "" {
		port = v
	}

	ip, err := getIP()
	if err != nil {
		return fmt.Errorf("getting ip: %s", err)
	}

	hostname, err := getHostname()
	if err != nil {
		return fmt.Errorf("getting hostname: %s", err)
	}

	exp, err := jaeger.NewExporter(jaeger.Options{
		AgentEndpoint: net.JoinHostPort(host, port),
		OnError: func(err error) {
			logger.Error("error sending trace", zap.Error(err))
		},
		Process: jaeger.Process{
			ServiceName: appName,
			Tags: []jaeger.Tag{
				jaeger.StringTag("host.name", hostname),
				jaeger.StringTag("version", buildinfo.Version),
				jaeger.StringTag("build.time", buildinfo.BuildTime),
				jaeger.StringTag("ip", ip),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("creating jaeger exporter: %s", err)
	}

	trace.RegisterExporter(exp)
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.ProbabilitySampler(probability)})
	logger.Info("tracing initialized", zap.Float64("probability", probability))
	return nil
}
