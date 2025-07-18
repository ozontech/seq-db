package tracing

import (
	"context"
	"net"
	"os"

	"go.opencensus.io/trace"

	"github.com/ozontech/seq-db/consts"
)

func getHostname() (string, error) {
	return os.Hostname()
}

func getIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, i := range ifaces {
		if i.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := i.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip.To4() != nil && !ip.IsLoopback() && ip.To4()[0] == 10 { // 10.*.*.*
				return ip.To4().String(), nil
			}
		}
	}
	return "unknown", nil // we don't want to fail if we can't find an IP
}

func StartSpan(ctx context.Context, name string, o ...trace.StartOption) (context.Context, *trace.Span) {
	rCtx, span := trace.StartSpan(ctx, name, o...)
	if v := ctx.Value(consts.JaegerDebugKey); v != nil {
		span.AddAttributes(trace.BoolAttribute(consts.JaegerDebugKey, true))
	}
	return rCtx, span
}
