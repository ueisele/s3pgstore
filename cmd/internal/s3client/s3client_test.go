package s3client

import (
	"net/http"
	"testing"
)

// TestBuildHTTPClient_PoolSizing locks in that the n knob feeds
// every connection-pool dimension. Any future refactor that
// drops one of these silently regresses TCP-churn behaviour
// under load — the test exists specifically to catch that.
func TestBuildHTTPClient_PoolSizing(t *testing.T) {
	c := BuildHTTPClient(64)
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport: want *http.Transport, got %T", c.Transport)
	}
	if got := tr.MaxIdleConns; got != 64 {
		t.Errorf("MaxIdleConns: want 64, got %d", got)
	}
	if got := tr.MaxIdleConnsPerHost; got != 64 {
		t.Errorf("MaxIdleConnsPerHost: want 64, got %d", got)
	}
	if got := tr.MaxConnsPerHost; got != 64 {
		t.Errorf("MaxConnsPerHost: want 64, got %d", got)
	}
	if tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2: want false (HTTP/1.1 preferred " +
			"for AWS SDK + S3 backends under load)")
	}
}

// TestBuildHTTPClient_DefaultsWhenZeroOrNegative verifies the
// "<=0 → default 32" sentinel. Any caller passing a misconfigured
// zero (e.g. unset env var defaulting to 0) gets a sane pool
// rather than a single-connection HTTP client.
func TestBuildHTTPClient_DefaultsWhenZeroOrNegative(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		c := BuildHTTPClient(n)
		tr := c.Transport.(*http.Transport)
		if got := tr.MaxConnsPerHost; got != defaultMaxInflightRequests {
			t.Errorf("BuildHTTPClient(%d).MaxConnsPerHost: "+
				"want %d, got %d",
				n, defaultMaxInflightRequests, got)
		}
	}
}
