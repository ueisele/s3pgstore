package s3client

import (
	"context"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
)

// trackedDialer wraps net.Dialer so every successful dial
// increments the open-conn gauge and the returned net.Conn
// decrements on Close. Lets the s3pgstore.s3.tcp.connections
// gauge surface the real "current open TCP sockets to S3"
// number — the saturation signal S3MaxOpenConnections is
// intended to bound. Drift (open > Close ever) means a Conn
// leak; that's a feature, not a bug.
type trackedDialer struct {
	dialer *net.Dialer
	m      *s3metrics
}

// DialContext implements the http.Transport DialContext shape.
func (t trackedDialer) DialContext(
	ctx context.Context, network, addr string,
) (net.Conn, error) {
	conn, err := t.dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	t.m.recordTCPConnOpen(ctx)
	return &trackedConn{Conn: conn, m: t.m}, nil
}

// trackedConn wraps net.Conn so Close fires
// recordTCPConnClose exactly once. net.Conn implementations
// are required to tolerate double-Close (Go stdlib
// convention); without the guard the gauge would
// over-decrement on a double-Close caller.
type trackedConn struct {
	net.Conn
	m      *s3metrics
	closed atomic.Bool
}

// Close decrements the open-conn gauge once, then closes the
// underlying net.Conn. context.Background is used because the
// Conn outlives any single request context — the metric Add
// only needs a context for OTel resource attribution, which is
// MeterProvider-scoped, not request-scoped.
func (c *trackedConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.m.recordTCPConnClose(context.Background())
	}
	return c.Conn.Close()
}

// tracingTransport wraps an http.RoundTripper to inject an
// httptrace.ClientTrace into every outgoing request. The
// trace's GotConn hook fires once per request before the body
// is written; info.Reused is true when the connection came
// from the idle pool and false when the request had to dial
// fresh. Drives the s3pgstore.s3.connection.reuse.count
// counter — operators read the rate ratio
// (reused / (reused + fresh)) as the idle-pool hit rate.
type tracingTransport struct {
	base http.RoundTripper
	m    *s3metrics
}

// RoundTrip implements http.RoundTripper.
func (t tracingTransport) RoundTrip(
	r *http.Request,
) (*http.Response, error) {
	ctx := r.Context()
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			t.m.recordConnectionReuse(ctx, info.Reused)
		},
	}
	r = r.WithContext(httptrace.WithClientTrace(ctx, trace))
	return t.base.RoundTrip(r)
}
