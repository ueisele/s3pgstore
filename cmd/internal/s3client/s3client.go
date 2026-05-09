// Package s3client builds the *http.Client and *s3.Client used
// by the s3pgstore-* binaries. Centralized here so all binaries
// pick up the same connection-pool tuning and the same
// StorageGRID-friendly checksum policy without diverging.
package s3client

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Connection-pool defaults. Match the library's default
// MaxInflightS3Requests (32) so a binary using the helper
// without overrides matches the library's saturation cap and
// the HTTP transport doesn't churn TCP connections under load.
const (
	defaultMaxInflightRequests = 32

	dialTimeout           = 10 * time.Second
	dialKeepAlive         = 30 * time.Second
	idleConnTimeout       = 90 * time.Second
	tlsHandshakeTimeout   = 10 * time.Second
	expectContinueTimeout = 1 * time.Second
)

// BuildHTTPClient returns an *http.Client whose Transport caps
// MaxConnsPerHost / MaxIdleConnsPerHost / MaxIdleConns at
// maxInflight — sized in lockstep with the library's
// MaxInflightS3Requests so a saturated semaphore drains without
// TCP-level connection churn (default MaxIdleConnsPerHost of 10
// causes TCP handshakes on every release above that threshold).
//
// HTTP/2 is intentionally disabled: the AWS S3 SDK and most S3
// implementations (MinIO, StorageGRID) perform measurably
// better over HTTP/1.1 at high concurrency because of HTTP/2
// stream-throttling and connection-multiplexing quirks under
// AWS retry policies.
//
// maxInflight <= 0 → defaultMaxInflightRequests (32).
func BuildHTTPClient(maxInflight int) *http.Client {
	if maxInflight <= 0 {
		maxInflight = defaultMaxInflightRequests
	}
	return &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   dialTimeout,
				KeepAlive: dialKeepAlive,
			}).DialContext,
			MaxIdleConns:          maxInflight,
			MaxIdleConnsPerHost:   maxInflight,
			MaxConnsPerHost:       maxInflight,
			IdleConnTimeout:       idleConnTimeout,
			TLSHandshakeTimeout:   tlsHandshakeTimeout,
			ExpectContinueTimeout: expectContinueTimeout,
			ForceAttemptHTTP2:     false,
		},
	}
}

// Options captures every knob BuildS3Client exposes.
type Options struct {
	// Region is the AWS region passed to LoadDefaultConfig.
	// Empty → "us-east-1" (matches AWS SDK convention for
	// non-region-aware S3-compatible backends like MinIO).
	Region string

	// Endpoint, when non-empty, overrides the AWS endpoint —
	// required for MinIO, StorageGRID, and other S3-compatible
	// backends. Triggers UsePathStyle so bucket names are not
	// promoted into the hostname.
	Endpoint string

	// MaxInflightRequests sizes the *http.Client's connection
	// pool. Pass the same value used for the library's
	// Config.MaxInflightS3Requests. Zero or negative →
	// defaultMaxInflightRequests.
	MaxInflightRequests int
}

// BuildS3Client constructs an AWS SDK v2 *s3.Client wired to a
// pool-sized *http.Client and configured for StorageGRID
// compatibility (ResponseChecksumValidation = WhenRequired —
// the SDK default WhenSupported makes StorageGRID log a warning
// on every GET because it doesn't return a body checksum on
// ordinary objects).
//
// Credentials follow the standard AWS SDK chain (env vars,
// shared credentials file, IRSA, IMDS).
func BuildS3Client(
	ctx context.Context, opts Options,
) (*awss3.Client, error) {
	region := opts.Region
	if region == "" {
		region = "us-east-1"
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithHTTPClient(
			BuildHTTPClient(opts.MaxInflightRequests)))
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	return awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		// StorageGRID compatibility: the SDK's default
		// WhenSupported asks for a checksum on every GET, which
		// StorageGRID logs a warning for. WhenRequired only
		// validates when the operation requires it (PUT path).
		o.ResponseChecksumValidation =
			aws.ResponseChecksumValidationWhenRequired
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
			o.UsePathStyle = true
		}
	}), nil
}
