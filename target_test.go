package s3pgstore

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Tests for the retry policy + transient classification + sem
// behavior live in s3client/ alongside the middleware
// implementation. What remains here is the small surface area
// s3target still owns post-middleware refactor: ETag verification
// on PUT and constructor validation.

func TestVerifyPutObjectETag_Match(t *testing.T) {
	out := &s3.PutObjectOutput{
		ETag: aws.String(`"abc123"`),
	}
	if err := verifyPutObjectETag(out, "abc123"); err != nil {
		t.Fatalf("matching ETag: %v", err)
	}
}

func TestVerifyPutObjectETag_Mismatch(t *testing.T) {
	out := &s3.PutObjectOutput{
		ETag: aws.String(`"abc123"`),
	}
	err := verifyPutObjectETag(out, "deadbeef")
	if err == nil {
		t.Fatal("mismatched ETag: want error, got nil")
	}
	if !strings.Contains(err.Error(), "integrity check failed") {
		t.Fatalf("error message: %q", err)
	}
}

func TestVerifyPutObjectETag_SkipsKMS(t *testing.T) {
	out := &s3.PutObjectOutput{
		ServerSideEncryption: s3types.ServerSideEncryptionAwsKms,
		ETag:                 aws.String(`"opaque"`),
	}
	if err := verifyPutObjectETag(out, "abc123"); err != nil {
		t.Fatalf("KMS skip: %v", err)
	}
}

func TestVerifyPutObjectETag_SkipsMultipart(t *testing.T) {
	out := &s3.PutObjectOutput{
		ETag: aws.String(`"abc-2"`),
	}
	if err := verifyPutObjectETag(out, "abc123"); err != nil {
		t.Fatalf("multipart skip: %v", err)
	}
}

func TestVerifyPutObjectETag_SkipsSSEC(t *testing.T) {
	out := &s3.PutObjectOutput{
		SSECustomerAlgorithm: aws.String("AES256"),
		ETag:                 aws.String(`"abc"`),
	}
	if err := verifyPutObjectETag(out, "deadbeef"); err != nil {
		t.Fatalf("SSE-C skip: %v", err)
	}
}

func TestNewS3Target_Validation(t *testing.T) {
	cases := []struct {
		name string
		cfg  s3TargetConfig
		ok   bool
	}{
		{
			name: "valid",
			cfg:  s3TargetConfig{S3Client: &s3.Client{}, S3Bucket: "b"},
			ok:   true,
		},
		{
			name: "missing client",
			cfg:  s3TargetConfig{S3Bucket: "b"},
		},
		{
			name: "missing bucket",
			cfg:  s3TargetConfig{S3Client: &s3.Client{}},
		},
		{
			name: "negative inflight",
			cfg: s3TargetConfig{S3Client: &s3.Client{}, S3Bucket: "b",
				S3MaxConcurrentOpsPerMethod: -1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tgt, err := newS3Target(tc.cfg)
			if tc.ok {
				if err != nil {
					t.Fatalf("want OK, got %v", err)
				}
				if tgt == nil {
					t.Fatalf("want non-nil target")
				}
				return
			}
			if err == nil {
				t.Fatalf("want error, got nil")
			}
		})
	}
}

func TestS3Target_EffectiveConcurrency(t *testing.T) {
	cases := []struct {
		name            string
		cfg             s3TargetConfig
		wantConcurrency int
	}{
		{
			name: "default when zero",
			cfg: s3TargetConfig{S3Client: &s3.Client{}, S3Bucket: "b",
				S3MaxConcurrentOpsPerMethod: 0},
			wantConcurrency: defaultS3MaxConcurrentOpsPerMethod,
		},
		{
			name: "explicit value passes through",
			cfg: s3TargetConfig{S3Client: &s3.Client{}, S3Bucket: "b",
				S3MaxConcurrentOpsPerMethod: 64},
			wantConcurrency: 64,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tgt, err := newS3Target(tc.cfg)
			if err != nil {
				t.Fatalf("newS3Target: %v", err)
			}
			if got := tgt.effectiveConcurrency(); got != tc.wantConcurrency {
				t.Errorf("effectiveConcurrency: want %d, got %d",
					tc.wantConcurrency, got)
			}
		})
	}
}
