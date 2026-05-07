package s3pgstore

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// stubExecutor is a no-op Executor used to satisfy Config
// validation in tests; never invoked.
type stubExecutor struct{}

func (stubExecutor) Run(context.Context, func(DBTX) error) error     { return nil }
func (stubExecutor) RunInTx(context.Context, func(DBTX) error) error { return nil }

// validConfig returns a Config that passes validation. Tests
// mutate one field at a time to verify each rule.
func validConfig() Config[map[string]any] {
	return Config[map[string]any]{
		Executor:          stubExecutor{},
		Bucket:            "bucket",
		S3Client:          &s3.Client{},
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf:    func(map[string]any) string { return "k" },
	}
}

func TestConfigValidate_OK(t *testing.T) {
	if err := validConfig().validate(); err != nil {
		t.Fatalf("baseline Config rejected: %v", err)
	}
}

func TestConfigValidate_RequiresExecutor(t *testing.T) {
	cfg := validConfig()
	cfg.Executor = nil
	requireErrContains(t, cfg.validate(), "Executor is required")
}

func TestConfigValidate_RequiresBucket(t *testing.T) {
	cfg := validConfig()
	cfg.Bucket = ""
	requireErrContains(t, cfg.validate(), "Bucket is required")
}

func TestConfigValidate_RequiresS3Client(t *testing.T) {
	cfg := validConfig()
	cfg.S3Client = nil
	requireErrContains(t, cfg.validate(), "S3Client is required")
}

func TestConfigValidate_RequiresPartitionKeyParts(t *testing.T) {
	cfg := validConfig()
	cfg.PartitionKeyParts = nil
	requireErrContains(t, cfg.validate(), "PartitionKeyParts must be non-empty")
}

func TestConfigValidate_RequiresPartitionKeyOf(t *testing.T) {
	cfg := validConfig()
	cfg.PartitionKeyOf = nil
	requireErrContains(t, cfg.validate(), "PartitionKeyOf is required")
}

func TestConfigValidate_TablePrefix(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		ok     bool
	}{
		{"empty (default applied)", "", true},
		{"good underscored", "myapp_", true},
		{"good multi underscored", "ab_cd_", true},
		{"missing trailing underscore", "myapp", false},
		{"starts with digit", "1myapp_", false},
		{"contains uppercase", "MyApp_", false},
		{"contains hyphen", "my-app_", false},
		{"too long", strings.Repeat("a", maxTablePrefixLen) + "_", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.TablePrefix = tc.prefix
			err := cfg.validate()
			if tc.ok && err != nil {
				t.Fatalf("want OK, got %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("want error for %q, got nil", tc.prefix)
			}
		})
	}
}

func TestConfigValidate_PartitionKeyParts(t *testing.T) {
	cfg := validConfig()
	cfg.PartitionKeyParts = []string{"valid", "Bad-Name"}
	requireErrContains(t, cfg.validate(), "PartitionKeyParts entry")
}

func TestConfigValidate_ExtensionColumnTypes(t *testing.T) {
	cfg := validConfig()
	cfg.ExtensionColumns = []ExtensionColumn{
		{Name: "ok", Type: "TEXT"},
		{Name: "ok2", Type: "JSONB"}, // disallowed
	}
	requireErrContains(t, cfg.validate(), "is not allowed")
}

func TestConfigValidate_ExtensionColumnNames(t *testing.T) {
	cfg := validConfig()
	cfg.ExtensionColumns = []ExtensionColumn{
		{Name: "Bad", Type: "TEXT"},
	}
	requireErrContains(t, cfg.validate(), "is not a valid identifier")
}

func TestConfigValidate_DuplicateExtensionColumnNames(t *testing.T) {
	cfg := validConfig()
	cfg.ExtensionColumns = []ExtensionColumn{
		{Name: "a", Type: "TEXT"},
		{Name: "a", Type: "BIGINT"},
	}
	requireErrContains(t, cfg.validate(), "duplicate Name")
}

func TestConfigValidate_AllExtensionColumnTypes(t *testing.T) {
	for _, tp := range allowedExtensionTypes {
		t.Run(tp, func(t *testing.T) {
			cfg := validConfig()
			cfg.ExtensionColumns = []ExtensionColumn{{Name: "x", Type: tp}}
			if err := cfg.validate(); err != nil {
				t.Fatalf("type %q rejected: %v", tp, err)
			}
		})
	}
}

func TestConfigValidate_MaterializedView(t *testing.T) {
	cfg := validConfig()
	cfg.MaterializedViews = []MaterializedViewDef[map[string]any]{
		{Name: "ok", Columns: []string{"a"}, Of: func(map[string]any) ([][]string, error) { return nil, nil }},
		{Name: "missing_of", Columns: []string{"a"}}, // no Of
	}
	requireErrContains(t, cfg.validate(), "Of is required")
}

func TestConfigValidate_MaterializedViewNoColumns(t *testing.T) {
	cfg := validConfig()
	cfg.MaterializedViews = []MaterializedViewDef[map[string]any]{
		{Name: "x", Of: func(map[string]any) ([][]string, error) { return nil, nil }},
	}
	requireErrContains(t, cfg.validate(), "Columns must be non-empty")
}

func TestConfigValidate_DuplicateMaterializedViewNames(t *testing.T) {
	cfg := validConfig()
	mv := MaterializedViewDef[map[string]any]{
		Name: "v", Columns: []string{"a"},
		Of: func(map[string]any) ([][]string, error) { return nil, nil },
	}
	cfg.MaterializedViews = []MaterializedViewDef[map[string]any]{mv, mv}
	requireErrContains(t, cfg.validate(), "duplicate Name")
}

func TestConfigValidate_DedupBothOrNeither(t *testing.T) {
	cfg := validConfig()
	cfg.EntityKeyOf = func(map[string]any) string { return "" }
	requireErrContains(t, cfg.validate(),
		"EntityKeyOf and VersionOf must be set together")

	cfg = validConfig()
	cfg.VersionOf = func(map[string]any) int64 { return 0 }
	requireErrContains(t, cfg.validate(),
		"EntityKeyOf and VersionOf must be set together")

	cfg = validConfig()
	cfg.EntityKeyOf = func(map[string]any) string { return "" }
	cfg.VersionOf = func(map[string]any) int64 { return 0 }
	if err := cfg.validate(); err != nil {
		t.Fatalf("both-set Config rejected: %v", err)
	}
}

func TestConfigValidate_NegativeEncodeBufPoolMaxBytes(t *testing.T) {
	cfg := validConfig()
	cfg.EncodeBufPoolMaxBytes = -1
	requireErrContains(t, cfg.validate(), "EncodeBufPoolMaxBytes must be >= 0")
}

func TestConfigResolved_AppliesDefaults(t *testing.T) {
	cfg := validConfig()
	r := cfg.resolved()
	if r.SchemaName != DefaultSchemaName {
		t.Errorf("SchemaName: want %q, got %q", DefaultSchemaName, r.SchemaName)
	}
	if r.TablePrefix != DefaultTablePrefix {
		t.Errorf("TablePrefix: want %q, got %q", DefaultTablePrefix, r.TablePrefix)
	}
	if r.NotifyChannel != DefaultNotifyChannel {
		t.Errorf("NotifyChannel: want %q, got %q",
			DefaultNotifyChannel, r.NotifyChannel)
	}
}

func TestConfigResolved_PreservesUserValues(t *testing.T) {
	cfg := validConfig()
	cfg.SchemaName = "custom"
	cfg.TablePrefix = "myapp_"
	cfg.NotifyChannel = "custom_chan"
	r := cfg.resolved()
	if r.SchemaName != "custom" || r.TablePrefix != "myapp_" ||
		r.NotifyChannel != "custom_chan" {
		t.Fatalf("user values overwritten: %+v", r)
	}
}

// requireErrContains fails the test if err is nil or its
// message doesn't include sub. Cheap substitute for testify.
func requireErrContains(t *testing.T, err error, sub string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error containing %q, got nil", sub)
	}
	if !strings.Contains(err.Error(), sub) {
		t.Fatalf("want error containing %q, got %q", sub, err.Error())
	}
}

// Compile-time assertion: poolExecutor satisfies Executor.
var _ Executor = (*poolExecutor)(nil)

// Compile-time guard so a future refactor of NewPoolExecutor
// can't quietly change the return type.
var _ Executor = NewPoolExecutor((*pgxpool.Pool)(nil))
