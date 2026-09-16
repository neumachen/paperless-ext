package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// minimalEnv sets the variables every application requires.
func minimalEnv(t *testing.T) {
	t.Helper()
	for k, v := range map[string]string{
		"FN_DB_PRIMARY_HOST": "postgres-primary",
		"FN_DB_NAME":         "filename_normalizer",
		"FN_DB_USER":         "fn_app",
		"FN_DB_PASSWORD":     "db-secret",
		"FN_AMQP_HOST":       "rabbitmq",
		"FN_AMQP_USER":       "fn_app",
		"FN_AMQP_PASSWORD":   "amqp-secret",
	} {
		t.Setenv(k, v)
	}
}

func TestLoadWatcherAppliesDocumentedDefaults(t *testing.T) {
	minimalEnv(t)

	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	if cfg.Application != AppWatcher {
		t.Errorf("application = %q", cfg.Application)
	}
	if cfg.Broker.Queue != "filename_normalizer.jobs.v1" {
		t.Errorf("default queue = %q", cfg.Broker.Queue)
	}
	if cfg.Broker.DeliveryLimit != 5 {
		t.Errorf("default delivery limit = %d", cfg.Broker.DeliveryLimit)
	}
	// The watcher applies migrations by default; the renamer does not, so two
	// instances cannot race on DDL.
	if !cfg.Database.ApplyMigrations {
		t.Error("the watcher should apply migrations by default")
	}
	// Discovery is implemented now, but it stays off unless a deployment
	// declares it: a watcher that starts scanning a root nobody configured is
	// a surprise, not a default.
	if cfg.Discovery.Enabled {
		t.Error("discovery must default to disabled")
	}
	if cfg.Discovery.Completion != CompletionStability {
		t.Errorf("default completion contract = %q", cfg.Discovery.Completion)
	}
	if cfg.Discovery.StabilityInterval != 30*time.Second {
		t.Errorf("default stability interval = %s", cfg.Discovery.StabilityInterval)
	}
	if cfg.Policy.Identity == "" {
		t.Error("the watcher must carry a policy identity")
	}
}

func TestLoadRenamerDefaultsPrefetchToConcurrency(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_RENAMER_CONCURRENCY", "4")

	cfg, err := LoadRenamer()
	if err != nil {
		t.Fatalf("LoadRenamer: %v", err)
	}
	if cfg.Concurrency != 4 {
		t.Errorf("concurrency = %d", cfg.Concurrency)
	}
	// Prefetching more than the instance can process would buffer work it has
	// no capacity for, so the window follows the bound by default.
	if cfg.Prefetch != 4 {
		t.Errorf("prefetch = %d, expected it to follow concurrency", cfg.Prefetch)
	}
	if cfg.Database.ApplyMigrations {
		t.Error("the renamer must not apply migrations by default")
	}
}

func TestLoadRenamerRejectsPrefetchBelowConcurrency(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_RENAMER_CONCURRENCY", "4")
	t.Setenv("FN_RENAMER_PREFETCH", "2")

	_, err := LoadRenamer()
	if err == nil {
		t.Fatal("expected a validation error")
	}
	if !strings.Contains(err.Error(), "FN_RENAMER_PREFETCH") {
		t.Errorf("error does not name the offending variable: %v", err)
	}
}

func TestMissingRequiredVariablesAreAllReported(t *testing.T) {
	for _, k := range []string{
		"FN_DB_PRIMARY_HOST", "FN_DB_NAME", "FN_DB_USER", "FN_DB_PASSWORD",
		"FN_AMQP_HOST", "FN_AMQP_USER", "FN_AMQP_PASSWORD",
	} {
		t.Setenv(k, "")
		_ = os.Unsetenv(k)
	}

	_, err := LoadWatcher()
	if err == nil {
		t.Fatal("expected a validation error with no configuration present")
	}
	var ve *ValidationError
	if !asValidationError(err, &ve) {
		t.Fatalf("error is not a ValidationError: %T", err)
	}
	// Reporting every problem at once avoids one restart per missing variable.
	if len(ve.Problems) < 7 {
		t.Errorf("expected every missing variable to be reported, got %d: %v", len(ve.Problems), ve.Problems)
	}
}

func asValidationError(err error, target **ValidationError) bool {
	ve, ok := err.(*ValidationError)
	if ok {
		*target = ve
	}
	return ok
}

func TestSecretsCanArriveFromAFile(t *testing.T) {
	minimalEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "db-password")
	if err := os.WriteFile(path, []byte("from-a-mounted-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = os.Unsetenv("FN_DB_PASSWORD")
	t.Setenv("FN_DB_PASSWORD_FILE", path)

	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	if cfg.Database.Password != "from-a-mounted-file" {
		t.Errorf("password = %q; the trailing newline should be trimmed", cfg.Database.Password)
	}
}

func TestUnreadableSecretFileIsAnError(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_DB_PASSWORD_FILE", filepath.Join(t.TempDir(), "absent"))

	_, err := LoadWatcher()
	if err == nil {
		t.Fatal("expected an error for an unreadable secret file")
	}
	if !strings.Contains(err.Error(), "FN_DB_PASSWORD_FILE") {
		t.Errorf("error does not name the variable: %v", err)
	}
}

func TestDiscoveryCanBeEnabledFromTheEnvironment(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_DISCOVERY_ENABLED", "true")

	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	if !cfg.Discovery.Enabled {
		t.Error("FN_DISCOVERY_ENABLED=true did not enable discovery")
	}
}

func TestEnablingCleanupIsAStartupError(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_CLEANUP_ENABLED", "true")

	// Destructive source deletion and ledger purging are unresolved policy.
	// Accepting the flag would imply a behaviour that does not exist.
	_, err := LoadWatcher()
	if err == nil {
		t.Fatal("enabling cleanup must fail while the retention policy is unresolved")
	}
	if !strings.Contains(err.Error(), "FN_CLEANUP_ENABLED") {
		t.Errorf("error does not name the variable: %v", err)
	}
}

func TestRelativeStoragePathsAreRejected(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_STORAGE_INCOMING", "relative/incoming")

	_, err := LoadWatcher()
	if err == nil {
		t.Fatal("a relative storage root must be rejected")
	}
	if !strings.Contains(err.Error(), "absolute") {
		t.Errorf("error does not explain the requirement: %v", err)
	}
}

func TestOutOfRangeValuesAreRejected(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"FN_RENAMER_CONCURRENCY", "0"},
		{"FN_RENAMER_CONCURRENCY", "65"},
		{"FN_AMQP_DELIVERY_LIMIT", "0"},
		{"FN_SHUTDOWN_TIMEOUT", "1ms"},
		{"FN_LOG_LEVEL", "verbose"},
		{"FN_DB_SSLMODE", "maybe"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			minimalEnv(t)
			t.Setenv(tc.key, tc.value)
			if _, err := LoadRenamer(); err == nil {
				t.Errorf("%s=%s was accepted", tc.key, tc.value)
			}
		})
	}
}

func TestReplicaRequiredWithoutAHostIsRejected(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_DB_REPLICA_REQUIRED", "true")

	if _, err := LoadWatcher(); err == nil {
		t.Fatal("requiring a standby without configuring one must be rejected")
	}
}

func TestSummaryContainsNoCredentials(t *testing.T) {
	minimalEnv(t)
	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}

	// Summary is what reaches the log stream at startup.
	for _, v := range cfg.Summary() {
		s := toString(v)
		for _, secret := range []string{"db-secret", "amqp-secret"} {
			if strings.Contains(s, secret) {
				t.Errorf("Summary leaked a credential: %q", s)
			}
		}
	}
	if len(cfg.Summary()) == 0 {
		t.Error("Summary is empty; startup would log nothing about its configuration")
	}
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		return ""
	}
}

func TestDSNAndURIEscapeCredentials(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_AMQP_PASSWORD", "p@ss/word:with?specials")
	t.Setenv("FN_AMQP_VHOST", "filename-normalizer")

	cfg, err := LoadRenamer()
	if err != nil {
		t.Fatalf("LoadRenamer: %v", err)
	}

	uri := cfg.Broker.URI()
	// An unescaped ':' or '/' in the password would change which host and
	// vhost the client actually connects to.
	if strings.Contains(uri, "p@ss/word:with?specials") {
		t.Errorf("the password was not escaped into the URI: %s", uri)
	}
	if !strings.Contains(uri, "%40") || !strings.Contains(uri, "%2F") {
		t.Errorf("expected percent-encoding in the URI: %s", uri)
	}
	if !strings.HasSuffix(uri, "/filename-normalizer") {
		t.Errorf("vhost was not appended correctly: %s", uri)
	}

	dsn := cfg.Database.DSN()
	for _, want := range []string{"host='postgres-primary'", "dbname='filename_normalizer'", "sslmode='disable'"} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN is missing %q: %s", want, dsn)
		}
	}
	if cfg.Database.ReplicaDSN() != "" {
		t.Error("ReplicaDSN should be empty when no standby is configured")
	}
}

func TestDSNQuotesEveryValue(t *testing.T) {
	minimalEnv(t)
	// A password that is perfectly valid for PostgreSQL but would break an
	// unquoted keyword/value connection string.
	const nasty = `p ass'w\"ord:/@ #x`
	t.Setenv("FN_DB_PASSWORD", nasty)

	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	dsn := cfg.Database.DSN()

	// Every parameter must still be present and parseable as one value.
	for _, want := range []string{
		"host='postgres-primary'", "port='5432'",
		"dbname='filename_normalizer'", "user='fn_app'",
		"sslmode='disable'", "application_name='filename-normalizer'",
	} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN is missing %q: %s", want, dsn)
		}
	}
	// The password must appear exactly once, quoted, with backslashes and
	// single quotes escaped.
	wantPassword := `password='p ass\'w\\"ord:/@ #x'`
	if !strings.Contains(dsn, wantPassword) {
		t.Errorf("password is not correctly quoted.\n got: %s\nwant to contain: %s", dsn, wantPassword)
	}
	// An unquoted interpolation would have let the space end the value and
	// turned the rest into further parameters. Assert that shape is gone.
	if strings.Contains(dsn, "password="+nasty) {
		t.Errorf("the password was interpolated unquoted: %s", dsn)
	}
}

func TestQuoteDSNValueFollowsLibpqRules(t *testing.T) {
	for in, want := range map[string]string{
		"":              `''`,
		"simple":        `'simple'`,
		"with space":    `'with space'`,
		`it's`:          `'it\'s'`,
		`back\slash`:    `'back\\slash'`,
		`both'\`:        `'both\'\\'`,
		"trailing ":     `'trailing '`,
		"equals=inside": `'equals=inside'`,
	} {
		if got := quoteDSNValue(in); got != want {
			t.Errorf("quoteDSNValue(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBrokerURIEscapesSpaceAndQuote(t *testing.T) {
	minimalEnv(t)
	const nasty = `am q'p:pass/word@host`
	t.Setenv("FN_AMQP_PASSWORD", nasty)

	cfg, err := LoadRenamer()
	if err != nil {
		t.Fatalf("LoadRenamer: %v", err)
	}
	uri := cfg.Broker.URI()

	// An unescaped space, colon, slash or at-sign in the userinfo would change
	// which host and vhost the client actually connects to.
	if strings.Contains(uri, nasty) {
		t.Errorf("the password was not escaped into the URI: %s", uri)
	}
	for _, want := range []string{"%20", "%27", "%3A", "%2F", "%40"} {
		if !strings.Contains(uri, want) {
			t.Errorf("URI is missing the escape %s: %s", want, uri)
		}
	}
	// The authority must still resolve to the configured host.
	if !strings.Contains(uri, "@rabbitmq:5672") {
		t.Errorf("URI does not address the configured host: %s", uri)
	}
}

func TestRolesForAppliesLeastPrivilege(t *testing.T) {
	s := Storage{
		Incoming: "/srv/in", Queued: "/srv/q", Staging: "/srv/s",
		Consume: "/srv/c", Failed: "/srv/f",
	}

	byName := func(app Application) map[string]bool {
		out := map[string]bool{}
		for _, r := range s.RolesFor(app) {
			out[r.Name] = r.WriteRequired
		}
		return out
	}

	w := byName(AppWatcher)
	r := byName(AppRenamer)

	if len(w) != 5 || len(r) != 5 {
		t.Fatalf("expected five roles per application, got %d and %d", len(w), len(r))
	}
	// Publication belongs to the renamer, so the watcher must not require
	// write access to the consumer's directory.
	if w["consume"] {
		t.Error("the watcher must not require write access to consume")
	}
	if !r["consume"] {
		t.Error("the renamer must require write access to consume")
	}
	if w["incoming"] || r["incoming"] {
		t.Error("neither application should require write access to incoming")
	}
	for _, role := range []string{"queued", "staging", "failed"} {
		if !w[role] || !r[role] {
			t.Errorf("both applications must be able to write %q", role)
		}
	}
}
