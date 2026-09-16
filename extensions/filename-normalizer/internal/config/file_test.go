package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeConfig writes a configuration file into the test's own directory and
// points FN_CONFIG_FILE at it.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "normalizer.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("FN_CONFIG_FILE", path)
	return path
}

func TestConfigFileRejectsUnknownFields(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"normalisation":{"max_name_bytes":100}}`)

	_, err := LoadWatcher()
	if err == nil {
		t.Fatal("a misspelled section must not be silently ignored")
	}
	if !strings.Contains(err.Error(), "normalisation") {
		t.Errorf("error does not name the unknown field: %v", err)
	}
}

func TestConfigFileRejectsUnknownNestedFields(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"discovery":{"stability_intervals":"30s"}}`)

	if _, err := LoadWatcher(); err == nil {
		t.Fatal("a misspelled key inside a section must not be ignored")
	}
}

func TestConfigFileRejectsWrongVersion(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":2}`)

	_, err := LoadWatcher()
	if err == nil {
		t.Fatal("an unsupported schema version must be rejected")
	}
	if !strings.Contains(err.Error(), "version 2") {
		t.Errorf("error does not name the version: %v", err)
	}
}

func TestConfigFileRejectsTrailingContent(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1}{"version":1}`)

	if _, err := LoadWatcher(); err == nil {
		t.Fatal("trailing content after the object must be rejected")
	}
}

func TestMissingConfigFileIsAHardFailure(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_CONFIG_FILE", filepath.Join(t.TempDir(), "absent.json"))

	if _, err := LoadWatcher(); err == nil {
		t.Fatal("a named but unreadable configuration file must fail the load")
	}
}

func TestNoConfigFileIsFine(t *testing.T) {
	minimalEnv(t)
	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher without a file: %v", err)
	}
	if cfg.ConfigFile != "" {
		t.Errorf("ConfigFile = %q, want empty", cfg.ConfigFile)
	}
}

// TestEnvironmentOverridesFile is the documented precedence.
func TestEnvironmentOverridesFile(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"discovery":{"enabled":true,"interval":"45s"}}`)
	t.Setenv("FN_DISCOVERY_INTERVAL", "90s")

	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	if !cfg.Discovery.Enabled {
		t.Error("the file should have enabled discovery")
	}
	if cfg.Discovery.Interval != 90*time.Second {
		t.Errorf("interval = %s, want the environment's 90s", cfg.Discovery.Interval)
	}
}

// TestFileSuppliesStorageRootsBeneathTheEnvironment covers the other half of
// the same precedence rule.
func TestFileSuppliesStorageRootsBeneathTheEnvironment(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"storage":{"failed":"/srv/from-file"}}`)

	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	if cfg.Storage.Failed != "/srv/from-file" {
		t.Errorf("failed root = %q, want the file's value", cfg.Storage.Failed)
	}

	t.Setenv("FN_STORAGE_FAILED", "/srv/from-env")
	cfg, err = LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	if cfg.Storage.Failed != "/srv/from-env" {
		t.Errorf("failed root = %q, want the environment's value", cfg.Storage.Failed)
	}
}

func TestNestedStorageRootsAreRejected(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_STORAGE_STAGING", "/srv/fn/incoming/staging")
	t.Setenv("FN_STORAGE_INCOMING", "/srv/fn/incoming")

	_, err := LoadWatcher()
	if err == nil {
		t.Fatal("a staging root inside the incoming root must be rejected")
	}
	if !strings.Contains(err.Error(), "inside") {
		t.Errorf("error does not explain the nesting: %v", err)
	}
}

func TestDuplicateStorageRootsAreRejected(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_STORAGE_STAGING", "/srv/fn/shared")
	t.Setenv("FN_STORAGE_QUEUED", "/srv/fn/shared")

	if _, err := LoadWatcher(); err == nil {
		t.Fatal("two roles sharing one path must be rejected")
	}
}

func TestRelativeStorageRootIsRejected(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_STORAGE_STAGING", "relative/path")

	if _, err := LoadWatcher(); err == nil {
		t.Fatal("a relative root must be rejected")
	}
}

func TestInvalidSelectionPatternIsRejected(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"discovery":{"include":["([unclosed"]}}`)

	_, err := LoadWatcher()
	if err == nil {
		t.Fatal("an uncompilable selection pattern must be rejected")
	}
	if !strings.Contains(err.Error(), "discovery.include[0]") {
		t.Errorf("error does not locate the pattern: %v", err)
	}
}

func TestNonIdempotentRuleIsRejectedAtLoad(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"normalization":{"rules":[
		{"name":"grows","pattern":"^(.+)$","replacement":"x$1"}
	]}}`)

	_, err := LoadWatcher()
	if err == nil {
		t.Fatal("a non-idempotent rule set must be rejected before intake begins")
	}
	if !strings.Contains(err.Error(), "idempotent") {
		t.Errorf("error does not explain the problem: %v", err)
	}
}

func TestInvalidRuleIsRejectedAtLoad(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"normalization":{"rules":[
		{"name":"Bad Name","pattern":"a"}
	]}}`)

	if _, err := LoadWatcher(); err == nil {
		t.Fatal("an invalid rule name must be rejected")
	}
}

// TestPolicyIdentityTracksNameAffectingSettings is what makes a rolling
// restart safe: a change that can alter a produced name changes the identity.
func TestPolicyIdentityTracksNameAffectingSettings(t *testing.T) {
	minimalEnv(t)

	writeConfig(t, `{"version":1}`)
	base, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}

	writeConfig(t, `{"version":1,"normalization":{"max_name_bytes":120}}`)
	shorter, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	if shorter.Policy.Identity == base.Policy.Identity {
		t.Error("changing the byte cap did not change the policy identity")
	}

	writeConfig(t, `{"version":1,"normalization":{"rules":[
		{"name":"drop-copy","pattern":"-copy$","replacement":""}
	]}}`)
	ruled, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	if ruled.Policy.Identity == base.Policy.Identity {
		t.Error("adding a rule did not change the policy identity")
	}
}

// TestPolicyIdentityIgnoresOperationalSettings: tuning concurrency must not
// strand queued work.
func TestPolicyIdentityIgnoresOperationalSettings(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"processing":{"concurrency":2}}`)
	a, err := LoadRenamer()
	if err != nil {
		t.Fatalf("LoadRenamer: %v", err)
	}

	writeConfig(t, `{"version":1,"processing":{"concurrency":8},"discovery":{"interval":"5s"}}`)
	b, err := LoadRenamer()
	if err != nil {
		t.Fatalf("LoadRenamer: %v", err)
	}
	if a.Policy.Identity != b.Policy.Identity {
		t.Error("an operational change altered the policy identity")
	}
}

// TestBothApplicationsAgreeOnPolicyIdentity: the watcher stamps it and the
// renamer checks it, so they must compute the same value from one file.
func TestBothApplicationsAgreeOnPolicyIdentity(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"normalization":{"max_name_bytes":180,"rules":[
		{"name":"trim-scan","pattern":"^scan[_-]","replacement":"","case_insensitive":true}
	]}}`)

	w, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	r, err := LoadRenamer()
	if err != nil {
		t.Fatalf("LoadRenamer: %v", err)
	}
	if w.Policy.Identity != r.Policy.Identity {
		t.Errorf("watcher %q and renamer %q disagree", w.Policy.Identity, r.Policy.Identity)
	}
}

func TestProcessingSettingsComeFromTheFile(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"processing":{"concurrency":3,"prefetch":6,"max_delivery_attempts":9,"dry_run":true}}`)

	cfg, err := LoadRenamer()
	if err != nil {
		t.Fatalf("LoadRenamer: %v", err)
	}
	if cfg.Concurrency != 3 || cfg.Prefetch != 6 || cfg.MaxDeliveryAttempts != 9 || !cfg.DryRun {
		t.Errorf("file processing settings not applied: %+v", cfg)
	}
}

func TestSelectionMatcher(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"discovery":{
		"include":["\\.(pdf|jpe?g)$"],
		"exclude":["^draft-"],
		"match_case_insensitive":true,
		"temporary_suffixes":[".part",".tmp"]
	}}`)

	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	m := cfg.Discovery.Matcher

	cases := []struct {
		base   string
		want   bool
		reason string
	}{
		{"statement.pdf", true, ""},
		{"Statement.PDF", true, ""},
		{"photo.jpeg", true, ""},
		{"notes.txt", false, "not_included"},
		{"draft-statement.pdf", false, "excluded_by_pattern"},
		{"statement.pdf.part", false, "temporary_suffix"},
		{"statement.pdf.TMP", false, "temporary_suffix"},
	}
	for _, c := range cases {
		ok, reason := m.Selects(c.base)
		if ok != c.want || reason != c.reason {
			t.Errorf("Selects(%q) = (%v, %q), want (%v, %q)", c.base, ok, reason, c.want, c.reason)
		}
	}
}

// TestExcludeBeatsInclude documents the precedence inside selection.
func TestExcludeBeatsInclude(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"discovery":{"include":["\\.pdf$"],"exclude":["secret"]}}`)

	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	if ok, _ := cfg.Discovery.Matcher.Selects("secret.pdf"); ok {
		t.Error("an excluded name was selected because it also matched an include pattern")
	}
}

func TestCompletionContractIsValidated(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"discovery":{"completion":"vibes"}}`)

	if _, err := LoadWatcher(); err == nil {
		t.Fatal("an unknown completion contract must be rejected")
	}

	writeConfig(t, `{"version":1,"discovery":{"completion":"rename"}}`)
	cfg, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}
	if cfg.Discovery.Completion != CompletionRename {
		t.Errorf("completion = %q", cfg.Discovery.Completion)
	}
}

func TestOutOfRangeNumbersAreRejected(t *testing.T) {
	for _, body := range []string{
		`{"version":1,"normalization":{"max_name_bytes":4}}`,
		`{"version":1,"normalization":{"max_extension_length":900}}`,
		`{"version":1,"discovery":{"batch":0,"interval":"1ns"}}`,
		`{"version":1,"discovery":{"max_file_bytes":-1}}`,
		`{"version":1,"discovery":{"interval":"not-a-duration"}}`,
	} {
		t.Run(body, func(t *testing.T) {
			minimalEnv(t)
			writeConfig(t, body)
			if _, err := LoadWatcher(); err == nil {
				t.Errorf("expected rejection for %s", body)
			}
		})
	}
}

// TestConfigFileHasNowhereToPutASecret is a structural privacy guarantee: the
// schema simply has no credential fields, so an example file cannot leak one.
func TestConfigFileHasNowhereToPutASecret(t *testing.T) {
	minimalEnv(t)
	writeConfig(t, `{"version":1,"database":{"password":"hunter2"}}`)

	_, err := LoadWatcher()
	if err == nil {
		t.Fatal("the schema must not accept a credential section")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Error("the rejection echoed the supplied value")
	}
}
