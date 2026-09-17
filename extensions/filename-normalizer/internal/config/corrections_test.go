package config

import (
	"os"
	"strings"
	"testing"
)

var (
	osMkdirAll = func(p string) error { return os.MkdirAll(p, 0o755) }
	osSymlink  = os.Symlink
)

// Regression tests for the third review round's configuration findings.

// FN-N006A: the policy fingerprint joined rule fields with an unescaped "|",
// so two rule sets with genuinely different naming behaviour hashed the same.
//
// Before: pattern "qz|qx" with replacement "qy" and pattern "qz" with
// replacement "qx|qy" produced identical fingerprint input. Both converge, so
// nothing else caught it, and two deployments naming documents differently
// would have shared a policy identity -- defeating the mechanism that exists
// precisely to tell differently-behaving configurations apart.
func TestPolicyIdentityDistinguishesAmbiguouslyJoinedRules(t *testing.T) {
	minimalEnv(t)

	writeConfig(t, `{"version":1,"normalization":{"rules":[
		{"name":"r","pattern":"qz|qx","replacement":"qy"}
	]}}`)
	a, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}

	writeConfig(t, `{"version":1,"normalization":{"rules":[
		{"name":"r","pattern":"qz","replacement":"qx|qy"}
	]}}`)
	b, err := LoadWatcher()
	if err != nil {
		t.Fatalf("LoadWatcher: %v", err)
	}

	if a.Policy.Identity == b.Policy.Identity {
		t.Errorf("two rule sets that name documents differently share the identity %s", a.Policy.Identity)
	}

	// And they really do differ, so the identities are obliged to.
	first, err := a.Policy.Normalize("qz.pdf", "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d")
	if err != nil {
		t.Fatalf("normalize under the first policy: %v", err)
	}
	second, err := b.Policy.Normalize("qz.pdf", "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d")
	if err != nil {
		t.Fatalf("normalize under the second policy: %v", err)
	}
	if first.Name == second.Name {
		t.Fatalf("the two policies produce the same name %q, so this test proves nothing", first.Name)
	}
	t.Logf("qz.pdf -> %q under the first policy and %q under the second", first.Name, second.Name)
}

// FN-N007: file-provided processing values reached intVal as its default, and
// intVal returns a default without range-checking it.
//
// Before: a concurrency of 10000 or a negative delivery limit passed startup
// while the gRPC validator rejected the same document -- so validating a
// configuration said nothing reliable about whether it would start.
func TestOutOfRangeProcessingValuesFromTheFileAreRejected(t *testing.T) {
	cases := []struct{ name, doc string }{
		{"concurrency too high", `{"version":1,"processing":{"concurrency":10000}}`},
		{"concurrency zero-adjacent negative", `{"version":1,"processing":{"concurrency":-1}}`},
		{"negative delivery limit", `{"version":1,"processing":{"max_delivery_attempts":-5}}`},
		{"prefetch too high", `{"version":1,"processing":{"prefetch":100000}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			minimalEnv(t)
			writeConfig(t, c.doc)

			_, startupErr := LoadRenamer()
			_, problems := ValidateDocument([]byte(c.doc))

			if startupErr == nil {
				t.Errorf("startup accepted %s", c.doc)
			}
			if len(problems) == 0 {
				t.Errorf("document validation accepted %s", c.doc)
			}
			// The two must agree: that is the property, not merely that each
			// rejects something.
			if (startupErr == nil) != (len(problems) == 0) {
				t.Errorf("startup and validation disagree: startup err=%v, validation problems=%v",
					startupErr, problems)
			}
		})
	}
}

// FN-N007: document validation omitted the equal/nested root checks that
// startup performs, so a document that could never start validated cleanly.
func TestDocumentValidationAppliesTheRootChecks(t *testing.T) {
	nested := `{"version":1,"storage":{
		"incoming":"/srv/fn/incoming",
		"queued":"/srv/fn/queued",
		"staging":"/srv/fn/incoming/staging",
		"consume":"/srv/fn/consume",
		"failed":"/srv/fn/failed"
	}}`
	_, problems := ValidateDocument([]byte(nested))
	if len(problems) == 0 {
		t.Fatal("document validation accepted a staging root inside the incoming root")
	}
	found := false
	for _, p := range problems {
		if strings.Contains(p, "inside") {
			found = true
		}
	}
	if !found {
		t.Errorf("the problems do not mention the nesting: %v", problems)
	}

	duplicate := `{"version":1,"storage":{
		"incoming":"/srv/fn/incoming",
		"queued":"/srv/fn/shared",
		"staging":"/srv/fn/shared",
		"consume":"/srv/fn/consume",
		"failed":"/srv/fn/failed"
	}}`
	if _, problems := ValidateDocument([]byte(duplicate)); len(problems) == 0 {
		t.Error("document validation accepted two roles sharing one path")
	}
}

// FN-N009: an expanding rule set must be refused by the validator, which is
// the path an untrusted candidate document takes into the running process.
func TestValidationRefusesExponentiallyExpandingRules(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`{"version":1,"normalization":{"rules":[`)
	for i := 0; i < 40; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"name":"double-`)
		sb.WriteString(string(rune('a' + i%26)))
		sb.WriteString(string(rune('a' + i/26)))
		sb.WriteString(`","pattern":"^(.+)$","replacement":"${1}${1}"}`)
	}
	sb.WriteString(`]}}`)

	_, problems := ValidateDocument([]byte(sb.String()))
	if len(problems) == 0 {
		t.Fatal("validation accepted a rule set that expands without bound")
	}
}

// FN-N008: the validator must refuse a transliterating rule, because the
// Unicode-preservation requirement is explicitly retained.
func TestValidationRefusesTransliteratingRules(t *testing.T) {
	doc := `{"version":1,"normalization":{"rules":[
		{"name":"umlaut","pattern":"ü","replacement":"u","all":true,"case_insensitive":true}
	]}}`
	_, problems := ValidateDocument([]byte(doc))
	if len(problems) == 0 {
		t.Fatal("validation accepted a rule that maps a preserved character to ASCII")
	}

	minimalEnv(t)
	writeConfig(t, doc)
	if _, err := LoadRenamer(); err == nil {
		t.Error("startup accepted a transliterating rule")
	}
}

// FN-N005: two roots that resolve to one directory must be refused, even when
// their pathnames differ. Exercised against a real filesystem.
func TestAliasedRootsAreRejected(t *testing.T) {
	minimalEnv(t)
	dir := t.TempDir()

	real := dir + "/real"
	alias := dir + "/alias"
	if err := mkdirAll(real); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := symlink(real, alias); err != nil {
		t.Skipf("symlinks are not available here: %v", err)
	}

	// Two roles pointing at the same directory through different names. The
	// textual checks see two different strings and pass; only the device and
	// inode comparison catches it.
	t.Setenv("FN_STORAGE_STAGING", real)
	t.Setenv("FN_STORAGE_CONSUME", alias)
	t.Setenv("FN_STORAGE_INCOMING", dir+"/in")
	t.Setenv("FN_STORAGE_QUEUED", dir+"/q")
	t.Setenv("FN_STORAGE_FAILED", dir+"/f")
	for _, p := range []string{dir + "/in", dir + "/q", dir + "/f"} {
		if err := mkdirAll(p); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}

	_, err := LoadRenamer()
	if err == nil {
		t.Fatal("two roots resolving to one directory were accepted; a working copy would be visible to the consumer")
	}
	if !strings.Contains(err.Error(), "same directory") {
		t.Errorf("the error does not explain the aliasing: %v", err)
	}
}

// Small helpers so the test reads as a filesystem scenario rather than as
// os-package plumbing.
func mkdirAll(p string) error           { return osMkdirAll(p) }
func symlink(target, link string) error { return osSymlink(target, link) }
