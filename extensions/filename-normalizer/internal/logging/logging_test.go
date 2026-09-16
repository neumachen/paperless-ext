package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func decodeOne(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("nothing was logged")
	}
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(line), &out); err != nil {
		t.Fatalf("log line is not JSON: %v (%s)", err, line)
	}
	return out
}

func TestOutputIsJSONWithProcessIdentity(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, Options{Level: slog.LevelInfo, Application: "watcher", Instance: "watcher-1"})

	log.Info("started", slog.String("event", "worker_started"), slog.String("worker", "dispatch"))

	rec := decodeOne(t, &buf)
	for _, key := range []string{"time", "level", "msg", "application", "instance", "event", "worker"} {
		if _, ok := rec[key]; !ok {
			t.Errorf("record is missing %q: %v", key, rec)
		}
	}
	if rec["application"] != "watcher" || rec["instance"] != "watcher-1" {
		t.Errorf("process identity is wrong: %v", rec)
	}
}

func TestUnapprovedKeysAreRedacted(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, Options{Level: slog.LevelInfo, Application: "renamer", Instance: "renamer-1"})

	// A caller that reaches for a document-identifying key must not be able to
	// emit its value, whatever the key is called.
	log.Info("delivery",
		slog.String("job_id", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		slog.String("source_name", "Bank Statement - August (Final) 2026.PDF"),
		slog.String("destination_path", "/srv/fn/consume/bank_statement.pdf"),
		slog.String("password", "hunter2"))

	rec := decodeOne(t, &buf)
	if rec["job_id"] != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" {
		t.Errorf("opaque job IDs are allowed in logs but were altered: %v", rec["job_id"])
	}
	for _, key := range []string{"source_name", "destination_path", "password"} {
		if rec[key] != Redacted {
			t.Errorf("%s was not redacted: %v", key, rec[key])
		}
	}
	raw := buf.String()
	for _, leak := range []string{"Bank Statement", "bank_statement.pdf", "hunter2"} {
		if strings.Contains(raw, leak) {
			t.Errorf("the output still contains %q: %s", leak, raw)
		}
	}
}

func TestRedactionSurvivesWithAndGroups(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, Options{Level: slog.LevelInfo, Application: "watcher", Instance: "w"}).
		With(slog.String("component", "dispatch")).
		With(slog.String("source_name", "secret-document.pdf"))

	log.Info("pass")

	raw := buf.String()
	if strings.Contains(raw, "secret-document.pdf") {
		t.Errorf("a pre-bound unapproved attribute leaked: %s", raw)
	}
	if !strings.Contains(raw, `"component":"dispatch"`) {
		t.Errorf("an approved pre-bound attribute was dropped: %s", raw)
	}
}

func TestLevelFilteringApplies(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, Options{Level: slog.LevelWarn, Application: "watcher", Instance: "w"})
	log.Info("should not appear", slog.String("event", "x"))
	if buf.Len() != 0 {
		t.Errorf("an info record was emitted at warn level: %s", buf.String())
	}
	log.Warn("should appear", slog.String("event", "y"))
	if buf.Len() == 0 {
		t.Error("a warn record was suppressed at warn level")
	}
}

func TestErrorKindClassifiesWithoutRevealingDetail(t *testing.T) {
	cases := map[string]string{
		"canceled":           "context canceled",
		"timeout":            "context deadline exceeded",
		"connection_refused": `dial tcp 10.0.0.5:5432: connect: connection refused`,
		"connection_lost":    "unexpected EOF",
		"permission_denied":  "open /srv/fn/consume: permission denied",
		"storage_full":       "write /srv/fn/staging/x: no space left on device",
		"read_only":          `ERROR: cannot execute INSERT in a read-only transaction (SQLSTATE 25006)`,
		"not_found":          "stat /srv/fn/incoming: no such file or directory",
		"authentication":     `failed SASL auth: authentication failure`,
		"schema_missing":     `ERROR: relation "jobs" does not exist (SQLSTATE 42P01)`,
		"in_recovery":        "primary endpoint is in recovery",
		"server_shutdown":    "FATAL: terminating connection due to administrator command (SQLSTATE 57P01)",
		"not_connected":      "broker connection is not established",
		"unclassified":       "something unexpected happened",
	}
	for want, msg := range cases {
		if got := ErrorKind(errors.New(msg)); got != want {
			t.Errorf("ErrorKind(%q) = %q, want %q", msg, got, want)
		}
	}
	if ErrorKind(nil) != "" {
		t.Error("ErrorKind(nil) should be empty")
	}

	// The classification must not be the error text: error strings from the
	// driver and the filesystem routinely embed a DSN or a path.
	dsnErr := fmt.Errorf("failed to connect to `host=db user=fn_app database=fn`: password authentication failed")
	kind := ErrorKind(dsnErr)
	if strings.Contains(kind, "host=") || strings.Contains(kind, "fn_app") {
		t.Errorf("ErrorKind leaked connection detail: %q", kind)
	}
	if kind != "authentication" {
		t.Errorf("ErrorKind = %q, want authentication", kind)
	}
}

func TestErrorKindValuesAreSafeLabels(t *testing.T) {
	// Every classification is used as a metric label value, so each must be a
	// short lowercase identifier.
	for _, msg := range []string{
		"context canceled", "connection refused", "no space left on device",
		"permission denied", "anything at all",
	} {
		kind := ErrorKind(errors.New(msg))
		if kind == "" || len(kind) > 32 {
			t.Errorf("ErrorKind(%q) = %q is not a usable label", msg, kind)
		}
		for _, r := range kind {
			if !(r >= 'a' && r <= 'z') && r != '_' {
				t.Errorf("ErrorKind(%q) = %q contains %q", msg, kind, r)
			}
		}
	}
}

func TestErrorKindsIsTheCompleteClosedSet(t *testing.T) {
	// Metrics pre-initialise their error_kind label space from ErrorKinds, so
	// any classification the function can return must be declared there.
	declared := map[string]bool{}
	for _, k := range ErrorKinds() {
		declared[k] = true
	}
	for _, msg := range []string{
		"context canceled", "context deadline exceeded", "connection refused",
		"connection reset by peer", "no such host", "permission denied",
		"no space left on device", "read-only transaction",
		`relation "jobs" does not exist`, "role does not exist",
		"authentication failure", "in recovery",
		"terminating connection due to administrator command",
		"broker connection is not established",
		"totally novel failure",
	} {
		kind := ErrorKind(errors.New(msg))
		if !declared[kind] {
			t.Errorf("ErrorKind(%q) = %q, which ErrorKinds() does not declare", msg, kind)
		}
	}
}

func TestParseLevel(t *testing.T) {
	for in, want := range map[string]slog.Level{
		"debug": slog.LevelDebug, "info": slog.LevelInfo, "": slog.LevelInfo,
		"WARN": slog.LevelWarn, "warning": slog.LevelWarn, "Error": slog.LevelError,
	} {
		got, ok := ParseLevel(in)
		if !ok || got != want {
			t.Errorf("ParseLevel(%q) = %v,%t want %v,true", in, got, ok, want)
		}
	}
	if _, ok := ParseLevel("trace"); ok {
		t.Error("ParseLevel accepted an unknown level")
	}
}

func TestAllowListCoversTheKeysTheApplicationsUse(t *testing.T) {
	// A missing key here would silently redact real operational output.
	for _, key := range []string{
		"event", "component", "worker", "job_id", "category", "error_kind",
		"state", "outcome", "attempt", "attempts", "dependency", "queue",
		"exchange", "storage_role", "storage_status", "ready", "signal",
		"duration_ms", "concurrency", "prefetch", "delivery_tag", "redelivered",
	} {
		if !IsSafeKey(key) {
			t.Errorf("%q is used by the applications but is not on the allow-list", key)
		}
	}
	// And the reverse: nothing document-identifying may be allowed.
	for _, key := range []string{
		"source_name", "source_path", "filename", "destination_path",
		"normalized_name", "reserved_name", "fingerprint", "content_fingerprint",
		"password", "dsn", "amqp_uri", "secret", "token",
	} {
		if IsSafeKey(key) {
			t.Errorf("%q must not be loggable", key)
		}
	}
}
