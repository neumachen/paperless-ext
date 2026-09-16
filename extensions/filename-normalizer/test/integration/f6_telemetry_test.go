//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// F6 — telemetry and isolation.
//
// Two separate obligations. Telemetry must be real: structured JSON logs and a
// scrapable metric surface on the running applications. Privacy must hold:
// document names, paths, contents, fingerprints and credentials must stay out
// of ordinary logs and out of metric labels, and opaque job IDs may appear in
// logs but never as an unbounded metric label.

// forbiddenMetricLabels lists label names that would leak document identity or
// produce unbounded cardinality if they ever appeared.
var forbiddenMetricLabels = []string{
	"job_id", "jobid", "job", "filename", "file", "name", "source_name",
	"path", "source_path", "destination", "fingerprint", "hash", "checksum",
	"sha256", "content", "password", "secret", "token", "credential", "dsn",
	"uri", "url", "user", "username",
}

// TestF6LogsAreStructuredJSON asserts every ordinary log line from every
// application parses as a JSON object with the expected common fields.
func TestF6LogsAreStructuredJSON(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseTelemetry)

	for _, service := range []string{"watcher", "renamer-1", "renamer-2"} {
		lines := readServiceLog(t, e, service)
		records, nonJSON := parseJSONLog(lines)

		if len(nonJSON) > 0 {
			t.Errorf("%s emitted %d non-JSON line(s); the first is: %s", service, len(nonJSON), nonJSON[0])
		}
		if len(records) == 0 {
			t.Fatalf("%s emitted no parseable JSON records", service)
		}

		for _, r := range records {
			for _, field := range []string{"time", "level", "msg", "application", "instance", "event"} {
				if _, ok := r.Fields[field]; !ok {
					t.Errorf("%s log record is missing the %q field: %s", service, field, r.Raw)
					break
				}
			}
			if _, err := time.Parse(time.RFC3339Nano, r.String("time")); err != nil {
				t.Errorf("%s log record has an unparseable timestamp: %s", service, r.Raw)
			}
			if app := r.String("application"); app != "watcher" && app != "renamer" {
				t.Errorf("%s log record reports application %q", service, app)
			}
		}
		t.Logf("%s: %d structured records, 0 unstructured", service, len(records))
	}
}

// TestF6LogsCarryNoDocumentIdentityOrCredentials scans every collected log
// line for the actual synthetic document names, the configured paths, the
// content fingerprints and the real credentials used by this stack.
func TestF6LogsCarryNoDocumentIdentityOrCredentials(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseTelemetry)

	// Build the forbidden-string set from real values, not from guesses.
	forbidden := map[string]string{}

	// Credentials actually in use.
	forbidden["database password"] = e.Cfg.Database.Password
	forbidden["broker password"] = e.Cfg.Broker.Password
	// A DSN or an AMQP URI would embed a credential.
	forbidden["database DSN"] = e.Cfg.Database.DSN()
	forbidden["broker URI"] = e.Cfg.Broker.URI()

	// The synthetic document names actually present in the incoming root.
	names, err := listFixtureNames(e.Cfg.Storage.Incoming)
	if err != nil {
		t.Fatalf("read the synthetic fixture names: %v", err)
	}
	if len(names) == 0 {
		t.Fatalf("no synthetic fixtures were found under %s; F6 requires real generated documents", e.Cfg.Storage.Incoming)
	}
	for i, n := range names {
		forbidden[fmt.Sprintf("document name %d", i)] = n
	}

	// A fingerprint of one fixture, in both hex and raw form.
	sample := filepath.Join(e.Cfg.Storage.Incoming, "synthetic", names[0])
	if data, rerr := os.ReadFile(sample); rerr == nil {
		sum := sha256.Sum256(data)
		forbidden["content fingerprint"] = hex.EncodeToString(sum[:])
	}

	// Configured absolute storage paths.
	for _, role := range e.Cfg.Storage.RolesFor("watcher") {
		forbidden["storage path "+role.Name] = role.Path
	}

	var scanned int
	for _, service := range []string{"watcher", "renamer-1", "renamer-2"} {
		lines := readServiceLog(t, e, service)
		for _, ln := range lines {
			scanned++
			for label, needle := range forbidden {
				if needle == "" {
					continue
				}
				if strings.Contains(ln, needle) {
					t.Errorf("%s leaked %s into an ordinary log line: %s", service, label, redactForReport(ln, needle))
				}
			}
		}
	}
	t.Logf("scanned %d log lines from 3 applications against %d real forbidden values", scanned, len(forbidden))

	// Job IDs, by contrast, are explicitly allowed in logs. Their presence is
	// asserted so the privacy rule is not satisfied by logging nothing useful.
	jobID := e.LoadState(t, "f4-e2e-job")
	found := false
	for _, service := range []string{"watcher", "renamer-1", "renamer-2"} {
		for _, ln := range readServiceLog(t, e, service) {
			if strings.Contains(ln, jobID) {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("job %s does not appear in any application log; opaque job IDs are supposed to be traceable", jobID)
	}
}

// TestF6MetricLabelsAreBoundedAndCarryNoDocumentIdentity walks every exposed
// series and checks the label names and values.
func TestF6MetricLabelsAreBoundedAndCarryNoDocumentIdentity(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	jobID := e.LoadState(t, "f4-e2e-job")
	names, err := listFixtureNames(e.Cfg.Storage.Incoming)
	if err != nil {
		t.Fatalf("read the synthetic fixture names: %v", err)
	}

	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		body, merr := GetMetrics(url)
		if merr != nil {
			t.Fatalf("%s /metrics: %v", url, merr)
		}

		samples := parseMetrics(body)
		if len(samples) == 0 {
			t.Fatalf("%s exposed no samples", url)
		}

		fnSeries := 0
		for _, s := range samples {
			if !strings.HasPrefix(s.Name, "fn_") {
				continue
			}
			fnSeries++
			for label, value := range s.Labels {
				lower := strings.ToLower(label)
				for _, bad := range forbiddenMetricLabels {
					if lower == bad {
						t.Errorf("%s exposes %s with a forbidden label %q", url, s.Name, label)
					}
				}
				// A job identifier or a document name must never be a label
				// value, whatever the metric.
				if value == jobID {
					t.Errorf("%s exposes a job identifier as the metric label %s{%s}", url, s.Name, label)
				}
				for _, n := range names {
					if strings.Contains(value, n) {
						t.Errorf("%s exposes a document name in the metric label %s{%s=%q}", url, s.Name, label, value)
					}
				}

				// The remaining check is about cardinality: the application's
				// own label values must be short closed-set identifiers.
				// "le" and "quantile" are Prometheus's numeric bucket bounds,
				// and fn_build_info deliberately carries version strings.
				if lower == "le" || lower == "quantile" || s.Name == "fn_build_info" {
					continue
				}
				if !jobs.SafeIdentifier(value) {
					t.Errorf("%s exposes %s{%s=%q} with a value outside the safe identifier set; "+
						"this is how document-derived text would enter a label", url, s.Name, label, value)
				}
			}
		}
		if fnSeries == 0 {
			t.Fatalf("%s exposed no fn_ series", url)
		}

		// Cardinality: each of the application's own labels must stay within
		// its enumerated closed set.
		checkLabelSet(t, url, body, "fn_dependency_up", "dependency",
			[]string{telemetry.DepPostgresPrimary, telemetry.DepPostgresReplica, telemetry.DepRabbitMQ, telemetry.DepStorage})
		checkLabelSet(t, url, body, "fn_jobs", "state", stateStrings())
		checkLabelSet(t, url, body, "fn_storage_root_available", "role",
			[]string{"incoming", "queued", "staging", "consume", "failed"})
		checkLabelSet(t, url, body, "fn_storage_root_status", "status", telemetry.StorageStatuses())
		checkLabelSet(t, url, body, "fn_deliveries_total", "outcome", telemetry.DeliveryOutcomes())

		t.Logf("%s: %d fn_ series, all labels within their closed sets", url, fnSeries)
	}
}

// TestF6RequiredTelemetryIsPresent asserts the observability the requirements
// call for is actually exposed by the running applications.
func TestF6RequiredTelemetryIsPresent(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	watcherRequired := []string{
		"fn_build_info", "fn_ready", "fn_dependency_up",
		"fn_storage_root_available", "fn_storage_root_status",
		"fn_jobs", "fn_oldest_pending_dispatch_age_seconds",
		"fn_accounting_runs_total", "fn_dispatch_attempts_total",
		"fn_dispatch_confirmed_total", "fn_dispatch_reclaimed_total",
		"fn_ledger_errors_total", "fn_broker_reconnects_total",
	}
	renamerRequired := []string{
		"fn_build_info", "fn_ready", "fn_dependency_up",
		"fn_storage_root_available", "fn_deliveries_total",
		"fn_redeliveries_total", "fn_deliveries_in_flight",
		"fn_delivery_duration_seconds_bucket", "fn_consumer_up",
		"fn_broker_reconnects_total",
	}

	assertPresent := func(url string, wanted []string) {
		body, err := GetMetrics(url)
		if err != nil {
			t.Fatalf("%s /metrics: %v", url, err)
		}
		for _, name := range wanted {
			if len(samplesFor(body, name)) == 0 {
				t.Errorf("%s does not expose %s", url, name)
			}
		}
		e.WriteEvidence(t, "f6-metrics-"+sanitizeName(url)+".txt", []byte(body))
	}

	assertPresent(e.WatcherURL, watcherRequired)
	for _, url := range e.RenamerURLs {
		assertPresent(url, renamerRequired)
	}
}

// TestF6LogAllowListRejectsUnapprovedKeys asserts the redaction in the logging
// package is active, by exercising it directly rather than trusting the app.
func TestF6LogAllowListRejectsUnapprovedKeys(t *testing.T) {
	Suite()
	for _, key := range []string{
		"source_name", "filename", "destination_path", "content_fingerprint",
		"password", "dsn", "amqp_uri",
	} {
		if logging.IsSafeKey(key) {
			t.Errorf("the logger allows %q as a log key; document identity and credentials must not be loggable", key)
		}
	}
	for _, key := range []string{"job_id", "event", "category", "error_kind", "state"} {
		if !logging.IsSafeKey(key) {
			t.Errorf("the logger rejects %q, which the applications need in ordinary output", key)
		}
	}
}

// TestF6FixturesAreRealAndIsolated asserts the suite runs against genuinely
// generated synthetic documents in the project's own isolated volumes.
func TestF6FixturesAreRealAndIsolated(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	names, err := listFixtureNames(e.Cfg.Storage.Incoming)
	if err != nil {
		t.Fatalf("read the incoming root: %v", err)
	}
	if len(names) < 5 {
		t.Fatalf("expected the generated synthetic fixture set, found %d files", len(names))
	}

	var bytesTotal int64
	dir := filepath.Join(e.Cfg.Storage.Incoming, "synthetic")
	for _, n := range names {
		info, serr := os.Stat(filepath.Join(dir, n))
		if serr != nil {
			t.Errorf("fixture %q is not readable: %v", n, serr)
			continue
		}
		if !info.Mode().IsRegular() {
			t.Errorf("fixture %q is not a regular file", n)
		}
		if info.Size() == 0 {
			t.Errorf("fixture %q is empty", n)
		}
		bytesTotal += info.Size()
	}

	// Coverage the naming milestone will need: whitespace, mixed case,
	// punctuation, German characters and CJK are all present as real files.
	shapes := map[string]bool{"space": false, "uppercase": false, "german": false, "cjk": false, "temp suffix": false, "hidden": false}
	for _, n := range names {
		if strings.Contains(n, " ") {
			shapes["space"] = true
		}
		if n != strings.ToLower(n) {
			shapes["uppercase"] = true
		}
		if strings.ContainsAny(n, "äöüßÜÄÖ") {
			shapes["german"] = true
		}
		for _, r := range n {
			if r > 0x2FFF {
				shapes["cjk"] = true
			}
		}
		if strings.HasSuffix(strings.ToLower(n), ".part") {
			shapes["temp suffix"] = true
		}
		if strings.HasPrefix(n, ".") {
			shapes["hidden"] = true
		}
	}
	var missing []string
	for shape, present := range shapes {
		if !present {
			missing = append(missing, shape)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("the generated fixture set does not cover: %s", strings.Join(missing, ", "))
	}

	// Isolation: the consume root must be empty. Nothing in this increment
	// publishes, so a file here would mean something fabricated a delivery.
	entries, err := os.ReadDir(e.Cfg.Storage.Consume)
	if err != nil {
		t.Fatalf("read the consume root: %v", err)
	}
	if len(entries) != 0 {
		var got []string
		for _, en := range entries {
			got = append(got, en.Name())
		}
		t.Errorf("the consume root holds %d entries (%v); no code path in this increment publishes a document",
			len(entries), got)
	}

	e.WriteEvidence(t, "f6-fixtures.txt", []byte(fmt.Sprintf(
		"incoming_root=%s files=%d total_bytes=%d consume_entries=%d\n",
		e.Cfg.Storage.Incoming, len(names), bytesTotal, len(entries))))
}

// TestF6LedgerRetainsHistory asserts nothing purges the durable history, which
// is the behaviour the unresolved cleanup policy requires for now.
func TestF6LedgerRetainsHistory(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	jobID := e.LoadState(t, "f4-e2e-job")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	before, err := led.Events(ctx, jobID)
	if err != nil {
		t.Fatalf("read retained history: %v", err)
	}
	if len(before) < 4 {
		t.Fatalf("job %s retained only %d history rows; the delivery history is not being kept", jobID, len(before))
	}

	// Every history row must carry an actor and a sanitized category where one
	// applies, so an outcome can be explained without consulting the document.
	for _, ev := range before {
		if ev.Actor == "" {
			t.Errorf("history row %d has no actor", ev.EventID)
		}
		if ev.Category != nil && !jobs.SafeIdentifier(*ev.Category) {
			t.Errorf("history row %d carries an unsafe category %q", ev.EventID, *ev.Category)
		}
	}

	time.Sleep(3 * time.Second)
	after, err := led.Events(ctx, jobID)
	if err != nil {
		t.Fatalf("re-read retained history: %v", err)
	}
	if len(after) < len(before) {
		t.Errorf("history rows for %s dropped from %d to %d; something is purging the ledger", jobID, len(before), len(after))
	}
}

// --- helpers ---------------------------------------------------------------

func checkLabelSet(t *testing.T, url, body, metric, label string, allowed []string) {
	t.Helper()
	allow := map[string]bool{}
	for _, a := range allowed {
		allow[a] = true
	}
	for _, v := range labelValueSet(body, metric, label) {
		if !allow[v] {
			t.Errorf("%s exposes %s{%s=%q}, which is outside the enumerated set %v", url, metric, label, v, allowed)
		}
	}
}

func stateStrings() []string {
	out := make([]string, 0, len(jobs.States()))
	for _, s := range jobs.States() {
		out = append(out, string(s))
	}
	return out
}

// listFixtureNames reads the generated synthetic document names.
func listFixtureNames(incoming string) ([]string, error) {
	dir := filepath.Join(incoming, "synthetic")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.Type().IsRegular() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// redactForReport keeps a leak report from repeating the leaked value.
func redactForReport(line, needle string) string {
	return strings.ReplaceAll(line, needle, "[LEAKED-VALUE-REDACTED-IN-REPORT]")
}

func sanitizeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}
