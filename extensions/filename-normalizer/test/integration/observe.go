//go:build integration

package integration

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Sample is one parsed Prometheus sample.
type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// parseMetrics reads a Prometheus text exposition into samples. It is a small
// dedicated parser rather than a dependency, because the assertions need the
// raw label sets in order to check cardinality rules.
func parseMetrics(body string) []Sample {
	var out []Sample
	sc := bufio.NewScanner(strings.NewReader(body))
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest := line, ""
		labels := map[string]string{}
		if i := strings.IndexByte(line, '{'); i >= 0 {
			j := strings.LastIndexByte(line, '}')
			if j < i {
				continue
			}
			name = line[:i]
			labels = parseLabels(line[i+1 : j])
			rest = strings.TrimSpace(line[j+1:])
		} else if i := strings.IndexByte(line, ' '); i >= 0 {
			name = line[:i]
			rest = strings.TrimSpace(line[i+1:])
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		out = append(out, Sample{Name: name, Labels: labels, Value: v})
	}
	return out
}

func parseLabels(s string) map[string]string {
	out := map[string]string{}
	var key, val strings.Builder
	inKey, inQuote, escaped := true, false, false
	for _, r := range s {
		switch {
		case escaped:
			val.WriteRune(r)
			escaped = false
		case inQuote && r == '\\':
			escaped = true
		case inQuote && r == '"':
			inQuote = false
			out[strings.TrimSpace(key.String())] = val.String()
			key.Reset()
			val.Reset()
			inKey = true
		case inQuote:
			val.WriteRune(r)
		case r == '=':
			inKey = false
		case r == '"' && !inKey:
			inQuote = true
		case r == ',':
			inKey = true
		default:
			if inKey {
				key.WriteRune(r)
			}
		}
	}
	return out
}

// samplesFor returns every sample of a metric family.
func samplesFor(body, name string) []Sample {
	var out []Sample
	for _, s := range parseMetrics(body) {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

// metricValue returns the single value of a metric matching the given labels.
func metricValue(t *testing.T, body, name string, labels map[string]string) float64 {
	t.Helper()
	var matches []Sample
	for _, s := range samplesFor(body, name) {
		if labelsMatch(s.Labels, labels) {
			matches = append(matches, s)
		}
	}
	switch len(matches) {
	case 0:
		t.Fatalf("metric %s%v is not exposed", name, labels)
		return 0
	case 1:
		return matches[0].Value
	default:
		t.Fatalf("metric %s%v matched %d samples", name, labels, len(matches))
		return 0
	}
}

// requireMetric returns a metric's value and fails if the series is absent.
//
// This is the default for assertions of the form "the value must be 0": a
// helper that substitutes zero for a missing series makes such an assertion
// pass when the metric was never exposed at all, which is the opposite of what
// it is supposed to establish.
func requireMetric(t *testing.T, body, name string, labels map[string]string) float64 {
	t.Helper()
	for _, s := range samplesFor(body, name) {
		if labelsMatch(s.Labels, labels) {
			return s.Value
		}
	}
	t.Fatalf("metric %s%v is not exposed, so nothing can be concluded from its value.\nexposed series for %s:\n%s",
		name, labels, name, describeSamples(body, name))
	return 0
}

// requireMetricErr is requireMetric for use inside a polling closure, where a
// missing series has to become an error rather than an immediate failure.
func requireMetricErr(body, name string, labels map[string]string) (float64, error) {
	for _, s := range samplesFor(body, name) {
		if labelsMatch(s.Labels, labels) {
			return s.Value, nil
		}
	}
	return 0, fmt.Errorf("metric %s%v is not exposed", name, labels)
}

// There is deliberately no "value or zero" helper here. Substituting zero for
// an absent series makes an assertion of the form "this metric must be 0" pass
// when the metric was never exposed at all, which is the opposite of what such
// an assertion is for. Use requireMetric or requireMetricErr.

func labelsMatch(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// postgresLogTime parses the timestamp PostgreSQL prefixes to each log line.
//
// The container's log_timezone is GMT, and log_line_prefix starts with %m, so
// a line begins "2026-09-16 17:13:18.974 GMT ". Continuation lines of a
// multi-line statement have no prefix and report false: they cannot be
// attributed to a fault interval, so an assertion must not count them.
func postgresLogTime(line string) (time.Time, bool) {
	const layout = "2006-01-02 15:04:05.000"
	if len(line) < len(layout) {
		return time.Time{}, false
	}
	ts, err := time.ParseInLocation(layout, line[:len(layout)], time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// LogRecord is one parsed structured log line.
type LogRecord struct {
	Raw    string
	Fields map[string]any
}

// String returns a field as a string, or "".
func (r LogRecord) String(key string) string {
	if v, ok := r.Fields[key].(string); ok {
		return v
	}
	return ""
}

// readServiceLog returns the collected log lines for a service.
//
// The orchestrator writes `docker compose logs` into the shared evidence
// directory before each phase, which keeps this container unprivileged: it
// reads a file rather than holding a Docker socket.
func readServiceLog(t *testing.T, e *Env, service string) []string {
	t.Helper()
	path := filepath.Join(e.Evidence, "logs", service+".log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("collected log for %q is missing at %s: %v (the orchestrator must collect logs before running this phase)", service, path, err)
	}
	var lines []string
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	sc.Buffer(make([]byte, 0, 1<<20), 4<<20)
	for sc.Scan() {
		if ln := strings.TrimSpace(sc.Text()); ln != "" {
			lines = append(lines, ln)
		}
	}
	if len(lines) == 0 {
		t.Fatalf("collected log for %q at %s is empty", service, path)
	}
	return lines
}

// recordsSince returns the structured records a service emitted at or after
// the given instant, plus how many records were discarded as older.
//
// Fault assertions use this so they read only the fault's own interval. The
// collected log holds the whole run, and an error from an earlier phase must
// not be allowed to satisfy a later phase's claim.
func recordsSince(t *testing.T, e *Env, service string, since time.Time) []LogRecord {
	t.Helper()
	all, nonJSON := parseJSONLog(readServiceLog(t, e, service))
	if len(nonJSON) > 0 {
		t.Errorf("%s emitted %d non-JSON line(s); the first is: %s", service, len(nonJSON), nonJSON[0])
	}

	var kept []LogRecord
	older := 0
	unparsed := 0
	for _, r := range all {
		ts, err := time.Parse(time.RFC3339Nano, r.String("time"))
		if err != nil {
			unparsed++
			continue
		}
		if ts.Before(since) {
			older++
			continue
		}
		kept = append(kept, r)
	}
	if unparsed > 0 {
		t.Errorf("%s emitted %d record(s) with an unparseable timestamp", service, unparsed)
	}
	t.Logf("%s: %d record(s) within the fault interval starting %s (%d earlier record(s) excluded)",
		service, len(kept), since.Format(time.RFC3339), older)
	return kept
}

// parseJSONLog parses the JSON records out of collected log lines, returning
// the records and the lines that were not valid JSON objects.
func parseJSONLog(lines []string) (records []LogRecord, nonJSON []string) {
	for _, ln := range lines {
		trimmed := strings.TrimSpace(ln)
		if !strings.HasPrefix(trimmed, "{") {
			nonJSON = append(nonJSON, ln)
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(trimmed), &fields); err != nil {
			nonJSON = append(nonJSON, ln)
			continue
		}
		records = append(records, LogRecord{Raw: trimmed, Fields: fields})
	}
	return records, nonJSON
}

// labelValueSet collects the distinct values a label takes across a family,
// which is how the cardinality assertions detect an unbounded label.
func labelValueSet(body, metric, label string) []string {
	seen := map[string]struct{}{}
	for _, s := range samplesFor(body, metric) {
		if v, ok := s.Labels[label]; ok {
			seen[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func describeSamples(body, metric string) string {
	var b strings.Builder
	for _, s := range samplesFor(body, metric) {
		keys := make([]string, 0, len(s.Labels))
		for k := range s.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s=%q", k, s.Labels[k]))
		}
		fmt.Fprintf(&b, "%s{%s} %v\n", s.Name, strings.Join(parts, ","), s.Value)
	}
	return b.String()
}
