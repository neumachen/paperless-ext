package telemetry

import (
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
)

func gather(t *testing.T, m *Metrics) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(families))
	for _, f := range families {
		out[f.GetName()] = f
	}
	return out
}

func TestEveryLabelValueIsASafeIdentifier(t *testing.T) {
	m := New("watcher", "watcher-1", "v1-candidate-test")

	// Exercise the paths the applications actually take, so the assertion
	// covers the labels that really get written.
	m.Deliveries.WithLabelValues("held").Inc()
	m.DispatchAttempts.WithLabelValues("confirmed").Inc()
	m.LedgerErrors.WithLabelValues("connection_refused").Inc()
	m.JobsByState.WithLabelValues(string(jobs.StateHeld)).Set(3)
	m.StorageStatus.WithLabelValues("incoming", "missing").Set(1)

	for name, family := range gather(t, m) {
		if !strings.HasPrefix(name, "fn_") {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				// fn_build_info deliberately carries version strings, which
				// are not identifier-shaped.
				if name == "fn_build_info" {
					continue
				}
				if !jobs.SafeIdentifier(pair.GetValue()) {
					t.Errorf("%s{%s=%q} is not a safe label value", name, pair.GetName(), pair.GetValue())
				}
			}
		}
	}
}

func TestNoMetricCarriesADocumentIdentifyingLabel(t *testing.T) {
	m := New("renamer", "renamer-1", "v1-candidate-test")
	forbidden := map[string]bool{
		"job_id": true, "job": true, "filename": true, "file": true,
		"name": true, "path": true, "source_name": true, "fingerprint": true,
		"hash": true, "content": true, "password": true, "secret": true,
		"user": true, "dsn": true, "uri": true,
	}
	for name, family := range gather(t, m) {
		if !strings.HasPrefix(name, "fn_") {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if forbidden[strings.ToLower(pair.GetName())] {
					t.Errorf("%s exposes the forbidden label %q", name, pair.GetName())
				}
			}
		}
	}
}

func TestClosedLabelSpaceIsPreInitialised(t *testing.T) {
	m := New("watcher", "watcher-1", "v1-candidate-test")
	families := gather(t, m)

	// A scraper must be able to tell "nothing happened yet" from "this series
	// does not exist", so every enumerated series exists at zero.
	wantJobStates := make([]string, 0, len(jobs.States()))
	for _, s := range jobs.States() {
		wantJobStates = append(wantJobStates, string(s))
	}
	sort.Strings(wantJobStates)
	if got := labelValues(families, "fn_jobs", "state"); !equal(got, wantJobStates) {
		t.Errorf("fn_jobs states = %v, want %v", got, wantJobStates)
	}

	wantDeps := dependencies()
	sort.Strings(wantDeps)
	if got := labelValues(families, "fn_dependency_up", "dependency"); !equal(got, wantDeps) {
		t.Errorf("fn_dependency_up dependencies = %v, want %v", got, wantDeps)
	}

	wantOutcomes := DeliveryOutcomes()
	sort.Strings(wantOutcomes)
	if got := labelValues(families, "fn_deliveries_total", "outcome"); !equal(got, wantOutcomes) {
		t.Errorf("fn_deliveries_total outcomes = %v, want %v", got, wantOutcomes)
	}

	wantRoles := storageRoles()
	sort.Strings(wantRoles)
	if got := labelValues(families, "fn_storage_root_available", "role"); !equal(got, wantRoles) {
		t.Errorf("fn_storage_root_available roles = %v, want %v", got, wantRoles)
	}
}

// TestDeliveryOutcomesDistinguishDeliveryFromEverythingElse replaces an
// earlier test that forbade a "delivered" outcome entirely, which was correct
// while nothing could be published. Publication exists now, so the property
// worth protecting is different: exactly one outcome may read as a delivery,
// and the outcomes that are explicitly NOT deliveries must stay distinct from
// it. A held or uncertain job must never be counted as a delivered one.
func TestDeliveryOutcomesDistinguishDeliveryFromEverythingElse(t *testing.T) {
	outcomes := map[string]bool{}
	for _, o := range DeliveryOutcomes() {
		if outcomes[o] {
			t.Errorf("duplicate delivery outcome %q", o)
		}
		outcomes[o] = true
	}
	// The one outcome that means a document reached the consume directory.
	if !outcomes["delivered"] {
		t.Error("no outcome records an actual delivery")
	}
	// Outcomes that must exist and must not be conflated with it.
	for _, o := range []string{"reconciled", "uncertain", "dry_run", "held", "requeued"} {
		if !outcomes[o] {
			t.Errorf("the outcome set is missing %q", o)
		}
	}
	// A dry run must never be able to look like a delivery.
	if outcomes["published"] || outcomes["completed"] {
		t.Error("the outcome set contains a second delivery-sounding label")
	}
}

// TestPublishedCounterIsSeparateFromDeliveryOutcomes: the published counter is
// the only metric that asserts a document reached the consumer.
func TestPublishedCounterIsSeparateFromDeliveryOutcomes(t *testing.T) {
	m := New("renamer", "renamer-1", "v1-candidate-test")
	families := gather(t, m)
	if families["fn_documents_published_total"] == nil {
		t.Error("fn_documents_published_total is not registered")
	}
	if families["fn_documents_published_bytes_total"] == nil {
		t.Error("fn_documents_published_bytes_total is not registered")
	}
	// It starts at zero: a freshly started process has published nothing.
	if v := families["fn_documents_published_total"].GetMetric()[0].GetCounter().GetValue(); v != 0 {
		t.Errorf("a new process reports %v published documents", v)
	}
}

// TestBuildInfoReportsTheRuntimePolicyIdentity: the label used to be the
// build constant "unimplemented". A policy exists now, and two processes built
// from the same source can still run different configured policies, so the
// label carries the runtime identity the process was given.
func TestBuildInfoReportsTheRuntimePolicyIdentity(t *testing.T) {
	m := New("watcher", "watcher-1", "v1-candidate-test")
	families := gather(t, m)
	f := families["fn_build_info"]
	if f == nil {
		t.Fatal("fn_build_info is not registered")
	}
	if len(f.GetMetric()) != 1 {
		t.Fatalf("expected exactly one fn_build_info series, got %d", len(f.GetMetric()))
	}
	labels := map[string]string{}
	for _, pair := range f.GetMetric()[0].GetLabel() {
		labels[pair.GetName()] = pair.GetValue()
	}
	if labels["policy_version"] != "v1-candidate-test" {
		t.Errorf("policy_version = %q, want the identity the process was given", labels["policy_version"])
	}
	if labels["policy_version"] == "unimplemented" {
		t.Error("the build still reports an unimplemented naming policy")
	}
	if labels["contract_version"] != "1" {
		t.Errorf("contract_version = %q", labels["contract_version"])
	}
	if f.GetMetric()[0].GetGauge().GetValue() != 1 {
		t.Error("fn_build_info should be 1")
	}
}

func TestRegistryHasNoDuplicateRegistrations(t *testing.T) {
	// Registering the same collector twice would panic at startup; building
	// two registries proves the constructor is self-contained.
	_ = New("watcher", "a", "v1-candidate-test")
	_ = New("renamer", "b", "v1-candidate-test")

	reg := prometheus.NewRegistry()
	if err := reg.Register(prometheus.NewGauge(prometheus.GaugeOpts{Name: "probe"})); err != nil {
		t.Fatalf("sanity registration failed: %v", err)
	}
}

func labelValues(families map[string]*dto.MetricFamily, metric, label string) []string {
	f := families[metric]
	if f == nil {
		return nil
	}
	seen := map[string]struct{}{}
	for _, m := range f.GetMetric() {
		for _, pair := range m.GetLabel() {
			if pair.GetName() == label {
				seen[pair.GetValue()] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
