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
	m := New("watcher", "watcher-1")

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
	m := New("renamer", "renamer-1")
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
	m := New("watcher", "watcher-1")
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

func TestDeliveryOutcomesNeverClaimDelivery(t *testing.T) {
	// This increment publishes nothing, so no outcome may read as a delivery
	// to the consumer directory.
	for _, o := range DeliveryOutcomes() {
		if o == "delivered" || o == "published" || o == "completed" {
			t.Errorf("the outcome set includes %q, which this increment cannot produce", o)
		}
	}
}

func TestBuildInfoReportsAnUnimplementedPolicy(t *testing.T) {
	m := New("watcher", "watcher-1")
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
	if labels["policy_version"] != "unimplemented" {
		t.Errorf("policy_version = %q; no naming policy is implemented in this build", labels["policy_version"])
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
	_ = New("watcher", "a")
	_ = New("renamer", "b")

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
