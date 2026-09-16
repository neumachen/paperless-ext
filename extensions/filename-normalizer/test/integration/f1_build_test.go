//go:build integration

package integration

import (
	"encoding/json"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo"
)

// F1 — reproducible container-only development.
//
// The strongest part of F1 is structural and is established by the fact that
// this binary is running at all: it was compiled inside the pinned toolchain
// image during `docker build`, and it executes in a container on the project
// network. What is asserted here is that the artefacts actually deployed carry
// the pinned identity, so a stale or host-built binary cannot pass unnoticed.

// TestF1SuiteRunsFromThePinnedToolchain asserts the suite binary itself was
// built by the toolchain the Dockerfile pins.
func TestF1SuiteRunsFromThePinnedToolchain(t *testing.T) {
	e := Suite()
	got := runtime.Version()
	const want = "go1.26.8"
	if got != want {
		t.Fatalf("suite was compiled with %s but the pinned toolchain is %s; the build did not use the pinned image", got, want)
	}
	e.WriteEvidence(t, "f1-suite-toolchain.txt",
		[]byte("suite toolchain: "+got+"\npinned: "+want+"\n"))
}

// TestF1ApplicationsReportPinnedBuildIdentity reads the running applications'
// own liveness documents and metrics.
func TestF1ApplicationsReportPinnedBuildIdentity(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline, PhasePrimaryRecovered, PhaseRabbitRecovered, PhaseReplicaRecovered)

	targets := append([]string{e.WatcherURL}, e.RenamerURLs...)
	var report strings.Builder

	for _, url := range targets {
		body, status, err := GetHealth(url)
		if err != nil || status != 200 {
			t.Fatalf("%s /healthz: status=%d err=%v", url, status, err)
		}
		var doc struct {
			Status        string `json:"status"`
			Version       string `json:"version"`
			Revision      string `json:"revision"`
			GoVersion     string `json:"go_version"`
			PolicyVersion string `json:"policy_version"`
		}
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatalf("%s /healthz is not JSON: %v", url, err)
		}
		if doc.Status != "alive" {
			t.Errorf("%s reports status %q", url, doc.Status)
		}
		if doc.GoVersion != runtime.Version() {
			t.Errorf("%s was built with %s but the suite with %s; the images are not from one build",
				url, doc.GoVersion, runtime.Version())
		}
		if doc.Version == "" || doc.Version == "0.0.0-dev" {
			t.Errorf("%s reports an unstamped version %q; -ldflags did not apply", url, doc.Version)
		}
		if doc.Revision == "" || doc.Revision == "unknown" {
			t.Logf("%s reports revision %q: the build did not receive a git revision", url, doc.Revision)
		}
		// The naming policy is not implemented in this increment. An artefact
		// claiming a real policy version would be a false claim of capability.
		if doc.PolicyVersion != buildinfo.PolicyVersion {
			t.Errorf("%s reports policy_version %q, expected %q", url, doc.PolicyVersion, buildinfo.PolicyVersion)
		}
		if doc.PolicyVersion != "unimplemented" {
			t.Errorf("%s claims naming policy %q, but no normalization is implemented in this build", url, doc.PolicyVersion)
		}
		report.WriteString(url + " -> " + string(body))
	}
	e.WriteEvidence(t, "f1-application-build-identity.txt", []byte(report.String()))
}

var buildInfoLine = regexp.MustCompile(`fn_build_info\{([^}]*)\}\s+([0-9.]+)`)

// TestF1BuildInfoMetricIsExposed asserts the build identity is also visible to
// a scraper, with a bounded label set.
func TestF1BuildInfoMetricIsExposed(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		body, err := GetMetrics(url)
		if err != nil {
			t.Fatalf("%s /metrics: %v", url, err)
		}
		m := buildInfoLine.FindStringSubmatch(body)
		if m == nil {
			t.Fatalf("%s does not expose fn_build_info", url)
		}
		labels := m[1]
		for _, want := range []string{"application=", "version=", "go_version=", "policy_version=", "contract_version="} {
			if !strings.Contains(labels, want) {
				t.Errorf("%s fn_build_info is missing label %s: %s", url, want, labels)
			}
		}
		if !strings.Contains(labels, `policy_version="unimplemented"`) {
			t.Errorf("%s fn_build_info claims an implemented naming policy: %s", url, labels)
		}
	}
}
