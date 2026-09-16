//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo"
)

// Which candidate is actually running.
//
// A matching Go version and a shared git revision label do not exclude a stale
// application image: an uncommitted edit leaves the revision unchanged, and a
// build step that silently failed leaves an older image in place. Every
// artifact produced by one image build carries the same content hash of the
// build context, so requiring the applications to report the digest this suite
// binary was stamped with ties the tested processes to the tested source.

// TestF1ApplicationsRunTheSameCandidateAsTheSuite is the provenance gate.
func TestF1ApplicationsRunTheSameCandidateAsTheSuite(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	want := buildinfo.SourceDigest
	if want == "" || want == "unknown" {
		t.Fatalf("this suite binary carries no source digest (%q), so it cannot establish which candidate is running; "+
			"the image build must stamp buildinfo.SourceDigest", want)
	}

	var report strings.Builder
	fmt.Fprintf(&report, "suite source_digest=%s\n", want)

	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		body, status, err := GetHealth(url)
		if err != nil || status != 200 {
			t.Fatalf("%s /healthz: status=%d err=%v", url, status, err)
		}
		var doc struct {
			Instance     string `json:"instance"`
			SourceDigest string `json:"source_digest"`
			Version      string `json:"version"`
			Revision     string `json:"revision"`
		}
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatalf("%s /healthz is not JSON: %v", url, err)
		}
		fmt.Fprintf(&report, "%s instance=%s source_digest=%s version=%s revision=%s\n",
			url, doc.Instance, doc.SourceDigest, doc.Version, doc.Revision)

		if doc.SourceDigest == "" || doc.SourceDigest == "unknown" {
			t.Errorf("%s reports no source digest (%q); it was not built by the pinned image build", url, doc.SourceDigest)
			continue
		}
		if doc.SourceDigest != want {
			t.Errorf("%s runs source digest %q but this suite was built from %q: the application image is stale "+
				"relative to the source under test", url, doc.SourceDigest, want)
		}
	}

	// The same digest must also be visible to a scraper.
	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		body, err := GetMetrics(url)
		if err != nil {
			t.Fatalf("%s /metrics: %v", url, err)
		}
		found := false
		for _, sample := range samplesFor(body, "fn_build_info") {
			if sample.Labels["source_digest"] == want {
				found = true
			}
		}
		if !found {
			t.Errorf("%s does not expose fn_build_info with source_digest=%q:\n%s",
				url, want, describeSamples(body, "fn_build_info"))
		}
	}

	t.Logf("provenance:\n%s", report.String())
	e.WriteEvidence(t, "f1-source-provenance.txt", []byte(report.String()))
}

// TestF1PublicCommandsPropagateTheirExitStatus asserts the exit statuses of
// the public entry points, as executed.
//
// A previous version of this claimed to invoke the healthcheck subcommand but
// actually called the suite's HTTP helper. That proves the endpoint answers;
// it says nothing about the command's exit status, which is what Compose and
// an operator depend on. The commands are therefore executed by the
// orchestrator — the suite's container holds the test binary, not the
// application binaries — and their real statuses are asserted here.
func TestF1PublicCommandsPropagateTheirExitStatus(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseTelemetry)

	// A direct binary invocation is asserted exactly; a make-mediated one is
	// asserted as zero or non-zero. Make reports its own status for a failed
	// recipe — 2, not the recipe's 1 — and the contract here is that the
	// failure propagates at all, not that make adopts the binary's code.
	cases := []struct {
		key      string
		wantZero bool
		exact    string
		what     string
	}{
		{"healthcheck-watcher-running-exit", true, "0",
			"fn-watcher healthcheck against its own running process"},
		{"healthcheck-renamer-1-running-exit", true, "0",
			"fn-renamer healthcheck against its own running process"},
		{"healthcheck-no-listener-exit", false, "1",
			"fn-watcher healthcheck --addr :19999, where nothing listens"},
		{"make-ready-all-up-exit", true, "0",
			"make ready with every application up"},
		{"make-ready-one-down-exit", false, "",
			"make ready with renamer-2 stopped (non-zero; make reports 2 for a failed recipe)"},
	}

	var report strings.Builder
	for _, c := range cases {
		got, ok := e.OptionalState(c.key)
		if !ok {
			t.Errorf("the orchestrator recorded no exit status for %s (state key %q); "+
				"this assertion cannot be satisfied by inference", c.what, c.key)
			continue
		}
		got = strings.TrimSpace(got)
		expectation := "non-zero"
		if c.wantZero {
			expectation = "0"
		} else if c.exact != "" {
			expectation = c.exact
		}
		fmt.Fprintf(&report, "%-72s exit=%-3s (expected %s)\n", c.what, got, expectation)

		switch {
		case c.wantZero && got != "0":
			t.Errorf("%s exited %q, expected 0", c.what, got)
		case !c.wantZero && got == "0":
			t.Errorf("%s exited 0; the failure did not propagate", c.what)
		case !c.wantZero && c.exact != "" && got != c.exact:
			t.Errorf("%s exited %q, expected exactly %q", c.what, got, c.exact)
		}
	}

	// The negative cases are the finding's substance: without them a command
	// that always exits 0 would satisfy the positive cases.
	if down, ok := e.OptionalState("make-ready-one-down-exit"); ok && strings.TrimSpace(down) == "0" {
		t.Errorf("make ready exited 0 with an application stopped, which is exactly the defect: " +
			"it reports the problem and succeeds")
	}

	t.Logf("%s", report.String())
	e.WriteEvidence(t, "f1-public-command-exit-statuses.txt", []byte(report.String()))
}
