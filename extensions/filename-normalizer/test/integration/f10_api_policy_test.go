//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/grpcapi/gen/filenamenormalizerv1"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/naming"
)

// gRPC, configuration semantics and candidate-policy edge cases, against the
// running services over the real network.

// dialAPI opens a real gRPC connection to a running application.
func dialAPI(t *testing.T, target string) pb.NormalizerClient {
	t.Helper()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", target, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewNormalizerClient(conn)
}

// apiTargets are the gRPC endpoints of the running applications.
func apiTargets(e *Env) []string {
	return []string{"watcher:9090", "renamer-1:9090", "renamer-2:9090"}
}

func apiCtx(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// ---------------------------------------------------------------------------
// Status, state and effective configuration
// ---------------------------------------------------------------------------

func TestAPIStatusFromEveryApplication(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	ctx, cancel := apiCtx(t)
	defer cancel()

	var report strings.Builder
	report.WriteString("gRPC GetStatus, from a real client over the network\n\n")

	policy := ""
	for _, target := range apiTargets(e) {
		resp, err := dialAPI(t, target).GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			t.Fatalf("%s GetStatus: %v", target, err)
		}
		if !resp.GetAlive() {
			t.Errorf("%s reports not alive", target)
		}
		if resp.GetPolicyIdentity() == "" || resp.GetPolicyIdentity() == "unimplemented" {
			t.Errorf("%s reports policy identity %q", target, resp.GetPolicyIdentity())
		}
		if !strings.Contains(resp.GetPolicyStatus(), "candidate") {
			t.Errorf("%s does not disclose that the policy is a candidate: %q", target, resp.GetPolicyStatus())
		}
		// Every application must run the same policy, or documents would be
		// named two different ways depending on which instance got them.
		if policy == "" {
			policy = resp.GetPolicyIdentity()
		} else if policy != resp.GetPolicyIdentity() {
			t.Errorf("%s runs policy %q while another runs %q", target, resp.GetPolicyIdentity(), policy)
		}
		fmt.Fprintf(&report, "%-16s alive=%t ready=%t policy=%s deps=%d\n",
			target, resp.GetAlive(), resp.GetReady(), resp.GetPolicyIdentity(), len(resp.GetDependencies()))
	}
	e.WriteEvidence(t, "grpc-status.txt", []byte(report.String()))
}

func TestAPIProcessingStateCarriesNoDocumentIdentities(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	ctx, cancel := apiCtx(t)
	defer cancel()

	// Publish a document first. Go runs tests in source order within a file
	// and files in name order, so this one can run before any A-test has
	// delivered anything; asserting on another test's side effects would make
	// it pass or fail depending on ordering.
	led := e.Ledger(t)
	name := "apistate-" + e.RunID + ".pdf"
	place(t, e, name, []byte("%PDF-1.4 api state probe\n"))
	if job := awaitJob(t, e, led, name); job.State != jobs.StateDelivered {
		t.Fatalf("the probe document reached %q (%s)", job.State, derefCategory(job))
	}

	resp, err := dialAPI(t, "watcher:9090").GetProcessingState(ctx, &pb.GetProcessingStateRequest{})
	if err != nil {
		t.Fatalf("GetProcessingState: %v", err)
	}
	// The closed state set must be present in full, zeros included.
	for _, st := range jobs.States() {
		if _, ok := resp.GetJobsByState()[string(st)]; !ok {
			t.Errorf("the state map omits %q", st)
		}
	}
	if resp.GetDeliveredTotal() <= 0 {
		t.Errorf("delivered_total is %d; this phase has published documents", resp.GetDeliveredTotal())
	}
	if resp.GetReservationsTotal() <= 0 {
		t.Errorf("reservations_total is %d", resp.GetReservationsTotal())
	}
	if !resp.GetDiscoveryEnabled() {
		t.Error("the watcher reports discovery as disabled")
	}
	if resp.GetDiscoveryLastRunUnix() == 0 {
		t.Error("no discovery run is recorded, so an idle watcher cannot be told from a stopped one")
	}

	// Keys are closed-set identifiers, never document text.
	for k := range resp.GetJobsByState() {
		if !jobs.SafeIdentifier(k) {
			t.Errorf("state key %q is not a safe identifier", k)
		}
	}
	for k := range resp.GetHoldsByCategory() {
		if !jobs.SafeIdentifier(k) {
			t.Errorf("category key %q is not a safe identifier", k)
		}
	}

	var report strings.Builder
	report.WriteString("gRPC GetProcessingState — aggregate only\n\n")
	states := make([]string, 0, len(resp.GetJobsByState()))
	for k := range resp.GetJobsByState() {
		states = append(states, k)
	}
	sort.Strings(states)
	for _, k := range states {
		fmt.Fprintf(&report, "  %-18s %d\n", k, resp.GetJobsByState()[k])
	}
	fmt.Fprintf(&report, "\ndelivered_total=%d reservations_total=%d discovery_enabled=%t last_run=%d\n",
		resp.GetDeliveredTotal(), resp.GetReservationsTotal(),
		resp.GetDiscoveryEnabled(), resp.GetDiscoveryLastRunUnix())
	report.WriteString("\nEvery key is a closed-set identifier. No filename, path or fingerprint\n" +
		"appears in this response.\n")
	e.WriteEvidence(t, "grpc-processing-state.txt", []byte(report.String()))
}

func TestAPIEffectiveConfigHasNoCredentials(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	ctx, cancel := apiCtx(t)
	defer cancel()

	resp, err := dialAPI(t, "renamer-1:9090").GetEffectiveConfig(ctx, &pb.GetEffectiveConfigRequest{})
	if err != nil {
		t.Fatalf("GetEffectiveConfig: %v", err)
	}
	body := resp.GetEffectiveJson()
	if body == "" {
		t.Fatal("the effective configuration is empty")
	}

	// The real credentials must not appear. They are read from the same secret
	// files the applications use, so this is a comparison against the actual
	// values rather than against a pattern.
	for label, secret := range map[string]string{
		"database password": e.Cfg.Database.Password,
		"broker password":   e.Cfg.Broker.Password,
	} {
		if secret == "" {
			t.Fatalf("the suite has no %s to check against", label)
		}
		if strings.Contains(body, secret) {
			t.Errorf("the effective configuration contains the %s", label)
		}
	}
	for _, word := range []string{"password", "secret", "credential"} {
		if strings.Contains(strings.ToLower(body), word) {
			t.Errorf("the effective configuration mentions %q", word)
		}
	}
	if !strings.Contains(body, "candidate") {
		t.Error("the effective configuration does not disclose the policy's candidate status")
	}
	e.WriteEvidence(t, "grpc-effective-config.json", []byte(body))
}

// ---------------------------------------------------------------------------
// Errors and non-mutation
// ---------------------------------------------------------------------------

func TestAPIRejectsInvalidRequestsWithoutEchoingThem(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	ctx, cancel := apiCtx(t)
	defer cancel()
	client := dialAPI(t, "watcher:9090")

	poison := "../../etc/passwd-NOT-A-JOB-ID"
	_, err := client.InspectJob(ctx, &pb.InspectJobRequest{JobId: poison})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("a malformed job id returned %v, expected InvalidArgument", status.Code(err))
	}
	if err != nil && strings.Contains(err.Error(), poison) {
		t.Errorf("the error echoed the caller's malformed input: %v", err)
	}

	_, err = client.InspectJob(ctx, &pb.InspectJobRequest{JobId: "11111111-1111-4111-8111-111111111111"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("an unknown job returned %v, expected NotFound", status.Code(err))
	}

	_, err = client.ValidateConfig(ctx, &pb.ValidateConfigRequest{ConfigJson: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("an empty configuration returned %v, expected InvalidArgument", status.Code(err))
	}

	_, err = client.PreviewName(ctx, &pb.PreviewNameRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("an empty preview returned %v, expected InvalidArgument", status.Code(err))
	}

	e.WriteEvidence(t, "grpc-invalid-requests.txt", []byte(
		"gRPC error behaviour\n\n"+
			"  malformed job id      -> InvalidArgument, input not echoed\n"+
			"  unknown job id        -> NotFound\n"+
			"  empty config document -> InvalidArgument\n"+
			"  empty preview request -> InvalidArgument\n"))
}

// TestAPIPreviewAndValidateMutateNothing is the guarantee that the API is not
// a second processing path.
func TestAPIPreviewAndValidateMutateNothing(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	ctx, cancel := apiCtx(t)
	defer cancel()
	led := e.Ledger(t)
	client := dialAPI(t, "watcher:9090")

	before, err := led.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	beforeRes, _ := led.CountReservations(ctx)
	beforeConsume := countEntries(t, e.Cfg.Storage.Consume)

	// A preview of names that would certainly collide with real documents.
	_, err = client.PreviewName(ctx, &pb.PreviewNameRequest{
		Filenames: []string{
			"Bank Statement - August (Final) 2026.PDF",
			"Medical  --  Statement.pdf",
			strings.Repeat("long", 200) + ".pdf",
		},
	})
	if err != nil {
		t.Fatalf("PreviewName: %v", err)
	}
	_, err = client.ValidateConfig(ctx, &pb.ValidateConfigRequest{
		ConfigJson: `{"version":1,"normalization":{"max_name_bytes":120}}`,
	})
	if err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}

	after, err := led.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	afterRes, _ := led.CountReservations(ctx)
	afterConsume := countEntries(t, e.Cfg.Storage.Consume)

	if before.Total != after.Total {
		t.Errorf("the job count changed across a preview and a validate: %d -> %d", before.Total, after.Total)
	}
	if beforeRes != afterRes {
		t.Errorf("the reservation count changed: %d -> %d", beforeRes, afterRes)
	}
	if beforeConsume != afterConsume {
		t.Errorf("the consume directory changed: %d -> %d entries", beforeConsume, afterConsume)
	}

	e.WriteEvidence(t, "grpc-non-mutation.txt", []byte(fmt.Sprintf(
		"Non-mutation across PreviewName and ValidateConfig\n\n"+
			"                jobs  reservations  consume entries\n"+
			"  before        %-5d %-13d %d\n"+
			"  after         %-5d %-13d %d\n\n"+
			"A preview computes a name; it does not reserve one. The reservation table and\n"+
			"the destination directory are untouched.\n",
		before.Total, beforeRes, beforeConsume, after.Total, afterRes, afterConsume)))
}

func countEntries(t *testing.T, dir string) int {
	t.Helper()
	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	return len(des)
}

// TestAPIPreviewMatchesWhatIsActuallyPublished closes the loop: a preview that
// disagreed with the real pipeline would be worse than no preview.
func TestAPIPreviewMatchesWhatIsActuallyPublished(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	ctx, cancel := apiCtx(t)
	defer cancel()
	led := e.Ledger(t)

	names := []string{
		"Preview Check " + e.RunID + ".PDF",
		"preview--check--" + e.RunID + ".pdf",
		"Überweisung " + e.RunID + ".PDF",
	}
	resp, err := dialAPI(t, "renamer-1:9090").PreviewName(ctx, &pb.PreviewNameRequest{Filenames: names})
	if err != nil {
		t.Fatalf("PreviewName: %v", err)
	}
	predicted := map[string]string{}
	for _, r := range resp.GetResults() {
		if r.GetAccepted() {
			predicted[r.GetOriginal()] = r.GetNormalized()
		}
	}

	var report strings.Builder
	report.WriteString("Preview agrees with publication\n\n")
	for _, name := range names {
		place(t, e, name, []byte("%PDF-1.4 preview agreement\n"))
		job := awaitJob(t, e, led, name)
		if job.State != jobs.StateDelivered {
			t.Errorf("%q reached %q", name, job.State)
			continue
		}
		actual := filepath.Base(publishedPath(t, e, led, job))
		want := predicted[name]
		// The published name equals the preview, or the preview plus a
		// collision suffix when the bare name was already taken.
		base := strings.TrimSuffix(want, filepath.Ext(want))
		if actual != want && !strings.HasPrefix(actual, base+"_") {
			t.Errorf("%q previewed as %q but published as %q", name, want, actual)
		}
		fmt.Fprintf(&report, "  %-46q\n      preview   %s\n      published %s\n", name, want, actual)
	}
	report.WriteString("\nA published name is the previewed name, or the previewed name plus a\n" +
		"collision suffix when the bare one was already reserved. The preview never\n" +
		"promised the suffix, which is why it carries a disclaimer.\n")
	e.WriteEvidence(t, "grpc-preview-agreement.txt", []byte(report.String()))
}

// ---------------------------------------------------------------------------
// Configuration semantics
// ---------------------------------------------------------------------------

// TestAPIValidateDetectsAPolicyChangeThatWouldStrandQueuedWork is the check an
// operator needs before a rolling restart.
func TestAPIValidateDetectsAPolicyChangeThatWouldStrandQueuedWork(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	ctx, cancel := apiCtx(t)
	defer cancel()
	client := dialAPI(t, "watcher:9090")

	running, err := client.GetEffectiveConfig(ctx, &pb.GetEffectiveConfigRequest{})
	if err != nil {
		t.Fatalf("GetEffectiveConfig: %v", err)
	}

	cases := []struct {
		label       string
		doc         string
		wantValid   bool
		wantDiffers bool
	}{
		{"operational change only", `{"version":1,"processing":{"concurrency":4,"prefetch":8}}`, true, false},
		{"name-affecting change", `{"version":1,"normalization":{"max_name_bytes":120}}`, true, true},
		{"a new transform rule", `{"version":1,"normalization":{"rules":[{"name":"drop-scan","pattern":"^scan-","replacement":""}]}}`, true, true},
		{"not idempotent", `{"version":1,"normalization":{"rules":[{"name":"grows","pattern":"^(.+)$","replacement":"x$1"}]}}`, false, false},
		{"unknown field", `{"version":1,"normalisation":{}}`, false, false},
	}

	var report strings.Builder
	report.WriteString("gRPC ValidateConfig\n\n")
	fmt.Fprintf(&report, "running policy: %s\n\n", running.GetPolicyIdentity())

	for _, c := range cases {
		resp, err := client.ValidateConfig(ctx, &pb.ValidateConfigRequest{ConfigJson: c.doc})
		if err != nil {
			t.Fatalf("%s: %v", c.label, err)
		}
		if resp.GetValid() != c.wantValid {
			t.Errorf("%s: valid=%t, want %t (problems: %v)", c.label, resp.GetValid(), c.wantValid, resp.GetProblems())
		}
		if c.wantValid && resp.GetDiffersFromRunning() != c.wantDiffers {
			t.Errorf("%s: differs_from_running=%t, want %t", c.label, resp.GetDiffersFromRunning(), c.wantDiffers)
		}
		if !resp.GetValid() && len(resp.GetProblems()) == 0 {
			t.Errorf("%s: invalid but no problems reported", c.label)
		}
		if !strings.Contains(resp.GetActivation(), "restart") {
			t.Errorf("%s: the activation note does not mention a restart", c.label)
		}
		fmt.Fprintf(&report, "  %-26s valid=%-5t differs=%-5t problems=%d\n",
			c.label, resp.GetValid(), resp.GetDiffersFromRunning(), len(resp.GetProblems()))
		for _, p := range resp.GetProblems() {
			fmt.Fprintf(&report, "        %s\n", p)
		}
	}
	report.WriteString("\nAn operational change does not alter the policy identity, so tuning\n" +
		"concurrency cannot strand queued work. A name-affecting change does, which is\n" +
		"the warning an operator needs before a rolling restart.\n")
	e.WriteEvidence(t, "grpc-validate-config.txt", []byte(report.String()))
}

// TestPolicyMismatchIsHeldRatherThanReinterpreted registers a job stamped with
// a different policy identity and requires the renamer to refuse it.
func TestPolicyMismatchIsHeldRatherThanReinterpreted(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	ctx, cancel := apiCtx(t)
	defer cancel()
	led := e.Ledger(t)

	name := "policy-drift-" + e.RunID + ".pdf"
	place(t, e, name, []byte("%PDF-1.4 accepted under another policy\n"))

	// Register it directly, stamped with a policy this stack is not running.
	// This is exactly what a rolling restart across differing configurations
	// produces: a durable job whose naming rules are not the ones the renamer
	// now has.
	size := int64(41)
	algo := "sha256"
	fingerprint := make([]byte, 32)
	foreignPolicy := naming.PolicyVersion + "+ffffffffffff"
	job, err := led.RegisterJob(ctx, ledger.RegisterInput{
		SourceRoot:      e.Cfg.Storage.Incoming,
		SourceName:      "stale-" + name,
		SizeBytes:       &size,
		FingerprintAlgo: &algo,
		Fingerprint:     fingerprint,
		PolicyIdentity:  foreignPolicy,
	})
	if err != nil {
		t.Fatalf("register a job under a foreign policy: %v", err)
	}
	republishJob(t, e, job.JobID)

	final := awaitJobByID(t, led, job.JobID)
	if final.State != jobs.StateHeld {
		t.Errorf("state %q, expected held", final.State)
	}
	if cat := derefCategory(final); cat != string(jobs.CategoryPolicyMismatch) {
		t.Errorf("category %q, expected %q", cat, jobs.CategoryPolicyMismatch)
	}
	if final.NormalizedName != nil {
		t.Errorf("a job under a foreign policy was given a normalized name %q", *final.NormalizedName)
	}

	e.WriteEvidence(t, "policy-mismatch.txt", []byte(fmt.Sprintf(
		"job accepted under policy: %s\nrenamer is running:        %s\n"+
			"terminal state:            %s\ncategory:                  %s\n"+
			"normalized name assigned:  %t\n\n"+
			"A rolling restart across differing configurations cannot silently reinterpret\n"+
			"queued work: the job is held for an operator instead of being renamed under\n"+
			"rules it was not accepted with.\n",
		foreignPolicy, e.Cfg.Policy.Identity,
		final.State, derefCategory(final), final.NormalizedName != nil)))
}

// ---------------------------------------------------------------------------
// Candidate-policy edge cases, previewed against the running service
// ---------------------------------------------------------------------------

// TestCandidatePolicyEdgeCasesArePreviewedConsistently documents the candidate
// choices by exercising them against the running policy.
func TestCandidatePolicyEdgeCasesArePreviewedConsistently(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	ctx, cancel := apiCtx(t)
	defer cancel()

	names := []string{
		"Überweisung Straße.PDF",
		"請求書 2026.PDF",
		"report.final.PDF",
		"!!!.PDF",
		"archive.tar.gz",
		"noextension",
		"trailingdot.",
		"weird.p df",
		"a – b — c − d.pdf",
		"report ① ½.pdf",
		strings.Repeat("verylongsegment", 40) + ".pdf",
		strings.Repeat("請求書", 100) + ".pdf",
		"  spaces   everywhere  .pdf",
		"___---___.pdf",
	}
	resp, err := dialAPI(t, "renamer-1:9090").PreviewName(ctx, &pb.PreviewNameRequest{Filenames: names})
	if err != nil {
		t.Fatalf("PreviewName: %v", err)
	}
	if len(resp.GetResults()) != len(names) {
		t.Fatalf("%d results for %d names", len(resp.GetResults()), len(names))
	}

	var report strings.Builder
	report.WriteString("Candidate naming policy — edge cases\n\n")
	fmt.Fprintf(&report, "policy: %s\nstatus: candidate for local synthetic testing, not an accepted production policy\n\n",
		resp.GetPolicyIdentity())

	for _, r := range resp.GetResults() {
		if r.GetAccepted() {
			if len(r.GetNormalized()) > naming.DefaultMaxNameBytes {
				t.Errorf("%q previewed as a %d byte name, over the cap", r.GetOriginal(), len(r.GetNormalized()))
			}
			if strings.ContainsAny(r.GetNormalized(), `/\`) {
				t.Errorf("%q previewed with a path separator", r.GetOriginal())
			}
			short := r.GetOriginal()
			if len(short) > 40 {
				short = short[:37] + "..."
			}
			fmt.Fprintf(&report, "  ACCEPT %-42q -> %s", short, r.GetNormalized())
			if r.GetUsedFallback() {
				report.WriteString("   [empty-stem fallback]")
			}
			if r.GetShortened() {
				report.WriteString("   [shortened]")
			}
			report.WriteString("\n")
		} else {
			if r.GetCategory() == "" {
				t.Errorf("%q was refused without a category", r.GetOriginal())
			}
			fmt.Fprintf(&report, "  HOLD   %-42q -> %s\n", r.GetOriginal(), r.GetCategory())
		}
	}
	report.WriteString("\nEvery accepted name is within the byte cap and contains no path separator.\n" +
		"Refusals carry a closed-set category; the policy never guesses a file type.\n")
	e.WriteEvidence(t, "candidate-policy-edge-cases.txt", []byte(report.String()))
}

// TestConfiguredRulesArePreviewedAndCannotBypassInvariants exercises the
// transform-rule surface against the running service.
func TestConfiguredRulesArePreviewedAndCannotBypassInvariants(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	ctx, cancel := apiCtx(t)
	defer cancel()
	client := dialAPI(t, "renamer-1:9090")

	candidate := `{"version":1,"normalization":{"rules":[
		{"name":"strip-scan","pattern":"^scan[_-]?\\d*[_-]","replacement":"","case_insensitive":true},
		{"name":"hostile","pattern":"boom","replacement":"../../etc/PASSWD","all":true}
	]}}`

	resp, err := client.PreviewName(ctx, &pb.PreviewNameRequest{
		Filenames:  []string{"SCAN_0042_Invoice.pdf", "boom.pdf", "untouched.pdf"},
		ConfigJson: candidate,
	})
	if err != nil {
		t.Fatalf("PreviewName with a candidate configuration: %v", err)
	}

	got := map[string]*pb.PreviewResult{}
	for _, r := range resp.GetResults() {
		got[r.GetOriginal()] = r
	}

	if n := got["SCAN_0042_Invoice.pdf"].GetNormalized(); n != "invoice.pdf" {
		t.Errorf("the scanner-prefix rule produced %q, want %q", n, "invoice.pdf")
	}
	// The hostile rule cannot inject traversal or a separator: whatever it
	// emits still goes through the character pipeline.
	hostile := got["boom.pdf"].GetNormalized()
	if strings.ContainsAny(hostile, `/\`) || strings.Contains(hostile, "..") {
		t.Errorf("a rule injected a path into the name: %q", hostile)
	}
	if hostile != "etcpasswd.pdf" {
		t.Errorf("hostile rule produced %q, want %q", hostile, "etcpasswd.pdf")
	}
	if n := got["untouched.pdf"].GetNormalized(); n != "untouched.pdf" {
		t.Errorf("a name no rule matched changed to %q", n)
	}
	if resp.GetPolicyIdentity() == e.Cfg.Policy.Identity {
		t.Error("previewing against a different rule set reported the running policy identity")
	}

	// A rule set that is not idempotent is refused rather than previewed.
	_, err = client.PreviewName(ctx, &pb.PreviewNameRequest{
		Filenames:  []string{"doc.pdf"},
		ConfigJson: `{"version":1,"normalization":{"rules":[{"name":"grows","pattern":"^(.+)$","replacement":"x$1"}]}}`,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("a non-idempotent rule set previewed with %v, expected InvalidArgument", status.Code(err))
	}

	e.WriteEvidence(t, "configured-rules.txt", []byte(fmt.Sprintf(
		"Configured transform rules, previewed against the running service\n\n"+
			"  candidate policy identity: %s\n"+
			"  running policy identity:   %s\n\n"+
			"  SCAN_0042_Invoice.pdf -> %s        (strip-scan)\n"+
			"  boom.pdf              -> %s        (hostile rule, neutralised)\n"+
			"  untouched.pdf         -> %s\n\n"+
			"The hostile rule tried to emit \"../../etc/PASSWD\". Rules run on the stem\n"+
			"BEFORE the character pipeline, so separators and traversal are removed and the\n"+
			"extension is untouched. A non-idempotent rule set is refused outright.\n",
		resp.GetPolicyIdentity(), e.Cfg.Policy.Identity,
		got["SCAN_0042_Invoice.pdf"].GetNormalized(), hostile,
		got["untouched.pdf"].GetNormalized())))
}
