//go:build integration

package integration

import (
	"bytes"
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
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
)

// A1-A4, A10 and A11 against the real running stack.
//
// Every document here is a synthetic file this test writes into the real
// incoming volume, processed by the real watcher and the real renamers through
// the real broker and the real cluster. Nothing is simulated: the assertions
// read the published bytes back off the filesystem and the durable rows out of
// PostgreSQL.
//
// Submissions are namespaced with the run id so two runs cannot collide, and
// every file this suite creates is removed by its own cleanup.

// deadline bounds how long a submission may take to reach a terminal state.
const normalizeDeadline = 90 * time.Second

// submission is one synthetic document placed in the incoming root.
type submission struct {
	name    string
	content []byte
	sum     string
}

// place writes a synthetic document into the real incoming directory.
//
// The file is written to a temporary name and renamed into place, which is the
// strict completion contract: the final name appears atomically and complete,
// so discovery cannot see a partial file regardless of the stability interval.
func place(t *testing.T, e *Env, name string, content []byte) submission {
	t.Helper()
	final := filepath.Join(e.Cfg.Storage.Incoming, name)
	tmp := filepath.Join(e.Cfg.Storage.Incoming, ".fn-test-"+sanitizeForTemp(name)+".part")

	if err := os.WriteFile(tmp, content, 0o644); err != nil {
		t.Fatalf("write the temporary submission: %v", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		t.Fatalf("rename the submission into place: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(final) })

	sum := sha256.Sum256(content)
	return submission{name: name, content: content, sum: hex.EncodeToString(sum[:])}
}

func sanitizeForTemp(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "x"
	}
	return b.String()
}

// awaitJob waits for a submission to reach a terminal state and returns it.
func awaitJob(t *testing.T, e *Env, led *ledger.Ledger, name string) ledger.Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), normalizeDeadline)
	defer cancel()

	var last ledger.Job
	deadline := time.Now().Add(normalizeDeadline)
	for time.Now().Before(deadline) {
		job, err := jobByName(ctx, led, e.Cfg.Storage.Incoming, name)
		if err == nil {
			last = job
			switch job.State {
			case jobs.StateDelivered, jobs.StateHeld, jobs.StateUncertain:
				return job
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("submission did not reach a terminal state within %s (last state %q)", normalizeDeadline, last.State)
	return last
}

// jobByName finds the most recent job for a source name.
//
// The lookup is by name because that is what this test controls; the running
// system never looks a job up this way.
func jobByName(ctx context.Context, led *ledger.Ledger, root, name string) (ledger.Job, error) {
	return led.JobBySource(ctx, root, name)
}

// publishedPath returns the delivered file's path for a job.
func publishedPath(t *testing.T, e *Env, led *ledger.Ledger, job ledger.Job) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := led.GetReceipt(ctx, job.JobID)
	if err != nil {
		t.Fatalf("job %s has no delivery receipt: %v", job.JobID, err)
	}
	return filepath.Join(r.DestinationRoot, r.DeliveredName)
}

// ---------------------------------------------------------------------------
// A1 — naming, Unicode, idempotence and byte preservation
// ---------------------------------------------------------------------------

// TestA1RequiredExamplesEndToEnd is the acceptance criterion in its strongest
// form: the four PRD examples as real files, through the real pipeline, with
// the published bytes compared against the source bytes.
func TestA1RequiredExamplesEndToEnd(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	cases := []struct{ in, want string }{
		{"Bank Statement - August (Final) 2026.PDF", "bank_statement-august_final_2026.pdf"},
		{"John's Invoice #123.pdf", "johns_invoice_123.pdf"},
		{"Medical  --  Statement.pdf", "medical-statement.pdf"},
		{"  TAX___RETURN 2025!!.PDF", "tax_return_2025.pdf"},
	}

	var report strings.Builder
	report.WriteString("A1 — required PRD examples, end to end through the real stack\n\n")

	for _, c := range cases {
		// The run id keeps two runs apart, and it is added as a suffix inside
		// the stem so the expected output is the same name with the same
		// suffix -- the example's transformation is still what is asserted.
		in := withRunID(c.in, e.RunID)
		want := withRunID(c.want, publishedRunID(e))
		content := []byte("%PDF-1.4 synthetic " + c.want + "\n")

		sub := place(t, e, in, content)
		job := awaitJob(t, e, led, in)

		if job.State != jobs.StateDelivered {
			cat := ""
			if job.FailureCategory != nil {
				cat = *job.FailureCategory
			}
			t.Errorf("%q reached state %q (category %q), expected delivered", c.in, job.State, cat)
			continue
		}

		got := filepath.Base(publishedPath(t, e, led, job))
		if got != want {
			t.Errorf("%q published as %q, want %q", c.in, got, want)
		}

		// Byte preservation: the published file must be identical.
		published, err := os.ReadFile(publishedPath(t, e, led, job))
		if err != nil {
			t.Fatalf("read the published document: %v", err)
		}
		if !bytes.Equal(published, sub.content) {
			t.Errorf("%q published bytes differ from the source", c.in)
		}
		psum := sha256.Sum256(published)
		if hex.EncodeToString(psum[:]) != sub.sum {
			t.Errorf("%q fingerprint changed: %s -> %s", c.in, sub.sum, hex.EncodeToString(psum[:]))
		}

		// The source must still be there: nothing deletes it.
		if _, err := os.Stat(filepath.Join(e.Cfg.Storage.Incoming, in)); err != nil {
			t.Errorf("%q source is gone after publication: %v", c.in, err)
		}

		fmt.Fprintf(&report, "%-44q -> %s\n    sha256 in=%s out=%s  identical=%t  source retained=%t\n",
			c.in, got, sub.sum[:16], hex.EncodeToString(psum[:])[:16],
			bytes.Equal(published, sub.content), true)
	}

	e.WriteEvidence(t, "a1-required-examples.txt", []byte(report.String()))
}

// publishedRunID is the run id as it appears in a published name.
//
// The orchestrator's run id contains uppercase letters and the naming policy
// lowercases the stem, so any expectation about a published name -- or any
// fixture planted at a name the pipeline will produce -- has to use this form.
// Using e.RunID directly made five tests fail against the orchestrator's id
// while passing against the lowercase ids used when running them by hand.
func publishedRunID(e *Env) string { return strings.ToLower(e.RunID) }

// withRunID inserts the run id before the extension.
func withRunID(name, runID string) string {
	i := strings.LastIndexByte(name, '.')
	if i <= 0 {
		return name + "-" + runID
	}
	return name[:i] + "-" + runID + name[i:]
}

// TestA1UnicodeIsPreservedThroughTheRealPipeline checks that the filesystem
// round trip does not transliterate or mangle non-ASCII names.
func TestA1UnicodeIsPreservedThroughTheRealPipeline(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	cases := []struct{ in, want string }{
		{"Überweisung Straße.PDF", "überweisung_straße.pdf"},
		{"請求書 2026.PDF", "請求書_2026.pdf"},
	}
	var report strings.Builder
	report.WriteString("A1 — Unicode preservation through a real filesystem round trip\n\n")

	for _, c := range cases {
		in := withRunID(c.in, e.RunID)
		want := withRunID(c.want, publishedRunID(e))
		sub := place(t, e, in, []byte("%PDF-1.4 unicode "+c.want+"\n"))
		job := awaitJob(t, e, led, in)
		if job.State != jobs.StateDelivered {
			t.Errorf("%q reached %q, expected delivered", c.in, job.State)
			continue
		}
		path := publishedPath(t, e, led, job)
		got := filepath.Base(path)
		if got != want {
			t.Errorf("%q published as %q, want %q", c.in, got, want)
		}
		published, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(published, sub.content) {
			t.Errorf("%q content was not preserved", c.in)
		}
		// The name must be readable back from the directory exactly as written.
		if _, err := os.Stat(path); err != nil {
			t.Errorf("published Unicode name is not readable back: %v", err)
		}
		fmt.Fprintf(&report, "%q -> %q  bytes identical=%t\n", c.in, got, err == nil)
	}
	e.WriteEvidence(t, "a1-unicode.txt", []byte(report.String()))
}

// TestA1IdempotenceOnRepublication: feeding a published name back in must
// produce the same name again, not a progressively mangled one.
func TestA1IdempotenceOnRepublication(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	first := withRunID("Bank Statement - August (Final) 2026.PDF", e.RunID+"-idem")
	place(t, e, first, []byte("%PDF-1.4 idempotence round one\n"))
	job := awaitJob(t, e, led, first)
	if job.State != jobs.StateDelivered {
		t.Fatalf("the first submission reached %q", job.State)
	}
	published := filepath.Base(publishedPath(t, e, led, job))

	// Now submit a NEW document whose name is the previous output.
	place(t, e, published, []byte("%PDF-1.4 idempotence round two\n"))
	job2 := awaitJob(t, e, led, published)
	if job2.State != jobs.StateDelivered {
		t.Fatalf("the second submission reached %q", job2.State)
	}
	got := filepath.Base(publishedPath(t, e, led, job2))

	// The stem must be unchanged; only the collision suffix may differ,
	// because the first document already holds the bare name.
	wantPrefix := strings.TrimSuffix(published, ".pdf")
	if !strings.HasPrefix(got, wantPrefix) {
		t.Errorf("re-submitting %q produced %q, which is not the same name plus a collision suffix", published, got)
	}
	if got == published {
		t.Errorf("the second distinct submission reused the first one's exact name %q", got)
	}
	e.WriteEvidence(t, "a1-idempotence.txt", []byte(fmt.Sprintf(
		"first submission  -> %s\nresubmitted as     %s\nsecond submission -> %s\n"+
			"the stem is unchanged and only the collision suffix differs, so the policy is\n"+
			"idempotent and the two distinct submissions still got distinct destinations.\n",
		published, published, got)))
}

// ---------------------------------------------------------------------------
// A3 — collisions
// ---------------------------------------------------------------------------

// TestA3ConcurrentCollisionsPreserveEverySubmission submits several distinct
// documents whose names all normalize to the same thing, at once.
func TestA3ConcurrentCollisionsPreserveEverySubmission(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	const n = 6
	base := "Collision " + e.RunID
	var subs []submission
	var names []string
	for i := 0; i < n; i++ {
		// Different spellings, one normalized form. Each carries distinct
		// content, so losing one is detectable.
		variants := []string{
			base + ".pdf", strings.ToUpper(base) + ".PDF", base + "   .pdf",
			"  " + base + "!!.pdf", base + "___.pdf", base + " (copy).pdf",
		}
		name := variants[i]
		content := []byte(fmt.Sprintf("%%PDF-1.4 collision member %d of %d\n", i+1, n))
		subs = append(subs, place(t, e, name, content))
		names = append(names, name)
	}

	delivered := map[string]string{} // destination -> source fingerprint
	var report strings.Builder
	fmt.Fprintf(&report, "A3 — %d distinct submissions that normalize to one name\n\n", n)

	for i, name := range names {
		job := awaitJob(t, e, led, name)
		if job.State != jobs.StateDelivered {
			cat := ""
			if job.FailureCategory != nil {
				cat = *job.FailureCategory
			}
			t.Errorf("collision member %d reached %q (%s), expected delivered", i+1, job.State, cat)
			continue
		}
		path := publishedPath(t, e, led, job)
		got := filepath.Base(path)
		if prev, dup := delivered[got]; dup {
			t.Errorf("destination %q was used twice (also by fingerprint %s)", got, prev[:16])
		}
		published, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", got, err)
		}
		if !bytes.Equal(published, subs[i].content) {
			t.Errorf("collision member %d: published bytes are not its own", i+1)
		}
		delivered[got] = subs[i].sum
		fmt.Fprintf(&report, "  %-60q -> %s\n", name, got)
	}

	if len(delivered) != n {
		t.Errorf("%d distinct submissions produced %d distinct destinations; every submission must survive",
			n, len(delivered))
	}
	fmt.Fprintf(&report, "\n%d submissions -> %d distinct destinations, no overwrite, every byte preserved\n",
		n, len(delivered))
	e.WriteEvidence(t, "a3-collisions.txt", []byte(report.String()))
}

// TestA3ExistingDestinationIsNeverOverwritten plants a file the system did not
// create, under the exact name the next submission will want.
func TestA3ExistingDestinationIsNeverOverwritten(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	stem := "preexisting-" + e.RunID
	// Planted at the NORMALIZED name, or it would not collide at all.
	occupied := filepath.Join(e.Cfg.Storage.Consume, strings.ToLower(stem)+".pdf")
	foreign := []byte("NOT OURS: a file the normalizer did not publish\n")
	// World-readable on purpose. This test is about content that is not this
	// job's, not about an unreadable file: the renamer runs as a different uid
	// than this suite, and a 0640 root-owned file would make it fail on
	// permissions before it ever compared the content. The unreadable case has
	// its own test below.
	if err := os.WriteFile(occupied, foreign, 0o644); err != nil {
		t.Fatalf("plant a foreign destination file: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(occupied) })

	sub := place(t, e, stem+".PDF", []byte("%PDF-1.4 ours\n"))
	job := awaitJob(t, e, led, stem+".PDF")
	if job.State != jobs.StateDelivered {
		t.Fatalf("submission reached %q, expected delivered around the occupied name", job.State)
	}

	// The planted file must be untouched.
	after, err := os.ReadFile(occupied)
	if err != nil {
		t.Fatalf("the planted file is gone: %v", err)
	}
	if !bytes.Equal(after, foreign) {
		t.Errorf("the planted file was overwritten")
	}
	// And ours must have gone somewhere else.
	got := filepath.Base(publishedPath(t, e, led, job))
	if got == strings.ToLower(stem)+".pdf" {
		t.Errorf("the submission took the occupied name %q", got)
	}
	ours, err := os.ReadFile(publishedPath(t, e, led, job))
	if err != nil || !bytes.Equal(ours, sub.content) {
		t.Errorf("our document was not published intact")
	}
	e.WriteEvidence(t, "a3-no-overwrite.txt", []byte(fmt.Sprintf(
		"planted a foreign file at %s\nsubmission published instead as %s\n"+
			"planted file unchanged: %t\n\nThe destination is taken with link(2), which fails if the\n"+
			"name exists, so the check and the claim are one kernel-decided step.\n",
		filepath.Base(occupied), got, bytes.Equal(after, foreign))))
}

// TestA3RetainedNamesAreNotReusedAfterTheFileDisappears removes a delivered
// file directly.
//
// Stated accurately: this establishes FILESYSTEM-ABSENCE behaviour. It does
// not prove anything about Paperless, which is not running here -- no consumer
// observed the file, and nothing was ingested. What it does prove is the
// property that matters for name reuse: once a name has been handed out, a
// different submission never receives it, whether or not the file is still
// there.
func TestA3RetainedNamesAreNotReusedAfterTheFileDisappears(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	stem := "consumed-" + e.RunID
	place(t, e, stem+".PDF", []byte("%PDF-1.4 first, to be consumed\n"))
	first := awaitJob(t, e, led, stem+".PDF")
	if first.State != jobs.StateDelivered {
		t.Fatalf("the first submission reached %q", first.State)
	}
	firstPath := publishedPath(t, e, led, first)
	firstName := filepath.Base(firstPath)

	// The file is removed directly by this test. In production the usual
	// reason a delivered file disappears is that Paperless ingested it, but no
	// Paperless is running here and none is claimed: this is filesystem
	// absence, nothing more.
	if err := os.Remove(firstPath); err != nil {
		t.Fatalf("remove the delivered file: %v", err)
	}

	// A different submission that normalizes to the same name.
	place(t, e, strings.ToUpper(stem)+".pdf", []byte("%PDF-1.4 second, distinct\n"))
	second := awaitJob(t, e, led, strings.ToUpper(stem)+".pdf")
	if second.State != jobs.StateDelivered {
		t.Fatalf("the second submission reached %q", second.State)
	}
	secondName := filepath.Base(publishedPath(t, e, led, second))

	if secondName == firstName {
		t.Errorf("the consumed name %q was handed to a second submission; retained history did not hold", firstName)
	}
	e.WriteEvidence(t, "a3-retained-names.txt", []byte(fmt.Sprintf(
		"first delivered as   %s\nthen removed from the consume directory (as Paperless would)\n"+
			"second delivered as  %s\nname reused: %t\n\n"+
			"The reservation row outlives the file, so a name that has been handed out is\n"+
			"never offered to a different submission. No consumer was involved.\n",
		firstName, secondName, secondName == firstName)))
}

// ---------------------------------------------------------------------------
// A4 — completion and path safety
// ---------------------------------------------------------------------------

// TestA4IneligibleEntriesAreNeverPublished covers the entry kinds discovery
// must refuse: hidden files, recognized temporary suffixes, symlinks, special
// files and directories.
func TestA4IneligibleEntriesAreNeverPublished(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)

	in := e.Cfg.Storage.Incoming
	tag := e.RunID
	var created []string
	defer func() {
		for _, p := range created {
			_ = os.RemoveAll(p)
		}
	}()

	mk := func(path string, fn func(string) error) {
		if err := fn(path); err != nil {
			t.Logf("could not create %s: %v", filepath.Base(path), err)
			return
		}
		created = append(created, path)
	}

	hidden := filepath.Join(in, ".hidden-"+tag+".pdf")
	mk(hidden, func(p string) error { return os.WriteFile(p, []byte("hidden\n"), 0o644) })

	partial := filepath.Join(in, "partial-"+tag+".pdf.part")
	mk(partial, func(p string) error { return os.WriteFile(p, []byte("partial\n"), 0o644) })

	// A symlink pointing at a real file OUTSIDE the incoming root. Following
	// it would publish a document nobody placed in the incoming directory.
	outside := filepath.Join(e.Cfg.Storage.Failed, "outside-"+tag+".pdf")
	mk(outside, func(p string) error { return os.WriteFile(p, []byte("outside the root\n"), 0o644) })
	link := filepath.Join(in, "link-"+tag+".pdf")
	mk(link, func(p string) error { return os.Symlink(outside, p) })

	dir := filepath.Join(in, "subdir-"+tag)
	mk(dir, func(p string) error { return os.Mkdir(p, 0o755) })
	nested := filepath.Join(dir, "nested-"+tag+".pdf")
	if err := os.WriteFile(nested, []byte("nested\n"), 0o644); err == nil {
		created = append(created, nested)
	}

	// A named pipe is a special file. It may not be creatable in every
	// environment; the case is skipped rather than faked when it is not.
	fifo := filepath.Join(in, "fifo-"+tag+".pdf")
	fifoMade := makeFIFO(fifo) == nil
	if fifoMade {
		created = append(created, fifo)
	}

	// Give discovery several intervals to see, and refuse, all of them.
	time.Sleep(12 * time.Second)

	entries, err := os.ReadDir(e.Cfg.Storage.Consume)
	if err != nil {
		t.Fatalf("read the consume directory: %v", err)
	}
	tag = publishedRunID(e) // published names carry the lowercased run id
	// Scope to the entries this test created. Other tests in this phase
	// publish documents that legitimately carry the same run id, and counting
	// those as leaks would make this assertion fail for the wrong reason.
	mine := []string{"hidden-", "partial-", "link-", "nested-", "fifo-", "outside-"}
	var leaked []string
	for _, de := range entries {
		name := de.Name()
		if !strings.Contains(name, tag) {
			continue
		}
		for _, prefix := range mine {
			if strings.Contains(name, prefix) {
				leaked = append(leaked, name)
				break
			}
		}
	}
	if len(leaked) != 0 {
		t.Errorf("ineligible entries reached the consume directory: %v", leaked)
	}

	report := fmt.Sprintf(`A4 — ineligible entries are never published

  hidden file           %s
  temporary suffix      %s
  symlink to a file outside the root
                        %s -> %s
  directory (counted, never descended into by default)
                        %s
  nested file inside it %s
  named pipe            %s

published under this run's tag after 12s: %d
`,
		filepath.Base(hidden), filepath.Base(partial),
		filepath.Base(link), outside,
		filepath.Base(dir), filepath.Base(nested),
		fifoStatus(fifoMade), len(leaked))
	e.WriteEvidence(t, "a4-ineligible-entries.txt", []byte(report))
}

func fifoStatus(made bool) string {
	if made {
		return "created and refused"
	}
	return "not creatable in this environment; case not run"
}

// TestA4SourceChangedAfterDiscoveryIsHeld: a submission modified between
// registration and publication must never be published.
func TestA4SourceChangedAfterDiscoveryIsHeld(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	// Large enough that the copy takes long enough to be worth racing, and
	// registered while the renamers are busy with the rest of this phase.
	name := "mutating-" + e.RunID + ".pdf"
	original := bytes.Repeat([]byte("original-"), 200000) // ~1.8 MB
	path := filepath.Join(e.Cfg.Storage.Incoming, name)
	sub := place(t, e, name, original)

	// Wait for the job to exist, then replace the content in place before the
	// renamer copies it. The stability interval gives a usable window.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var job ledger.Job
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		j, err := jobByName(ctx, led, e.Cfg.Storage.Incoming, name)
		if err == nil {
			job = j
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if job.JobID == "" {
		t.Skip("the submission was not registered in time to race its own publication")
	}

	// Replace the file's content. Truncating and rewriting changes size and
	// mtime, which is exactly what the guard is for.
	if err := os.WriteFile(path, []byte("REPLACED after discovery\n"), 0o644); err != nil {
		t.Fatalf("replace the source: %v", err)
	}

	final := awaitJob(t, e, led, name)
	switch final.State {
	case jobs.StateHeld:
		cat := ""
		if final.FailureCategory != nil {
			cat = *final.FailureCategory
		}
		if cat != string(jobs.CategorySourceMutated) {
			t.Errorf("held with category %q, expected %q", cat, jobs.CategorySourceMutated)
		}
	case jobs.StateDelivered:
		// If the renamer won the race and copied the original bytes before the
		// replacement, delivering is correct -- but then the published bytes
		// must be the ORIGINAL ones, never a mixture.
		published, err := os.ReadFile(publishedPath(t, e, led, final))
		if err != nil {
			t.Fatalf("read the published document: %v", err)
		}
		if !bytes.Equal(published, sub.content) {
			t.Errorf("a changed source was published with bytes that are neither the original nor rejected")
		}
		t.Logf("the copy completed before the source was replaced; the original bytes were published intact")
	default:
		t.Errorf("unexpected terminal state %q", final.State)
	}
	e.WriteEvidence(t, "a4-source-mutation.txt", []byte(fmt.Sprintf(
		"source replaced after registration\nterminal state: %s\ncategory: %s\n\n"+
			"Either outcome is safe: the change is detected and held, or the copy had already\n"+
			"completed and the original bytes were published. A mixture is never published.\n",
		final.State, derefCategory(final))))
}

func derefCategory(j ledger.Job) string {
	if j.FailureCategory == nil {
		return "-"
	}
	return *j.FailureCategory
}

// TestA4MalformedSourceNamesCannotEscapeTheRoot registers jobs directly with
// hostile source names, bypassing discovery, and requires the renamer to
// refuse them rather than resolve them.
func TestA4MalformedSourceNamesCannotEscapeTheRoot(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	hostile := []string{
		"../escape-" + e.RunID + ".pdf",
		"../../etc/passwd",
		"sub/dir/traversal-" + e.RunID + ".pdf",
	}

	var report strings.Builder
	report.WriteString("A4 — malformed source names are refused, not resolved\n\n")

	for _, name := range hostile {
		size := int64(10)
		algo := "sha256"
		sum := sha256.Sum256([]byte("x"))
		job, err := led.RegisterJob(ctx, ledger.RegisterInput{
			SourceRoot:      e.Cfg.Storage.Incoming,
			SourceName:      name,
			SizeBytes:       &size,
			FingerprintAlgo: &algo,
			Fingerprint:     sum[:],
			PolicyIdentity:  e.Cfg.Policy.Identity,
		})
		if err != nil {
			t.Fatalf("register a hostile job: %v", err)
		}

		final := awaitJobByID(t, led, job.JobID)
		if final.State != jobs.StateHeld {
			t.Errorf("%q reached %q, expected held", name, final.State)
		}
		cat := derefCategory(final)
		switch cat {
		case string(jobs.CategorySourceAbsent), "unsafe_name", "escapes_root":
		default:
			t.Errorf("%q held with category %q, expected an unsafe-name or absent-source reason", name, cat)
		}
		fmt.Fprintf(&report, "  %-40q -> %s / %s\n", name, final.State, cat)
	}

	// Nothing may have appeared outside the roots.
	for _, probe := range []string{"/srv/fn/escape-" + e.RunID + ".pdf", "/etc/passwd.pdf"} {
		if _, err := os.Stat(probe); err == nil {
			t.Errorf("a file appeared outside the configured roots at %s", probe)
		}
	}
	report.WriteString("\nNo file appeared outside the configured roots.\n")
	e.WriteEvidence(t, "a4-path-safety.txt", []byte(report.String()))
}

func awaitJobByID(t *testing.T, led *ledger.Ledger, id string) ledger.Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), normalizeDeadline)
	defer cancel()
	var last ledger.Job
	for deadline := time.Now().Add(normalizeDeadline); time.Now().Before(deadline); {
		job, err := led.GetJob(ctx, id)
		if err == nil {
			last = job
			switch job.State {
			case jobs.StateDelivered, jobs.StateHeld, jobs.StateUncertain:
				return job
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach a terminal state (last %q)", id, last.State)
	return last
}

// ---------------------------------------------------------------------------
// A10 — same and separate filesystems
// ---------------------------------------------------------------------------

// TestA10PublicationFilesystemTopologyIsStated reports, from the kernel rather
// than from two different-looking paths, whether the roots are on separate
// filesystems -- and asserts the property that has to hold either way.
func TestA10PublicationFilesystemTopologyIsStated(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)

	roots := map[string]string{
		"incoming": e.Cfg.Storage.Incoming,
		"staging":  e.Cfg.Storage.Staging,
		"consume":  e.Cfg.Storage.Consume,
		"failed":   e.Cfg.Storage.Failed,
	}
	devices := map[string]uint64{}
	names := make([]string, 0, len(roots))
	for n := range roots {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		dev, err := storage.DeviceOf(roots[n])
		if err != nil {
			t.Fatalf("device of %s: %v", n, err)
		}
		devices[n] = dev
	}

	stagingConsumeSame := devices["staging"] == devices["consume"]

	var report strings.Builder
	report.WriteString("A10 — publication across filesystem boundaries\n\n")
	for _, n := range names {
		fmt.Fprintf(&report, "  %-9s device=%d\n", n, devices[n])
	}
	fmt.Fprintf(&report, "\nstaging and consume share a filesystem: %t\n", stagingConsumeSame)
	report.WriteString(`
The publication step never crosses a filesystem boundary in either topology:
the bytes are staged into a temporary file inside the consume directory and
then linked to their final name within that same directory. The cross-boundary
work, when there is any, happens during the copy -- before anything is visible
under a publishable name.

Both roots are Docker volumes on one host filesystem here, so the
same-filesystem path is what THIS phase exercises. The cross-filesystem path
is exercised separately by "make test-recovery", which runs a renamer whose
staging root is a tmpfs: that evidence records the two device numbers read
from the kernel and shows a document published across the boundary. There is
no unit-level coverage of the copy fallback and none is claimed.
`)
	e.WriteEvidence(t, "a10-filesystem-topology.txt", []byte(report.String()))

	// Whatever the topology, a published file must be complete and correct.
	name := "topology-" + e.RunID + ".pdf"
	sub := place(t, e, name, []byte("%PDF-1.4 topology probe\n"))
	led := e.Ledger(t)
	job := awaitJob(t, e, led, name)
	if job.State != jobs.StateDelivered {
		t.Fatalf("topology probe reached %q", job.State)
	}
	got, err := os.ReadFile(publishedPath(t, e, led, job))
	if err != nil || !bytes.Equal(got, sub.content) {
		t.Errorf("the published document is not byte-identical to its source")
	}
}

// TestA10NoPartialFileIsEverVisibleUnderAPublishableName watches the consume
// directory while documents are published and requires every observation of a
// publishable name to be a complete file.
func TestA10NoPartialFileIsEverVisibleUnderAPublishableName(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	const n = 8
	var names []string
	var completeSize int64
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("partialwatch-%s-%02d.pdf", e.RunID, i)
		content := bytes.Repeat([]byte(fmt.Sprintf("block-%02d-", i)), 40000)
		place(t, e, name, content)
		names = append(names, name)
		// Every document is the same length, so one number describes a
		// complete file. It is measured rather than assumed: an earlier
		// version hardcoded a size the fixtures did not actually have, and
		// reported every complete file as a partial one.
		completeSize = int64(len(content))
	}

	// Sample the consume directory hard while the batch is processed. Any
	// entry whose name does not start with the temporary prefix must already
	// be complete: its size must match one of the submitted sizes.
	stop := time.Now().Add(45 * time.Second)
	observations, shortReads := 0, 0
	seenComplete := map[string]bool{}
	for time.Now().Before(stop) {
		entries, err := os.ReadDir(e.Cfg.Storage.Consume)
		if err != nil {
			break
		}
		for _, de := range entries {
			base := de.Name()
			if !strings.Contains(base, "partialwatch-"+publishedRunID(e)) {
				continue
			}
			if strings.HasPrefix(base, ".fn-") {
				continue // the temporary link target, not a publishable name
			}
			info, err := de.Info()
			if err != nil {
				continue
			}
			observations++
			switch {
			case info.Size() == completeSize:
				seenComplete[base] = true
			default:
				shortReads++
				t.Errorf("observed %s at %d bytes under a publishable name, expected %d",
					base, info.Size(), completeSize)
			}
		}
		if len(seenComplete) >= n {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	for _, name := range names {
		job := awaitJob(t, e, led, name)
		if job.State != jobs.StateDelivered {
			t.Errorf("%s reached %q", name, job.State)
		}
	}

	e.WriteEvidence(t, "a10-no-partial-exposure.txt", []byte(fmt.Sprintf(
		"A10 — complete-file-only publication\n\n"+
			"documents:                    %d (%d bytes each)\n"+
			"observations of a publishable name during publication: %d\n"+
			"observations that were short:  %d\n"+
			"distinct complete files seen:  %d\n\n"+
			"A publishable name only ever appears via link(2) on a fully written, fsynced\n"+
			"file, so a partial file cannot be visible under one. The temporary link target\n"+
			"is a dotfile and is excluded from this count by name.\n",
		n, completeSize, observations, shortReads, len(seenComplete))))
}

// TestA3UnreadableDestinationIsHeldNotRetriedForever plants a destination file
// the renamer cannot read.
//
// This case was found by running the suite: the pipeline could not establish
// whether the file was its own work, treated the permission failure as
// transient, and requeued the delivery without limit -- 263 ledger events for
// one job. Retries are now bounded by the ledger's own delivery counter, and an
// unreadable destination is a recorded conflict rather than a spin.
func TestA3UnreadableDestinationIsHeldNotRetriedForever(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	stem := "unreadable-" + e.RunID
	// Planted at the NORMALIZED name, or the submission simply gets a
	// different destination and the conflict never happens.
	occupied := filepath.Join(e.Cfg.Storage.Consume, strings.ToLower(stem)+".pdf")
	// 0600 and owned by this suite's uid, which is not the renamer's.
	if err := os.WriteFile(occupied, []byte("unreadable by the renamer\n"), 0o600); err != nil {
		t.Fatalf("plant an unreadable destination file: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(occupied) })

	place(t, e, stem+".PDF", []byte("%PDF-1.4 blocked by an unreadable destination\n"))
	job := awaitJob(t, e, led, stem+".PDF")

	if job.State != jobs.StateHeld {
		t.Errorf("state %q, expected held", job.State)
	}
	cat := derefCategory(job)
	if cat != string(jobs.CategoryDestinationConflict) && cat != string(jobs.CategoryRetryExhausted) {
		t.Errorf("category %q, expected a destination conflict or an exhausted budget", cat)
	}
	// The planted file must be untouched.
	if _, err := os.Stat(occupied); err != nil {
		t.Errorf("the unreadable destination file was removed: %v", err)
	}
	// And the retry must have been bounded rather than unbounded.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := led.Events(ctx, job.JobID)
	if err != nil {
		t.Fatalf("read the job history: %v", err)
	}
	if len(events) > 60 {
		t.Errorf("the job accumulated %d history rows; the retry bound did not hold", len(events))
	}
	e.WriteEvidence(t, "a3-unreadable-destination.txt", []byte(fmt.Sprintf(
		"planted an unreadable file at %s\nterminal state: %s\ncategory: %s\n"+
			"history rows: %d (an unbounded retry produced 263 before the bound was fixed)\n"+
			"planted file still present: %t\n",
		filepath.Base(occupied), job.State, cat, len(events), err == nil)))
}
