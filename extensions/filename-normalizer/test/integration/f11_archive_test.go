//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/watcher"
)

// Moving delivered originals out of the drop folder, against the real ledger,
// the real renamers and the real filesystem.
//
// Each test owns a private drop folder: a subdirectory of the real incoming
// volume, with its own archive directory inside. The stack's watcher does not
// descend into subdirectories, so nothing here is discovered by it; the tests
// register their own submissions through the real ledger code, the stack's
// dispatcher and renamers deliver them for real, and the archival pass under
// test runs here, in-process, against that folder. The stack's own watcher has
// archival off, so the only thing moving these originals is the code under
// test.

// dropFolder is a private incoming root with an archive directory inside it.
type dropFolder struct {
	root    string
	archive string
}

func newDropFolder(t *testing.T, e *Env, label string) dropFolder {
	t.Helper()
	root := filepath.Join(e.Cfg.Storage.Incoming,
		"archive-"+sanitizeForTemp(e.RunID)+"-"+label+"-"+testNonce(t)[:8])
	// World-readable: the renamers run as another user and must read the
	// sources, exactly as they read the real incoming root.
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("create the private drop folder: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Mkdir(filepath.Join(root, "processed"), 0o755); err != nil {
		t.Fatalf("create the archive directory: %v", err)
	}
	return dropFolder{root: root, archive: filepath.Join(root, "processed")}
}

// drop writes one original into the drop folder and registers it as the
// watcher would, with archival requested.
func (d dropFolder) drop(t *testing.T, e *Env, led *ledger.Ledger, name string, content []byte) (ledger.Job, storage.Entry) {
	t.Helper()
	path := filepath.Join(d.root, name)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write the original: %v", err)
	}
	entry, err := storage.Inspect(d.root, name)
	if err != nil {
		t.Fatalf("inspect the original: %v", err)
	}
	sum, size, err := storage.Fingerprint(path)
	if err != nil {
		t.Fatalf("fingerprint the original: %v", err)
	}
	inode, device := int64(entry.Inode), int64(entry.Device)
	modified := entry.ModTime
	var birth *time.Time
	if entry.BtimeKnown {
		b := entry.Btime.Truncate(time.Microsecond)
		birth = &b
	}
	algo := storage.FingerprintAlgorithm
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	job, err := led.RegisterJob(ctx, ledger.RegisterInput{
		SourceRoot: d.root, SourceName: name,
		SizeBytes: &size, FingerprintAlgo: &algo, Fingerprint: sum,
		PolicyIdentity: e.Cfg.Policy.Identity,
		SourceInode:    &inode, SourceDevice: &device, SourceModifiedAt: &modified,
		SourceBirthTime: birth,
		DestinationRoot: e.Cfg.Storage.Consume,
		ArchiveDir:      "processed",
	})
	if err != nil {
		t.Fatalf("register the original: %v", err)
	}
	return job, entry
}

// archiver builds the archival worker for a private drop folder.
func (d dropFolder) archiver(e *Env, led *ledger.Ledger) *watcher.Archiver {
	cfg := config.WatcherConfig{
		Common: e.Cfg.Common,
		Archive: config.Archive{
			Enabled: true, Directory: "processed", Interval: time.Second, Batch: 50,
		},
	}
	cfg.Storage.Incoming = d.root
	cfg.Storage.ArchiveDir = "processed"
	m := telemetry.New("watcher", "archive-test", e.Cfg.Policy.Identity)
	return watcher.NewArchiver(cfg, led, e.Log, m)
}

func (d dropFolder) pass(t *testing.T, e *Env, led *ledger.Ledger) (int, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return d.archiver(e, led).Pass(ctx)
}

func archivalOf(t *testing.T, led *ledger.Ledger, jobID string) (state, name, category string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	state, name, category, err := led.ArchivalFor(ctx, jobID)
	if err != nil {
		t.Fatalf("read the archival of %s: %v", jobID, err)
	}
	return state, name, category
}

func requireDelivered(t *testing.T, led *ledger.Ledger, job ledger.Job) ledger.Job {
	t.Helper()
	got := awaitJobByID(t, led, job.JobID)
	if got.State != jobs.StateDelivered {
		t.Fatalf("job %s ended %q (%s), expected delivered", job.JobID, got.State, derefCategory(got))
	}
	return got
}

func mustNotExist(t *testing.T, path, what string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("%s is still there (%v)", what, err)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", filepath.Base(path), err)
	}
	return string(b)
}

// TestArchiveMovesADeliveredOriginal is the ordinary day: a document is
// dropped, delivered, and its original moved out of the drop folder -- the
// same file, not a copy, with its bytes intact.
func TestArchiveMovesADeliveredOriginal(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	d := newDropFolder(t, e, "moves")

	content := fmt.Sprintf("%%PDF-1.4 archive moves a delivered original %s\n", e.RunID)
	job, before := d.drop(t, e, led, "Invoice 2026-09.pdf", []byte(content))
	requireDelivered(t, led, job)

	examined, moved := d.pass(t, e, led)
	if examined != 1 || moved != 1 {
		t.Fatalf("pass examined %d and moved %d, want 1 and 1", examined, moved)
	}
	mustNotExist(t, filepath.Join(d.root, "Invoice 2026-09.pdf"), "the original in the drop folder")
	archived := filepath.Join(d.archive, "Invoice 2026-09.pdf")
	after, err := storage.Inspect(d.archive, "Invoice 2026-09.pdf")
	if err != nil {
		t.Fatalf("the original is not in the archive: %v", err)
	}
	if !storage.SameIdentity(before, after) {
		t.Errorf("the archived file is not the original: %+v, was %+v", after, before)
	}
	if got := mustRead(t, archived); got != content {
		t.Errorf("the archived bytes changed")
	}
	state, name, _ := archivalOf(t, led, job.JobID)
	if state != ledger.ArchivalArchived || name != "Invoice 2026-09.pdf" {
		t.Errorf("archival recorded %q under %q, want archived under the original name", state, name)
	}

	events, err := led.Events(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	recorded := false
	for _, ev := range events {
		recorded = recorded || ev.EventType == jobs.EventSourceArchived
	}
	if !recorded {
		t.Error("no source_archived event was written to the job's history")
	}

	// Settled means settled: the next pass has nothing to do.
	if examined, _ := d.pass(t, e, led); examined != 0 {
		t.Errorf("a settled archival was examined again (%d)", examined)
	}

	e.WriteEvidence(t, "archive-moves-a-delivered-original.txt", []byte(fmt.Sprintf(
		"job state:        %s\narchival:         %s\nsame file:        %t (inode %d, birth time known %t)\n",
		jobs.StateDelivered, state, storage.SameIdentity(before, after), before.Inode, before.BtimeKnown)))
}

// TestArchiveKeepsEveryOriginalUnderAReusedName: a scanner writes scan.pdf
// every morning. Both mornings' originals must survive, each traceable to its
// job.
func TestArchiveKeepsEveryOriginalUnderAReusedName(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	d := newDropFolder(t, e, "reused")

	monday := fmt.Sprintf("%%PDF-1.4 monday %s\n", e.RunID)
	first, _ := d.drop(t, e, led, "scan.pdf", []byte(monday))
	requireDelivered(t, led, first)
	if _, moved := d.pass(t, e, led); moved != 1 {
		t.Fatalf("monday's original was not archived")
	}

	tuesday := fmt.Sprintf("%%PDF-1.4 tuesday %s\n", e.RunID)
	second, _ := d.drop(t, e, led, "scan.pdf", []byte(tuesday))
	requireDelivered(t, led, second)
	if _, moved := d.pass(t, e, led); moved != 1 {
		t.Fatalf("tuesday's original was not archived")
	}

	if got := mustRead(t, filepath.Join(d.archive, "scan.pdf")); got != monday {
		t.Errorf("monday's archived original was replaced")
	}
	_, name, _ := archivalOf(t, led, second.JobID)
	short := second.JobID[:strings.IndexByte(second.JobID, '-')]
	if name != "scan."+short+".pdf" {
		t.Errorf("tuesday's original was archived as %q, want it tagged with its job", name)
	}
	if got := mustRead(t, filepath.Join(d.archive, name)); got != tuesday {
		t.Errorf("tuesday's archived original holds the wrong bytes")
	}
	mustNotExist(t, filepath.Join(d.root, "scan.pdf"), "an original in the drop folder")
}

// TestArchiveLeavesAChangedOriginalInTheDropFolder: what sits in the drop
// folder is no longer what reached Paperless, so it must stay visible.
func TestArchiveLeavesAChangedOriginalInTheDropFolder(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	d := newDropFolder(t, e, "changed")

	job, _ := d.drop(t, e, led, "contract.pdf", []byte("%PDF-1.4 as delivered "+e.RunID+"\n"))
	requireDelivered(t, led, job)

	f, err := os.OpenFile(filepath.Join(d.root, "contract.pdf"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("an edit made after delivery\n"); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	if _, moved := d.pass(t, e, led); moved != 0 {
		t.Fatalf("a changed original was moved")
	}
	if _, err := os.Lstat(filepath.Join(d.root, "contract.pdf")); err != nil {
		t.Errorf("the changed original left the drop folder: %v", err)
	}
	mustNotExist(t, filepath.Join(d.archive, "contract.pdf"), "a changed original in the archive")
	state, _, category := archivalOf(t, led, job.JobID)
	if state != ledger.ArchivalRefused || category != string(jobs.CategorySourceMutated) {
		t.Errorf("archival recorded %q/%q, want refused/source_mutated", state, category)
	}
}

// TestArchiveRecordsAnOriginalThatIsGone: somebody tidied the drop folder by
// hand. Nothing is moved and the job says so.
func TestArchiveRecordsAnOriginalThatIsGone(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	d := newDropFolder(t, e, "gone")

	job, _ := d.drop(t, e, led, "receipt.pdf", []byte("%PDF-1.4 removed by hand "+e.RunID+"\n"))
	requireDelivered(t, led, job)
	if err := os.Remove(filepath.Join(d.root, "receipt.pdf")); err != nil {
		t.Fatal(err)
	}

	if _, moved := d.pass(t, e, led); moved != 0 {
		t.Fatalf("a pass reported moving an original that was gone")
	}
	state, _, category := archivalOf(t, led, job.JobID)
	if state != ledger.ArchivalAbsent || category != string(jobs.CategorySourceAbsent) {
		t.Errorf("archival recorded %q/%q, want absent/source_absent", state, category)
	}
}

// TestArchiveRecognisesAMoveItDidNotRecord: a watcher that moved an original
// and died before recording it. The next pass must find it in the archive and
// record it archived, not call it gone.
func TestArchiveRecognisesAMoveItDidNotRecord(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	d := newDropFolder(t, e, "unrecorded")

	job, _ := d.drop(t, e, led, "letter.pdf", []byte("%PDF-1.4 moved before a crash "+e.RunID+"\n"))
	requireDelivered(t, led, job)
	if err := os.Rename(filepath.Join(d.root, "letter.pdf"), filepath.Join(d.archive, "letter.pdf")); err != nil {
		t.Fatal(err)
	}

	d.pass(t, e, led)
	state, name, _ := archivalOf(t, led, job.JobID)
	if state != ledger.ArchivalArchived || name != "letter.pdf" {
		t.Errorf("archival recorded %q under %q, want the earlier move recognised", state, name)
	}
}

// TestArchiveDoesNotTouchAHeldJobsOriginal: a document that did not reach
// Paperless stays where the person resolving it will look.
func TestArchiveDoesNotTouchAHeldJobsOriginal(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	d := newDropFolder(t, e, "held")

	// No extension: the candidate policy refuses to guess a file type.
	job, _ := d.drop(t, e, led, "no-extension-"+sanitizeForTemp(e.RunID), []byte("undeclared type "+e.RunID+"\n"))
	got := awaitJobByID(t, led, job.JobID)
	if got.State != jobs.StateHeld {
		t.Fatalf("job ended %q, expected held for a missing extension", got.State)
	}

	if examined, _ := d.pass(t, e, led); examined != 0 {
		t.Errorf("a held job's original was examined for archival")
	}
	if _, err := os.Lstat(filepath.Join(d.root, job.SourceName)); err != nil {
		t.Errorf("a held job's original left the drop folder: %v", err)
	}
	if state, _, _ := archivalOf(t, led, job.JobID); state != ledger.ArchivalPending {
		t.Errorf("a held job's archival is %q, want it left pending", state)
	}
}
