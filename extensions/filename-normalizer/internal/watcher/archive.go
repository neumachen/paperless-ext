package watcher

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// Archiver moves each delivered original out of the incoming root.
//
// # What it is for
//
// A drop folder that only ever grows is not usable day to day: every document
// ever handled stays next to the ones still waiting, and nobody can tell them
// apart. Once a job is delivered -- its receipt committed and the document in
// the consumer's directory -- its original has done its work, and this moves it
// into an archive directory inside the incoming root. Discovery does not
// descend into directories, so an archived original is never registered again.
//
// # What it will not do
//
// Nothing is deleted, ever. For each delivered job the outcome is one of:
//
//   - the original is moved, under its own name if that is free and with the
//     job id added before the extension if it is not. Nothing is replaced: the
//     move is a no-replace rename, and scanners reuse names every day.
//   - the name no longer holds this job's original -- gone, or given to a
//     different file -- and nothing is moved. A different file at the name is
//     a new submission, and discovery registers it as one.
//   - the original is still there but changed after it was delivered. It is
//     left where it is: what sits in the drop folder is no longer what reached
//     Paperless, and moving it into the archive would hide exactly that.
//
// Originals of jobs that were held or are uncertain are not touched at all.
// They are what a person resolving those jobs starts from.
//
// # Why the watcher
//
// The watcher already owns the incoming root, and there is exactly one of
// it. The renamers keep incoming read-only.
type Archiver struct {
	cfg     config.WatcherConfig
	led     *ledger.Ledger
	log     *slog.Logger
	metrics *telemetry.Metrics

	// Whether the archive directory could be opened last time, so a missing
	// directory is logged when it goes missing rather than on every pass.
	dirKnown, dirOK bool
}

// NewArchiver builds the archival worker.
func NewArchiver(cfg config.WatcherConfig, led *ledger.Ledger, log *slog.Logger, m *telemetry.Metrics) *Archiver {
	return &Archiver{cfg: cfg, led: led, log: log, metrics: m}
}

// Run archives on the configured interval until the context is cancelled.
func (a *Archiver) Run(ctx context.Context) {
	if !a.cfg.Archive.Enabled {
		a.log.Info("source archival is disabled by configuration",
			slog.String("event", "archive_disabled"),
			slog.String("worker", "archive"))
		return
	}
	a.log.Info("source archival started",
		slog.String("event", "archive_started"),
		slog.Float64("interval_seconds", a.cfg.Archive.Interval.Seconds()))

	t := time.NewTicker(a.cfg.Archive.Interval)
	defer t.Stop()
	for {
		a.Pass(ctx)
		select {
		case <-ctx.Done():
			a.log.Info("source archival stopped", slog.String("event", "archive_stopped"))
			return
		case <-t.C:
		}
	}
}

// noteDirectory publishes whether the configured archive directory could be
// opened, and says so in the log when that changes -- not on every pass.
func (a *Archiver) noteDirectory(err error) {
	ok := err == nil
	a.metrics.ArchiveDirAvailable.Set(boolGauge(ok))
	if a.dirKnown && a.dirOK == ok {
		return
	}
	a.dirKnown, a.dirOK = true, ok
	if ok {
		a.log.Info("the archive directory is available",
			slog.String("event", "archive_directory_available"))
		return
	}
	a.log.Error("the archive directory could not be opened; it is never created, so it has to exist inside the incoming root",
		slog.String("event", "archive_directory_unusable"),
		slog.String("dependency", "storage"),
		slog.String("category", storage.RejectionCategory(err)),
		slog.String("error_kind", logging.ErrorKind(err)))
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// Pass deals with every archival that is due and reports how many it examined
// and how many originals it moved.
func (a *Archiver) Pass(ctx context.Context) (examined, moved int) {
	root := a.cfg.Storage.Incoming

	// The same rule discovery applies: a root that is not the directory this
	// process validated is not read from, and certainly not moved out of.
	if err := a.cfg.Roots.Verify(); err != nil {
		a.metrics.ArchiveRuns.WithLabelValues("error").Inc()
		a.log.Error("a storage root is not the directory it was at startup; not archiving",
			slog.String("event", "storage_root_changed"),
			slog.String("dependency", "storage"),
			slog.String("category", string(jobs.CategoryStorageUnavailable)))
		return 0, 0
	}

	src, err := storage.OpenDir(root)
	if err != nil {
		a.metrics.ArchiveRuns.WithLabelValues("error").Inc()
		a.log.Error("the incoming root could not be opened for archival",
			slog.String("event", "archive_failed"),
			slog.String("dependency", "storage"),
			slog.String("category", storage.RejectionCategory(err)))
		return 0, 0
	}
	defer func() { _ = src.Close() }()

	// One descriptor per archive directory, opened from the verified root one
	// component at a time and never created. The configured one is opened on
	// every pass, not only when there is work, so a missing directory shows
	// before the first delivered original is waiting on it.
	dirs := map[string]*storage.Dir{}
	defer func() {
		for _, d := range dirs {
			_ = d.Close()
		}
	}()
	open := func(name string) (*storage.Dir, error) {
		if d, ok := dirs[name]; ok {
			return d, nil
		}
		d, derr := src.OpenSubdir(name)
		if name == a.cfg.Archive.Directory {
			a.noteDirectory(derr)
		}
		if derr != nil {
			return nil, derr
		}
		dirs[name] = d
		return d, nil
	}
	_, _ = open(a.cfg.Archive.Directory)

	due, err := a.led.ArchivalsDue(ctx, root, a.cfg.Archive.Batch)
	if err != nil {
		if ctx.Err() == nil {
			a.metrics.ArchiveRuns.WithLabelValues("error").Inc()
			a.log.Error("could not read the originals due to be archived",
				slog.String("event", "archive_failed"),
				slog.String("dependency", "postgres_primary"),
				slog.String("error_kind", logging.ErrorKind(err)))
		}
		return 0, 0
	}
	defer a.refreshWaiting(ctx, root)

	for _, due := range due {
		if ctx.Err() != nil {
			break
		}
		examined++
		dst, derr := open(due.ArchiveDir)
		if derr != nil {
			a.deferArchival(ctx, due, jobs.Category(storage.RejectionCategory(derr)))
			continue
		}
		if a.archive(ctx, src, dst, due) {
			moved++
		}
	}

	a.metrics.ArchiveRuns.WithLabelValues("ok").Inc()
	if examined > 0 {
		a.log.Info("source archival pass complete",
			slog.String("event", "archive_pass"),
			slog.Int("examined", examined),
			slog.Int("count", moved))
	}
	return examined, moved
}

// archive settles one delivered job's original and reports whether it moved.
func (a *Archiver) archive(ctx context.Context, src, dst *storage.Dir, due ledger.Archival) bool {
	job := due.Job
	log := a.log.With(slog.String("job_id", job.JobID))
	name := job.SourceName
	candidates := archiveCandidates(name, job.JobID)

	// What is at the name now?
	got, err := src.Identify(name)
	switch {
	case errors.Is(err, fs.ErrNotExist), errors.Is(err, storage.ErrNotRegular):
		// Not this job's original any more. A previous pass may have moved it
		// and stopped before recording that; ask the archive before saying the
		// original is gone.
		if at, ok := findArchived(dst, job, candidates); ok {
			return a.settle(ctx, log, job, ledger.ArchivalArchived, candidates[at], "", "already_archived", at)
		}
		return a.settle(ctx, log, job, ledger.ArchivalAbsent, "", jobs.CategorySourceAbsent, "source_absent", 0)
	case err != nil:
		a.deferArchival(ctx, due, jobs.Category(storage.RejectionCategory(err)))
		return false
	}

	if !job.RegisteredFileIs(got) {
		// A different file holds the name: a new submission under a reused
		// name, which discovery registers on its own. This job's original may
		// already be in the archive.
		if at, ok := findArchived(dst, job, candidates); ok {
			return a.settle(ctx, log, job, ledger.ArchivalArchived, candidates[at], "", "already_archived", at)
		}
		return a.settle(ctx, log, job, ledger.ArchivalAbsent, "", jobs.CategorySourceAbsent, "source_absent", 0)
	}
	if !job.RegisteredSourceIs(got) {
		return a.settle(ctx, log, job, ledger.ArchivalRefused, "", jobs.CategorySourceMutated, "source_changed", 0)
	}

	// The bytes must still be the bytes that were delivered. Size and
	// modification time agreeing is a claim a writer can make; the content
	// hash is the fact, and it is read through the descriptor that was
	// checked against the identity above.
	f, _, oerr := src.OpenOwn(name, got)
	if oerr != nil {
		// Changed or gone since it was identified. The next pass looks again
		// rather than deciding on a moving target.
		a.deferArchival(ctx, due, jobs.Category(storage.RejectionCategory(oerr)))
		return false
	}
	sum, size, herr := storage.FingerprintFile(f)
	_ = f.Close()
	if herr != nil {
		a.deferArchival(ctx, due, jobs.Category(storage.RejectionCategory(herr)))
		return false
	}
	if (job.Fingerprint != nil && !bytes.Equal(sum, job.Fingerprint)) ||
		(job.SizeBytes != nil && size != *job.SizeBytes) {
		return a.settle(ctx, log, job, ledger.ArchivalRefused, "", jobs.CategorySourceMutated, "source_changed", 0)
	}

	// Move it, never replacing anything.
	for i, target := range candidates {
		moved, merr := src.MoveOwned(name, got, dst, target)
		switch {
		case moved:
			if merr != nil {
				log.Warn("the original was moved, and flushing the directories afterwards failed",
					slog.String("event", "archive_flush_failed"),
					slog.String("error_kind", logging.ErrorKind(merr)))
			}
			return a.settle(ctx, log, job, ledger.ArchivalArchived, target, "", "archived", i)
		case errors.Is(merr, storage.ErrDestinationExists):
			continue
		case errors.Is(merr, fs.ErrNotExist), errors.Is(merr, storage.ErrMutated):
			// The name was emptied or given to another file between the checks
			// and the move, and nothing of this job's moved. The next pass
			// decides from what is there then.
			log.Info("the original changed hands while it was being archived; looking again next pass",
				slog.String("event", "archive_raced"),
				slog.String("category", storage.RejectionCategory(merr)))
			return false
		default:
			a.deferArchival(ctx, due, jobs.Category(storage.RejectionCategory(merr)))
			return false
		}
	}
	return a.settle(ctx, log, job, ledger.ArchivalRefused, "", jobs.CategoryCollisionExhausted, "collision_exhausted", len(candidates))
}

// settle records an outcome, and counts it only once it is recorded.
//
// The write is detached from the pass's context: a move that happened must be
// written down even when shutdown has begun, because the next watcher would
// otherwise find the original missing from the drop folder and have to work out
// from the archive what this one already knew.
func (a *Archiver) settle(ctx context.Context, log *slog.Logger, job ledger.Job, state, archivedName string, cat jobs.Category, outcome string, seq int) bool {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err := a.led.SettleArchival(wctx, job.JobID, state, archivedName, string(cat))
	switch {
	case errors.Is(err, ledger.ErrArchivalSettled):
		return false
	case err != nil:
		log.Error("could not record what happened to a delivered original",
			slog.String("event", "archive_not_recorded"),
			slog.String("dependency", "postgres_primary"),
			slog.String("outcome", outcome),
			slog.String("error_kind", logging.ErrorKind(err)))
		return false
	}
	a.metrics.ArchiveOutcomes.WithLabelValues(outcome).Inc()

	attrs := []any{
		slog.String("event", "source_archived"),
		slog.String("outcome", outcome),
	}
	if cat != "" {
		attrs = append(attrs, slog.String("category", string(cat)))
	}
	switch state {
	case ledger.ArchivalArchived:
		attrs = append(attrs, slog.Int("collision_sequence", seq))
		log.Info("a delivered original was moved into the archive directory", attrs...)
		return outcome == "archived"
	case ledger.ArchivalRefused:
		log.Warn("a delivered original was left in the drop folder", attrs...)
	default:
		log.Info("a delivered original is no longer in the drop folder; nothing was moved", attrs...)
	}
	return false
}

// deferArchival leaves the archival pending and says when to look again.
func (a *Archiver) deferArchival(ctx context.Context, due ledger.Archival, cat jobs.Category) {
	wait := archiveBackoff(a.cfg.Archive.Interval, due.Attempts)
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := a.led.DeferArchival(wctx, due.Job.JobID, string(cat), wait); err != nil {
		a.log.Warn("could not record a deferred archival",
			slog.String("event", "archive_not_recorded"),
			slog.String("job_id", due.Job.JobID),
			slog.String("error_kind", logging.ErrorKind(err)))
		return
	}
	a.metrics.ArchiveOutcomes.WithLabelValues("deferred").Inc()
	a.log.Warn("a delivered original could not be archived yet; trying again later",
		slog.String("event", "archive_deferred"),
		slog.String("job_id", due.Job.JobID),
		slog.String("category", string(cat)),
		slog.Int("attempts", due.Attempts+1),
		slog.Int64("interval_ms", wait.Milliseconds()))
}

func (a *Archiver) refreshWaiting(ctx context.Context, root string) {
	if n, err := a.led.ArchivalsWaiting(ctx, root); err == nil {
		a.metrics.ArchiveWaiting.Set(float64(n))
	}
}

// archiveBackoff doubles the wait with every failed attempt, up to an hour, so
// a share that is away for a while is not hammered and a transient fault is
// retried promptly.
func archiveBackoff(interval time.Duration, attempts int) time.Duration {
	const ceiling = time.Hour
	wait := interval
	for i := 0; i < attempts && wait < ceiling; i++ {
		wait *= 2
	}
	if wait > ceiling {
		wait = ceiling
	}
	return wait
}

// findArchived looks for this job's original under the names a previous pass
// would have moved it to, and reports which one holds it.
//
// It asks about identity, not content: a file that is this job's original and
// was changed after it was archived is still archived.
func findArchived(dst *storage.Dir, job ledger.Job, candidates []string) (int, bool) {
	for i, c := range candidates {
		e, err := dst.Identify(c)
		if err == nil && job.RegisteredFileIs(e) {
			return i, true
		}
	}
	return 0, false
}

// maxArchiveName is the longest directory entry the filesystems in question
// accept, in bytes.
const maxArchiveName = 255

// archiveCandidates lists the names an original may be archived under, in the
// order they are tried.
//
// The original name comes first, so what a person dropped is what they find.
// Names are reused -- a scanner writes scan.pdf every morning -- so the
// fallbacks carry the job id, which is also how an archived original is traced
// back to its job: the first block of the id, then the whole of it, before the
// extension.
//
//	scan.pdf  ->  scan.pdf, scan.3f2a1b4c.pdf, scan.3f2a1b4c-5d6e-...pdf
func archiveCandidates(name, jobID string) []string {
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	if stem == "" {
		stem, ext = name, ""
	}
	short := jobID
	if i := strings.IndexByte(jobID, '-'); i > 0 {
		short = jobID[:i]
	}
	return []string{name, tagName(stem, ext, short), tagName(stem, ext, jobID)}
}

// tagName puts a tag before the extension, shortening the stem on a character
// boundary when the result would not fit in a directory entry. An extension
// too long to keep beside the tag stays part of the stem instead.
func tagName(stem, ext, tag string) string {
	suffix := "." + tag + ext
	if len(suffix) >= maxArchiveName/2 {
		stem, suffix = stem+ext, "."+tag
	}
	if len(stem)+len(suffix) <= maxArchiveName {
		return stem + suffix
	}
	cut := maxArchiveName - len(suffix)
	for cut > 0 && !utf8.RuneStart(stem[cut]) {
		cut--
	}
	return stem[:cut] + suffix
}
