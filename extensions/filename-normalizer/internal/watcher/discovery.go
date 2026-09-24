package watcher

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/app"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
)

// Discoverer registers eligible completed submissions from the incoming root.
//
// # The completion contract
//
// A file appearing in a directory is not evidence that the producer finished
// writing it. Two contracts are supported, and neither is described as proof:
//
//   - "stability" accepts a submission whose size and modification time have
//     held still for the configured interval, measured by this process
//     watching it -- not inferred from the modification time alone. This is a
//     heuristic. A slow or stalled upload can satisfy it, which is why the
//     renamer verifies the content fingerprint again before publishing and
//     refuses a source that changed in between.
//   - "rename" additionally requires that the producer wrote a temporary name
//     and renamed the finished file into place. The rename is the completion
//     signal; files still carrying a recognized temporary suffix are never
//     eligible. The stability gate still applies, so a producer that does not
//     cooperate cannot bypass it.
//
// # Restart recovery
//
// Eligibility is derived from the filesystem and the ledger, never from
// in-process memory alone. A submission that arrived while the watcher was
// down is therefore picked up after it restarts: the ledger has no row for its
// identity, and once it has been watched holding still for the stability
// interval it is registered like any other. That is what makes reconciliation
// after an outage work without a separate catch-up path; it costs one stability
// interval after a restart, which is the price of not trusting the
// modification time alone.
//
// # Distinct submissions stay distinct
//
// Identity is the source root, the name, and the inode and device together.
// A producer that reuses a filename for new content produces a new inode and
// therefore a new job. This is not deduplication: two submissions with
// identical bytes still get separate jobs and separate destinations.
type Discoverer struct {
	base *app.Base
	cfg  config.WatcherConfig
	log  *slog.Logger

	// seen remembers how each path last looked and since when it has looked
	// exactly like that. It is lost on restart, which only delays a
	// registration by one stability interval: a file is never registered on
	// the strength of a modification time this process did not watch hold.
	seen map[string]observation
	// clock is time.Now outside tests.
	clock func() time.Time
}

// observation is what discovery last saw of an entry, and when it first saw
// the entry look exactly like that.
type observation struct {
	entry storage.Entry
	since time.Time
}

// NewDiscoverer builds the discovery worker.
func NewDiscoverer(base *app.Base, cfg config.WatcherConfig) *Discoverer {
	return &Discoverer{
		base:  base,
		cfg:   cfg,
		log:   base.Log.With(slog.String("component", "discovery")),
		seen:  map[string]observation{},
		clock: time.Now,
	}
}

// Run scans until the context is cancelled.
func (d *Discoverer) Run(ctx context.Context) {
	if !d.cfg.Discovery.Enabled {
		d.log.Info("discovery is disabled by configuration",
			slog.String("event", "discovery_disabled"),
			slog.String("worker", "discovery"))
		return
	}

	d.log.Info("discovery started",
		slog.String("event", "discovery_started"),
		slog.String("completion_contract", string(d.cfg.Discovery.Completion)),
		slog.Float64("interval_seconds", d.cfg.Discovery.Interval.Seconds()),
		slog.Float64("stability_seconds", d.cfg.Discovery.StabilityInterval.Seconds()),
		slog.Bool("recursive", d.cfg.Discovery.Recursive))

	// The first scan runs immediately when reconciliation is enabled, so work
	// that arrived during an outage is not delayed by a whole interval.
	if d.cfg.ReconcileOnStart {
		d.scan(ctx, true)
	}

	ticker := time.NewTicker(d.cfg.Discovery.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			d.log.Info("discovery stopped", slog.String("event", "discovery_stopped"))
			return
		case <-ticker.C:
			d.scan(ctx, false)
		}
	}
}

// scan examines the incoming root once.
func (d *Discoverer) scan(ctx context.Context, reconciling bool) {
	root := d.cfg.Storage.Incoming

	// The roots must still be the directories this process validated. A
	// remount between startup and now would otherwise go unnoticed until a
	// document had already been read from, or written to, the wrong place.
	if err := d.cfg.Roots.Verify(); err != nil {
		d.base.Metrics.DiscoveryRuns.WithLabelValues("error").Inc()
		d.base.Metrics.Discovered.WithLabelValues("storage_error").Inc()
		d.log.Error("a storage root is not the directory it was at startup; not scanning",
			slog.String("event", "storage_root_changed"),
			slog.String("dependency", "storage"),
			slog.String("category", "storage_unavailable"))
		return
	}

	// An unreadable or unmounted root is reported as unavailable storage. It
	// must never be read as "the directory is empty", because that is
	// indistinguishable from "there is no work" and would hide an outage.
	entries, err := d.readRoot(root)
	if err != nil {
		d.base.Metrics.DiscoveryRuns.WithLabelValues("error").Inc()
		d.base.Metrics.Discovered.WithLabelValues(storage.RejectionCategory(err)).Inc()
		d.log.Error("the incoming root could not be read; this is not an empty directory",
			slog.String("event", "discovery_failed"),
			slog.String("dependency", "storage"),
			slog.String("category", string(storageCategory(err))),
			slog.String("error_kind", logging.ErrorKind(err)))
		return
	}

	registered, examined := 0, 0
	for _, name := range entries {
		if ctx.Err() != nil {
			return
		}
		if registered >= d.cfg.Discovery.Batch {
			break
		}
		examined++
		if d.consider(ctx, root, name) {
			registered++
		}
	}

	d.base.Metrics.DiscoveryRuns.WithLabelValues("ok").Inc()
	d.base.Metrics.DiscoveryLastRun.SetToCurrentTime()
	if registered > 0 || reconciling {
		d.log.Info("discovery scan complete",
			slog.String("event", "discovery_scan"),
			slog.Bool("reconciling", reconciling),
			slog.Int("examined", examined),
			slog.Int("registered", registered))
	}
}

// readRoot lists candidate names, non-recursively by default.
//
// Directories are counted and never descended into unless recursion is
// configured, and their contents are never silently imported.
func (d *Discoverer) readRoot(root string) ([]string, error) {
	if !d.cfg.Discovery.Recursive {
		des, err := os.ReadDir(root)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(des))
		for _, de := range des {
			if de.IsDir() {
				d.base.Metrics.Discovered.WithLabelValues("directory").Inc()
				continue
			}
			names = append(names, de.Name())
		}
		sort.Strings(names)
		return names, nil
	}

	var names []string
	err := filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() {
			if path != root {
				d.base.Metrics.Discovered.WithLabelValues("directory").Inc()
			}
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		names = append(names, rel)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// consider evaluates one candidate and reports whether it was registered.
func (d *Discoverer) consider(ctx context.Context, root, name string) bool {
	// Recursive mode yields relative paths; only the base name is matched and
	// normalized, and SafeJoin still refuses anything that leaves the root.
	base := filepath.Base(name)

	if ok, reason := d.cfg.Discovery.Matcher.Selects(base); !ok {
		d.base.Metrics.Discovered.WithLabelValues(reason).Inc()
		return false
	}

	// In recursive mode the name is a relative subpath, which the non-recursive
	// join deliberately refuses. Using the wrong one made every nested file
	// rejected as an unsafe name while the option reported itself as enabled.
	entry, err := storage.InspectRel(root, name)
	if !d.cfg.Discovery.Recursive {
		entry, err = storage.Inspect(root, name)
	}
	if err != nil {
		reason := storage.RejectionCategory(err)
		d.base.Metrics.Discovered.WithLabelValues(reason).Inc()
		// A rejected entry is logged once per scan at debug level to avoid a
		// per-file line every interval for a directory of ignored files.
		d.log.Debug("entry is not eligible",
			slog.String("event", "entry_rejected"),
			slog.String("category", reason))
		return false
	}

	if d.cfg.Discovery.MaxFileBytes > 0 && entry.Size > d.cfg.Discovery.MaxFileBytes {
		d.base.Metrics.Discovered.WithLabelValues("too_large").Inc()
		d.log.Warn("submission exceeds the configured size limit",
			slog.String("event", "entry_rejected"),
			slog.String("category", "too_large"),
			slog.Int64("size_bytes", entry.Size),
			slog.Int64("limit_bytes", d.cfg.Discovery.MaxFileBytes))
		return false
	}

	if !d.complete(entry) {
		d.base.Metrics.Discovered.WithLabelValues("unstable").Inc()
		return false
	}

	// Already known? Identity, not name, decides -- and the birth time, where
	// the filesystem reports one, is part of it. See AlreadyRegistered.
	birth := birthTime(entry)
	if _, exists, err := d.base.Ledger.AlreadyRegistered(ctx, root, name, entry.Inode, entry.Device, birth); err != nil {
		d.log.Error("could not check whether a submission is already registered",
			slog.String("event", "discovery_lookup_failed"),
			slog.String("dependency", "postgres_primary"),
			slog.String("error_kind", logging.ErrorKind(err)))
		return false
	} else if exists {
		d.base.Metrics.Discovered.WithLabelValues("already_registered").Inc()
		delete(d.seen, entry.Path)
		return false
	}

	// Fingerprint before registering: the renamer verifies its working copy
	// against this value, which is how a source that changes later is caught.
	//
	// Read through a descriptor opened from the root a component at a time,
	// and hashed as the descriptor -- not by handing the pathname back to the
	// kernel a second time. The old code inspected the path, closed it, and
	// then re-opened it to hash: a parent directory replaced by a symlink
	// pointing outside the incoming root in between made those bytes come from
	// outside the accepted roots. The identity re-check afterwards caught it,
	// but catching it afterwards is not containment -- the read had happened.
	f, opened, err := storage.Open(root, name, d.cfg.Discovery.Recursive)
	if err != nil {
		d.base.Metrics.Discovered.WithLabelValues(storage.RejectionCategory(err)).Inc()
		d.log.Warn("could not open a submission for fingerprinting",
			slog.String("event", "entry_rejected"),
			slog.String("category", storage.RejectionCategory(err)))
		return false
	}
	defer f.Close()
	if !storage.SameFile(entry, opened) {
		d.base.Metrics.Discovered.WithLabelValues("unstable").Inc()
		d.log.Info("submission changed between inspection and reading; leaving it for the next scan",
			slog.String("event", "entry_unstable"))
		return false
	}

	sum, size, err := storage.FingerprintFile(f)
	if err != nil {
		d.base.Metrics.Discovered.WithLabelValues(storage.RejectionCategory(err)).Inc()
		d.log.Warn("could not fingerprint a submission",
			slog.String("event", "entry_rejected"),
			slog.String("category", storage.RejectionCategory(err)))
		return false
	}

	// The file must not have changed while it was being read. Asked of the
	// same descriptor, so this is about the bytes that were actually hashed
	// rather than about whatever the name leads to now.
	st, serr := f.Stat()
	if serr != nil || st.Size() != opened.Size || !st.ModTime().Equal(opened.ModTime) {
		d.base.Metrics.Discovered.WithLabelValues("unstable").Inc()
		d.log.Info("submission changed while it was being fingerprinted; leaving it for the next scan",
			slog.String("event", "entry_unstable"))
		return false
	}

	algo := storage.FingerprintAlgorithm
	inode, device := int64(entry.Inode), int64(entry.Device)
	modified := entry.ModTime
	job, err := d.base.Ledger.RegisterJob(ctx, ledger.RegisterInput{
		SourceRoot:       root,
		SourceName:       name,
		SizeBytes:        &size,
		FingerprintAlgo:  &algo,
		Fingerprint:      sum,
		PolicyIdentity:   d.cfg.Policy.Identity,
		SourceInode:      &inode,
		SourceDevice:     &device,
		SourceModifiedAt: &modified,
		SourceBirthTime:  birth,
		// The destination is part of what this submission is accepted under.
		// Recording it means a renamer configured with a different consume
		// root refuses the job rather than silently redirecting it.
		DestinationRoot: d.cfg.Storage.Consume,
		// So is what happens to the original once it is delivered.
		ArchiveDir: d.cfg.Storage.ArchiveDir,
	})
	if err != nil {
		d.base.Metrics.Discovered.WithLabelValues("storage_error").Inc()
		d.log.Error("could not register a submission",
			slog.String("event", "registration_failed"),
			slog.String("dependency", "postgres_primary"),
			slog.String("error_kind", logging.ErrorKind(err)))
		return false
	}

	d.base.Metrics.Discovered.WithLabelValues("registered").Inc()
	delete(d.seen, entry.Path)
	// The job id is the only job-specific value allowed in an ordinary log
	// line. The submission's name, path and fingerprint are not logged.
	d.log.Info("submission registered",
		slog.String("event", "submission_registered"),
		slog.String("job_id", job.JobID),
		slog.Int64("size_bytes", size),
		slog.String("policy_version", d.cfg.Policy.Identity))
	return true
}

// birthTime returns the entry's birth time for the ledger, or nil when the
// filesystem did not report one. It is truncated to what PostgreSQL keeps, so
// the value written and the value later compared are the same value.
func birthTime(e storage.Entry) *time.Time {
	if !e.BtimeKnown {
		return nil
	}
	t := e.Btime.Truncate(time.Microsecond)
	return &t
}

// complete applies the configured completion contract.
//
// Two gates, and a submission must pass both.
//
// The modification time must be older than the stability interval. It needs no
// memory of a previous scan, so it holds across a restart, and a producer still
// writing to a local filesystem keeps advancing it.
//
// And this process must have watched the entry hold still -- size, modification
// time and identity unchanged -- for the whole interval. The first gate alone is
// not enough, and on a NAS it is not even close: measured against the DS1517
// over SMB, a file written slowly under its final name showed a modification
// time that stopped advancing after its first write while its size kept
// growing, and it did not move again at close. Its modification time was
// therefore "old" while it was still being written, and the only check left
// was that two consecutive scans -- one scan interval apart, five seconds in
// production -- saw the same size. A producer that paused for longer than that
// had its partial file registered, fingerprinted and delivered.
func (d *Discoverer) complete(e storage.Entry) bool {
	interval := d.cfg.Discovery.StabilityInterval
	now := d.clock()

	obs, ok := d.seen[e.Path]
	if !ok || !storage.SameFile(obs.entry, e) {
		obs = observation{entry: e, since: now}
		d.seen[e.Path] = obs
	}
	if interval <= 0 {
		return true
	}
	if now.Sub(e.ModTime) < interval {
		return false
	}
	return now.Sub(obs.since) >= interval
}

// storageCategory maps a root-level failure to a closed-set category.
func storageCategory(err error) string {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "storage_unavailable"
	case errors.Is(err, fs.ErrPermission):
		return "permission_denied"
	default:
		return "storage_error"
	}
}
