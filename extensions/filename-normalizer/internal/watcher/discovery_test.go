package watcher

import (
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
)

// stabilityRig drives the completion gate with a clock the test controls.
type stabilityRig struct {
	d   *Discoverer
	now time.Time
}

func newStabilityRig(interval time.Duration) *stabilityRig {
	r := &stabilityRig{now: time.Date(2026, 9, 24, 21, 30, 0, 0, time.UTC)}
	cfg := config.WatcherConfig{}
	cfg.Discovery.StabilityInterval = interval
	r.d = &Discoverer{cfg: cfg, seen: map[string]observation{}, clock: func() time.Time { return r.now }}
	return r
}

func (r *stabilityRig) scanAfter(d time.Duration, e storage.Entry) bool {
	r.now = r.now.Add(d)
	return r.d.complete(e)
}

func slowScan(size int64, mtime time.Time) storage.Entry {
	return storage.Entry{Path: "/srv/fn/incoming/scan.pdf", Size: size, ModTime: mtime, Inode: 7922335, Device: 2097166}
}

// The case measured against the NAS: a producer writing under the final name,
// whose modification time stopped at its first write while the size kept
// growing. It paused for longer than a scan interval before finishing. The old
// gate registered it during the pause; this one must wait out the interval.
func TestAFileWhoseModificationTimeStoppedIsWatchedNotTrusted(t *testing.T) {
	r := newStabilityRig(60 * time.Second)
	stuck := r.now.Add(-2 * time.Minute) // "old" from the start

	if r.scanAfter(0, slowScan(204878, stuck)) {
		t.Fatal("registered on first sight")
	}
	if r.scanAfter(5*time.Second, slowScan(409756, stuck)) {
		t.Fatal("registered while growing")
	}
	// The producer pauses: two scans, five seconds apart, see the same size.
	if r.scanAfter(5*time.Second, slowScan(409756, stuck)) {
		t.Fatal("registered five seconds into a pause, with the file half written")
	}
	if r.scanAfter(5*time.Second, slowScan(409756, stuck)) {
		t.Fatal("registered ten seconds into a pause")
	}
	// It resumes, which restarts the quiet period ...
	if r.scanAfter(5*time.Second, slowScan(819512, stuck)) {
		t.Fatal("registered while growing again")
	}
	// ... and only a full interval of quiet completes it.
	if r.scanAfter(55*time.Second, slowScan(819512, stuck)) {
		t.Fatal("registered before a full interval of quiet")
	}
	if !r.scanAfter(5*time.Second, slowScan(819512, stuck)) {
		t.Fatal("not registered after a full interval of quiet")
	}
}

// A fresh modification time still holds a file back, whatever this process has
// watched.
func TestARecentModificationTimeStillHoldsAFileBack(t *testing.T) {
	r := newStabilityRig(30 * time.Second)
	e := slowScan(1000, r.now)
	for i := 0; i < 5; i++ {
		if r.scanAfter(5*time.Second, e) {
			t.Fatalf("registered %d seconds after its last modification", 5*(i+1))
		}
	}
	if !r.scanAfter(10*time.Second, e) {
		t.Fatal("not registered once both gates were satisfied")
	}
}

// A replacement under the same name, with the same size and times, is a
// different file: the quiet period starts again.
func TestAReplacementRestartsTheQuietPeriod(t *testing.T) {
	r := newStabilityRig(30 * time.Second)
	old := r.now.Add(-time.Hour)
	e := slowScan(1000, old)
	r.scanAfter(0, e)
	replaced := e
	replaced.Inode++
	if r.scanAfter(30*time.Second, replaced) {
		t.Fatal("a replacement inherited its predecessor's quiet period")
	}
	if !r.scanAfter(30*time.Second, replaced) {
		t.Fatal("the replacement was not registered after its own quiet period")
	}
}

func TestAZeroIntervalAcceptsAtOnce(t *testing.T) {
	r := newStabilityRig(0)
	if !r.scanAfter(0, slowScan(1, r.now)) {
		t.Fatal("a zero stability interval held a file back")
	}
}
