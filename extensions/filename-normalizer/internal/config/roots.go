package config

import (
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
)

// RootSet is the verified filesystem identity of the configured roots.
//
// # Why identity and not pathnames
//
// The textual checks in validateStorageRoots catch the obvious mistakes --
// relative paths, duplicates, one root written inside another -- but they
// compare strings, and the deployment is allowed to mount a root through a
// symlink. Two different strings can therefore name one directory, and the
// string comparison would see nothing wrong. The dangerous instance is staging
// and consume resolving to the same place: an incomplete working copy would be
// sitting in the consumer's directory.
//
// The set below resolves every root and compares device and inode, which is
// what actually distinguishes one directory from another. It also remembers
// the identity so a later check can tell whether a root is still the same
// directory: a remount between startup and use would otherwise be invisible.
type RootSet struct {
	roots []storage.RootID
}

// identifyRoots resolves the configured roots and checks they are distinct.
//
// A root that cannot be resolved is not fatal here: the applications are
// expected to start with storage unavailable and report not-ready rather than
// crash-loop, and the storage probe already reports that. What is fatal is an
// alias, because that is a configuration error no amount of waiting fixes.
func identifyRoots(l *loader, s Storage) RootSet {
	type role struct{ name, path string }
	want := []role{
		{"incoming", s.Incoming},
		{"queued", s.Queued},
		{"staging", s.Staging},
		{"consume", s.Consume},
		{"failed", s.Failed},
	}

	var out RootSet
	for _, r := range want {
		if r.path == "" {
			continue
		}
		id, err := storage.IdentifyRoot(r.name, r.path)
		if err != nil {
			// Unresolvable now; readiness will report it. Nothing is recorded,
			// so Verify has nothing to compare and will not block processing
			// once the root appears.
			continue
		}
		out.roots = append(out.roots, id)
	}

	if err := storage.DistinctRoots(out.roots); err != nil {
		l.fail("%v", err)
	}
	return out
}

// Verify re-resolves every recorded root and reports the first that changed.
//
// It is cheap -- a readlink and a stat per root -- and it runs before a job is
// processed, so a root that was remounted or repointed after startup is caught
// before a document is read from or written to the wrong directory.
func (rs RootSet) Verify() error {
	for _, r := range rs.roots {
		if err := r.Verify(); err != nil {
			return err
		}
	}
	return nil
}

// Devices reports the device backing each role, for evidence.
func (rs RootSet) Devices() map[string]uint64 {
	out := map[string]uint64{}
	for _, r := range rs.roots {
		out[r.Role] = r.Device
	}
	return out
}

// Resolved reports the directory each role actually resolves to.
func (rs RootSet) Resolved() map[string]string {
	out := map[string]string{}
	for _, r := range rs.roots {
		out[r.Role] = r.Real
	}
	return out
}

// warnOnFaults loads the injected fault points and makes them loud.
//
// A process running with an armed fault point will deliberately die in the
// middle of publishing a document. That must never be mistaken for a normal
// deployment, so it is recorded as a configuration problem-level warning in
// the startup summary rather than mentioned quietly.
func warnOnFaults(l *loader) FaultPoints {
	points, names := LoadFaultPoints()
	if len(names) == 0 {
		return nil
	}
	for _, n := range names {
		if len(n) > 8 && n[:8] == "unknown:" {
			l.fail("FN_FAULT_POINTS names an unknown fault point %q", n[8:])
		}
	}
	return points
}
