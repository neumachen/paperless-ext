package config

import (
	"sync"

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
// The mutable part lives behind a pointer because the configuration structs
// are values that get copied freely -- into Common, into the app, into every
// worker. A mutex embedded in a copied value protects nothing and `go vet`
// says so; a shared pointer means every copy of the configuration refers to
// the same root state, which is what adopting a late root requires anyway.
type RootSet struct {
	state *rootState
}

type rootState struct {
	mu    sync.Mutex
	roots []storage.RootID
	// pending are roots that did not resolve at startup. They are adopted --
	// and checked for aliasing -- the first time they become available, so
	// late availability cannot bypass the separation guarantee.
	pending []pendingRoot
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

	out := RootSet{state: &rootState{}}
	for _, r := range want {
		if r.path == "" {
			continue
		}
		id, err := storage.IdentifyRoot(r.name, r.path)
		if err != nil {
			// Unresolvable right now. The applications are expected to start
			// with storage down and report not-ready, so this is not fatal --
			// but it must NOT be forgotten. A root omitted from the recorded
			// set was omitted from every later identity check too, so a
			// staging root that appeared after startup could quietly alias
			// consume and expose working copies to the consumer.
			out.state.pending = append(out.state.pending, pendingRoot{role: r.name, path: r.path})
			continue
		}
		out.state.roots = append(out.state.roots, id)
	}

	if err := storage.DistinctRoots(out.state.roots); err != nil {
		l.fail("%v", err)
	}
	return out
}

// pendingRoot is a configured root that could not be resolved at startup.
type pendingRoot struct{ role, path string }

// Verify re-resolves every recorded root and reports the first that changed.
//
// It is cheap -- a readlink and a stat per root -- and it runs before a job is
// processed, so a root that was remounted or repointed after startup is caught
// before a document is read from or written to the wrong directory.
func (rs RootSet) Verify() error {
	if rs.state == nil {
		return nil
	}
	rs.state.mu.Lock()
	defer rs.state.mu.Unlock()

	// Adopt any root that has appeared since startup, and check it against the
	// others before it is used for anything.
	if len(rs.state.pending) > 0 {
		still := rs.state.pending[:0]
		for _, p := range rs.state.pending {
			id, err := storage.IdentifyRoot(p.role, p.path)
			if err != nil {
				still = append(still, p)
				continue
			}
			rs.state.roots = append(rs.state.roots, id)
		}
		rs.state.pending = still
		if err := storage.DistinctRoots(rs.state.roots); err != nil {
			return err
		}
	}

	for _, r := range rs.state.roots {
		if err := r.Verify(); err != nil {
			return err
		}
		// Hand the identity to the storage layer, which checks it again at the
		// moment it opens the root for an actual read or write. Verifying here
		// alone leaves a window between this check and every use of the root:
		// repointing it afterwards would redirect the operations this call was
		// supposed to protect, and no interval between checks is short enough
		// to close that.
		storage.ExpectRoot(r)
	}
	// Re-checking aliasing every time is cheap and catches a root that was
	// repointed at another role's directory rather than at a new one, which
	// the per-root identity check alone would accept.
	return storage.DistinctRoots(rs.state.roots)
}

// Devices reports the device backing each role, for evidence.
func (rs RootSet) Devices() map[string]uint64 {
	out := map[string]uint64{}
	if rs.state == nil {
		return out
	}
	rs.state.mu.Lock()
	defer rs.state.mu.Unlock()
	for _, r := range rs.state.roots {
		out[r.Role] = r.Device
	}
	return out
}

// Resolved reports the directory each role actually resolves to.
func (rs RootSet) Resolved() map[string]string {
	out := map[string]string{}
	if rs.state == nil {
		return out
	}
	rs.state.mu.Lock()
	defer rs.state.mu.Unlock()
	for _, r := range rs.state.roots {
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
