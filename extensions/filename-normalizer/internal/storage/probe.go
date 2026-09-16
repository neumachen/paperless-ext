// Package storage probes the configured filesystem roles.
//
// The requirement this package exists to satisfy: inaccessible or incorrectly
// mounted storage must never be interpreted as file absence. A directory that
// is missing, is not a directory, or cannot be read is reported with its own
// status, distinct from a directory that is present and simply empty.
package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Status is a closed-set probe result. It is used as a metric label value.
type Status string

const (
	// StatusOK means the root is a readable, writable directory with entries.
	StatusOK Status = "ok"
	// StatusEmpty means the root is a readable, writable directory with no
	// entries. This is a healthy state and is never conflated with StatusMissing.
	StatusEmpty Status = "empty"
	// StatusMissing means the path does not exist. This is an unavailable-storage
	// condition, not an empty directory.
	StatusMissing Status = "missing"
	// StatusNotADirectory means the path exists but is a file, device or socket,
	// which usually indicates an incorrect mount.
	StatusNotADirectory Status = "not_a_directory"
	// StatusPermissionDenied means the path exists but this process may not read it.
	StatusPermissionDenied Status = "permission_denied"
	// StatusNotWritable means the directory is readable but not writable by this
	// process, so it cannot hold claimed, staged or published work.
	StatusNotWritable Status = "not_writable"
	// StatusProbeError means the probe itself failed for another reason.
	StatusProbeError Status = "probe_error"
)

// Available reports whether the status permits normal operation.
func (s Status) Available() bool { return s == StatusOK || s == StatusEmpty }

// Result describes one probed role.
type Result struct {
	Role    string
	Status  Status
	Entries int
	// Path is retained for operator-facing diagnostics only. It is never
	// emitted by the ordinary logger, which has no "storage_path" safe key.
	Path string
}

// Report is a full probe pass.
type Report struct {
	Results []Result
	At      time.Time
}

// Available reports whether every probed role is usable.
func (r Report) Available() bool {
	for _, res := range r.Results {
		if !res.Status.Available() {
			return false
		}
	}
	return len(r.Results) > 0
}

// Unavailable lists the roles that are not usable, in role order.
func (r Report) Unavailable() []Result {
	var out []Result
	for _, res := range r.Results {
		if !res.Status.Available() {
			out = append(out, res)
		}
	}
	return out
}

// Root is one role to probe.
type Root struct {
	Role string
	Path string
	// WriteRequired marks a role this process must be able to write. A role it
	// only reads is not reported as unavailable merely because the deployment
	// mounted it read-only, which is intended least privilege.
	WriteRequired bool
}

// Probe evaluates every root. Probing never creates a root: a root that must
// exist is an operator/deployment responsibility, and silently creating it
// would mask an incorrect mount.
func Probe(roots []Root) Report {
	out := Report{At: time.Now().UTC()}
	for _, root := range roots {
		out.Results = append(out.Results, probeOne(root))
	}
	sort.Slice(out.Results, func(i, j int) bool { return out.Results[i].Role < out.Results[j].Role })
	return out
}

func probeOne(root Root) Result {
	res := Result{Role: root.Role, Path: root.Path, Status: StatusProbeError}

	info, err := os.Lstat(root.Path)
	switch {
	case err == nil:
	case errors.Is(err, fs.ErrNotExist):
		res.Status = StatusMissing
		return res
	case errors.Is(err, fs.ErrPermission):
		res.Status = StatusPermissionDenied
		return res
	default:
		return res
	}

	// A symlinked root is resolved once and then required to be a directory.
	// The resolution is deliberate: it must not silently follow a link that
	// appeared after deployment into a location outside the intended mount.
	if info.Mode()&os.ModeSymlink != 0 {
		target, lerr := filepath.EvalSymlinks(root.Path)
		if lerr != nil {
			if errors.Is(lerr, fs.ErrNotExist) {
				res.Status = StatusMissing
				return res
			}
			if errors.Is(lerr, fs.ErrPermission) {
				res.Status = StatusPermissionDenied
				return res
			}
			return res
		}
		info, err = os.Stat(target)
		if err != nil {
			if errors.Is(err, fs.ErrPermission) {
				res.Status = StatusPermissionDenied
			} else if errors.Is(err, fs.ErrNotExist) {
				res.Status = StatusMissing
			}
			return res
		}
	}

	if !info.IsDir() {
		res.Status = StatusNotADirectory
		return res
	}

	entries, err := os.ReadDir(root.Path)
	if err != nil {
		if errors.Is(err, fs.ErrPermission) {
			res.Status = StatusPermissionDenied
			return res
		}
		return res
	}
	res.Entries = len(entries)

	if root.WriteRequired && !writable(root.Path) {
		res.Status = StatusNotWritable
		return res
	}

	if res.Entries == 0 {
		res.Status = StatusEmpty
		return res
	}
	res.Status = StatusOK
	return res
}

// writable checks the permission actually granted to this process rather than
// inspecting mode bits, which do not account for ownership or the mount.
func writable(dir string) bool {
	f, err := os.CreateTemp(dir, ".fn-probe-*")
	if err != nil {
		return false
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return true
}
