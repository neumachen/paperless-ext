package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// FingerprintAlgorithm names the content hash recorded for every submission.
const FingerprintAlgorithm = "sha256"

// copyBufferSize is the streaming buffer. Documents are never read whole into
// memory: a submission can be arbitrarily large and the applications run with
// modest limits.
const copyBufferSize = 1 << 20

// Eligibility errors. Each one maps to a closed-set category, so a rejected
// submission is explained without quoting its name.
var (
	// ErrNotRegular reports a directory, socket, device or FIFO.
	ErrNotRegular = errors.New("not a regular file")
	// ErrSymlink reports a symbolic link, which is never followed.
	ErrSymlink = errors.New("symbolic link")
	// ErrEscapesRoot reports a path that resolves outside its authorized root.
	ErrEscapesRoot = errors.New("path escapes its root")
	// ErrUnsafeName reports a name containing a separator or traversal.
	ErrUnsafeName = errors.New("unsafe file name")
	// ErrHidden reports a dotfile.
	ErrHidden = errors.New("hidden file")
	// ErrMutated reports that a source changed after it was recorded.
	ErrMutated = errors.New("source changed after discovery")
	// ErrDestinationExists reports that a destination name is already taken by
	// a file this job did not publish.
	ErrDestinationExists = errors.New("destination already exists")
)

// SafeJoin resolves name inside root and refuses anything that leaves it.
//
// The name must be a single path element: discovery hands over base names, and
// a stored source_name that somehow contained a separator must never be
// interpreted as a path. The root itself is resolved through symlinks once, so
// a legitimately symlinked root still works, while a symlinked *entry* inside
// it is rejected separately by Inspect.
func SafeJoin(root, name string) (string, error) {
	if name == "" || name == "." || name == ".." {
		return "", fmt.Errorf("%w: empty or relative", ErrUnsafeName)
	}
	if strings.ContainsRune(name, os.PathSeparator) || strings.ContainsRune(name, '/') {
		return "", fmt.Errorf("%w: contains a separator", ErrUnsafeName)
	}
	if strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("%w: contains a NUL byte", ErrUnsafeName)
	}

	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve root: %w", err)
	}
	full := filepath.Join(realRoot, name)

	// Join already cleans, but verify explicitly rather than trusting it.
	rel, err := filepath.Rel(realRoot, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %s", ErrEscapesRoot, rel)
	}
	return full, nil
}

// Entry describes one inspected filesystem entry.
type Entry struct {
	Name    string
	Path    string
	Size    int64
	ModTime time.Time
	// Inode and Device identify the file itself, so a replacement between two
	// observations can be detected even when name, size and mtime match.
	Inode  uint64
	Device uint64
}

// Inspect stats a candidate without following symlinks and rejects everything
// that is not an ordinary, non-hidden regular file inside root.
//
// Lstat rather than Stat is the point: a symlink pointing at a file outside
// the root would satisfy Stat, and following it would publish a document the
// operator never placed in the incoming directory.
func Inspect(root, name string) (Entry, error) {
	// Containment is checked BEFORE the dotfile rule. "../escape.pdf" starts
	// with a dot and would otherwise be reported as a hidden file, which is
	// true but useless: the reason that matters is that it tried to leave the
	// root, and an operator reading "hidden_file" would not learn that.
	path, err := SafeJoin(root, name)
	if err != nil {
		return Entry{}, err
	}
	if strings.HasPrefix(filepath.Base(name), ".") {
		return Entry{}, ErrHidden
	}
	fi, err := os.Lstat(path)
	if err != nil {
		return Entry{}, err
	}
	switch {
	case fi.Mode()&fs.ModeSymlink != 0:
		return Entry{}, ErrSymlink
	case !fi.Mode().IsRegular():
		return Entry{}, fmt.Errorf("%w: mode %s", ErrNotRegular, fi.Mode().Type())
	}

	e := Entry{Name: name, Path: path, Size: fi.Size(), ModTime: fi.ModTime()}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		e.Inode = uint64(st.Ino)
		e.Device = uint64(st.Dev)
	}
	return e, nil
}

// SameFile reports whether two observations describe the same file, unchanged.
//
// Identity is compared before content: a source replaced by a different file
// with identical size and timestamp is still a different submission.
func SameFile(a, b Entry) bool {
	return a.Inode == b.Inode && a.Device == b.Device &&
		a.Size == b.Size && a.ModTime.Equal(b.ModTime)
}

// Fingerprint streams a file and returns its SHA-256 and size.
func Fingerprint(path string) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.CopyBuffer(h, f, make([]byte, copyBufferSize))
	if err != nil {
		return nil, 0, err
	}
	return h.Sum(nil), n, nil
}

// HexFingerprint renders a fingerprint for restricted diagnostics.
//
// A content hash identifies a document, so it belongs in restricted state and
// explicitly requested reports, never in ordinary logs or metric labels.
func HexFingerprint(sum []byte) string { return hex.EncodeToString(sum) }

// CopyResult describes a completed working copy.
type CopyResult struct {
	Path        string
	Size        int64
	Fingerprint []byte
}

// CopyVerified copies src to a working copy at dst, fsyncs it, and returns the
// content fingerprint computed from the bytes that were actually written.
//
// The fingerprint is taken during the copy rather than by re-reading the
// source, so what is verified is what landed on disk. dst is created
// exclusively; an existing working copy is removed first only when the caller
// has established that it owns it.
func CopyVerified(src, dst string) (CopyResult, error) {
	in, err := os.Open(src)
	if err != nil {
		return CopyResult{}, fmt.Errorf("open source: %w", err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return CopyResult{}, fmt.Errorf("create working copy: %w", err)
	}
	// Any failure after this point must not leave a half-written working copy
	// that a later attempt could mistake for a verified one.
	committed := false
	defer func() {
		out.Close()
		if !committed {
			_ = os.Remove(dst)
		}
	}()

	h := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(out, h), in, make([]byte, copyBufferSize))
	if err != nil {
		return CopyResult{}, fmt.Errorf("copy: %w", err)
	}
	if err := out.Sync(); err != nil {
		return CopyResult{}, fmt.Errorf("sync working copy: %w", err)
	}
	if err := out.Close(); err != nil {
		return CopyResult{}, fmt.Errorf("close working copy: %w", err)
	}
	committed = true
	return CopyResult{Path: dst, Size: n, Fingerprint: h.Sum(nil)}, nil
}

// PublishExclusive makes src visible at dst under a name that must not already
// exist, and never overwrites.
//
// It uses link(2) plus unlink(2) rather than rename(2). rename replaces an
// existing destination silently, so "check that it is absent, then rename" is
// a race the contract explicitly rejects. link fails with EEXIST if the
// destination exists, which makes the absence check and the publication one
// atomic step decided by the kernel.
//
// src and dst must be on the same filesystem, which is why the caller stages
// the temporary inside the destination directory. That also means the
// publication step never crosses a filesystem boundary, so it behaves
// identically for same-filesystem and cross-filesystem deployments.
func PublishExclusive(src, dst string) error {
	if err := os.Link(src, dst); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: %s", ErrDestinationExists, filepath.Base(dst))
		}
		return fmt.Errorf("link into place: %w", err)
	}
	// The destination is durable only once its directory entry is.
	if err := SyncDir(filepath.Dir(dst)); err != nil {
		// The link exists; report the failure without unlinking, because
		// removing a published document to tidy up an fsync error would be
		// worse than the error.
		return fmt.Errorf("sync destination directory: %w", err)
	}
	if err := os.Remove(src); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove the staged link: %w", err)
	}
	return nil
}

// SyncDir flushes a directory entry so a rename or link survives a crash.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// SameFilesystem reports whether two existing paths share a device.
//
// It exists so a deployment can state, from evidence rather than from two
// different-looking paths, whether its staging and destination roots are
// genuinely on separate filesystems.
func SameFilesystem(a, b string) (bool, error) {
	fa, err := os.Stat(a)
	if err != nil {
		return false, err
	}
	fb, err := os.Stat(b)
	if err != nil {
		return false, err
	}
	sa, oka := fa.Sys().(*syscall.Stat_t)
	sb, okb := fb.Sys().(*syscall.Stat_t)
	if !oka || !okb {
		return false, errors.New("device identity is unavailable on this platform")
	}
	return sa.Dev == sb.Dev, nil
}

// DeviceOf reports the device id backing a path, for evidence.
func DeviceOf(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("device identity is unavailable on this platform")
	}
	return uint64(st.Dev), nil
}

// RejectionCategory maps an eligibility error to its closed-set category.
func RejectionCategory(err error) string {
	switch {
	case errors.Is(err, ErrHidden):
		return "hidden_file"
	case errors.Is(err, ErrSymlink):
		return "symlink"
	case errors.Is(err, ErrNotRegular):
		return "not_regular_file"
	case errors.Is(err, ErrEscapesRoot):
		return "escapes_root"
	case errors.Is(err, ErrUnsafeName):
		return "unsafe_name"
	case errors.Is(err, ErrMutated):
		return "source_mutated"
	case errors.Is(err, fs.ErrPermission):
		return "permission_denied"
	case errors.Is(err, fs.ErrNotExist):
		return "source_absent"
	default:
		return "storage_error"
	}
}
