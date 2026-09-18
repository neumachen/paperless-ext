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
	// ErrRootChanged reports that an authorized root is not the directory it
	// was when the process validated it.
	ErrRootChanged = errors.New("storage root is not the directory it was")
	// ErrRootAlias reports two configured roots resolving to one directory.
	ErrRootAlias = errors.New("two storage roots resolve to the same directory")
)

// ---------------------------------------------------------------------------
// Root identity
// ---------------------------------------------------------------------------

// RootID is the filesystem identity of an authorized root.
//
// Roots are compared by device and inode rather than by pathname, because the
// deployment is allowed to mount a root through a symlink: two different
// strings can name one directory, and one string can name a different
// directory after a remount. A textual comparison satisfies neither case, and
// a staging root that is secretly the consume root would expose an incomplete
// working copy to the consumer.
type RootID struct {
	Role   string
	Path   string
	Real   string
	Device uint64
	Inode  uint64
}

// IdentifyRoot resolves a root and records the directory it actually is.
func IdentifyRoot(role, path string) (RootID, error) {
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return RootID{}, fmt.Errorf("resolve %s root: %w", role, err)
	}
	fi, err := os.Stat(real)
	if err != nil {
		return RootID{}, fmt.Errorf("stat %s root: %w", role, err)
	}
	if !fi.IsDir() {
		return RootID{}, fmt.Errorf("%s root is not a directory", role)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return RootID{}, errors.New("directory identity is unavailable on this platform")
	}
	return RootID{Role: role, Path: path, Real: real, Device: uint64(st.Dev), Inode: uint64(st.Ino)}, nil
}

// Verify re-resolves the root and reports whether it is still the same
// directory. It is called before a root is used, not only at startup.
func (r RootID) Verify() error {
	now, err := IdentifyRoot(r.Role, r.Path)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrRootChanged, r.Role, err)
	}
	if now.Device != r.Device || now.Inode != r.Inode {
		return fmt.Errorf("%w: %s was device %d inode %d and is now device %d inode %d",
			ErrRootChanged, r.Role, r.Device, r.Inode, now.Device, now.Inode)
	}
	return nil
}

// DistinctRoots reports an error if any two roots resolve to one directory.
//
// The paths may legitimately differ in spelling; what must differ is the
// directory. Staging and consume sharing a directory is the dangerous case,
// but any collision means one role can see another's intermediates.
func DistinctRoots(roots []RootID) error {
	seen := map[[2]uint64]RootID{}
	for _, r := range roots {
		key := [2]uint64{r.Device, r.Inode}
		if prev, dup := seen[key]; dup {
			return fmt.Errorf("%w: %s (%s) and %s (%s) are both %s",
				ErrRootAlias, prev.Role, prev.Path, r.Role, r.Path, r.Real)
		}
		seen[key] = r
	}
	return nil
}

// ---------------------------------------------------------------------------
// Names and containment
// ---------------------------------------------------------------------------

// SafeJoin resolves a name inside a root and refuses anything that leaves it.
//
// The name may contain separators only when the caller allows a relative
// subpath, which discovery does in recursive mode. Every component is checked
// individually: a component that is empty, "." or ".." is refused outright, so
// containment does not depend on the cleaned result alone.
func SafeJoin(root, name string) (string, error) { return safeJoin(root, name, false) }

// SafeJoinRel is SafeJoin for a relative subpath below the root.
func SafeJoinRel(root, name string) (string, error) { return safeJoin(root, name, true) }

func safeJoin(root, name string, allowSubdirs bool) (string, error) {
	if name == "" || name == "." || name == ".." {
		return "", fmt.Errorf("%w: empty or relative", ErrUnsafeName)
	}
	if strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("%w: contains a NUL byte", ErrUnsafeName)
	}
	if filepath.IsAbs(name) {
		return "", fmt.Errorf("%w: absolute", ErrUnsafeName)
	}
	if strings.ContainsRune(name, '\\') {
		return "", fmt.Errorf("%w: contains a backslash", ErrUnsafeName)
	}

	parts := strings.Split(name, "/")
	if !allowSubdirs && len(parts) != 1 {
		return "", fmt.Errorf("%w: contains a separator", ErrUnsafeName)
	}
	for _, part := range parts {
		switch part {
		case "", ".", "..":
			return "", fmt.Errorf("%w: unsafe path component", ErrUnsafeName)
		}
	}

	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve root: %w", err)
	}
	full := filepath.Join(realRoot, filepath.Join(parts...))

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

// SameFile reports whether two observations describe the same file, unchanged.
func SameFile(a, b Entry) bool {
	return a.Inode == b.Inode && a.Device == b.Device &&
		a.Size == b.Size && a.ModTime.Equal(b.ModTime)
}

// ---------------------------------------------------------------------------
// Opening: the check and the read are the same file
// ---------------------------------------------------------------------------

// openRegular opens a path without following a final symlink and without
// blocking, then confirms from the OPEN DESCRIPTOR that it is a regular file.
//
// This is the whole point of the function. Checking a pathname with Lstat and
// then opening the same pathname later is two operations on a name, not one
// operation on a file: between them the name can be repointed at a symlink
// leading outside the root, or at a FIFO whose open blocks forever. Here the
// kernel resolves the name once, and every subsequent question -- is it
// regular? what is its identity? what are its bytes? -- is answered about the
// descriptor that resulted, not about the name.
//
// O_NOFOLLOW refuses a final symlink outright. O_NONBLOCK means a FIFO cannot
// hang the open; fstat then rejects it for not being a regular file.
func openRegular(path string) (*os.File, Entry, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		// ELOOP is what O_NOFOLLOW returns for a symlink; report it as one.
		if errors.Is(err, syscall.ELOOP) {
			return nil, Entry{}, ErrSymlink
		}
		return nil, Entry{}, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, Entry{}, err
	}
	if !fi.Mode().IsRegular() {
		f.Close()
		return nil, Entry{}, fmt.Errorf("%w: mode %s", ErrNotRegular, fi.Mode().Type())
	}

	e := Entry{
		Name: filepath.Base(path), Path: path,
		Size: fi.Size(), ModTime: fi.ModTime(),
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		e.Inode = uint64(st.Ino)
		e.Device = uint64(st.Dev)
	}
	return f, e, nil
}

// Open opens an eligible entry inside a root and returns the descriptor
// together with the identity of the file that was actually opened.
//
// The caller reads from the returned descriptor. It must not re-open the path.
//
// # Why every component is opened, not just the last one
//
// O_NOFOLLOW protects the FINAL component only. In recursive discovery the
// name is a relative subpath, so "sub/doc.pdf" can be redirected by replacing
// `sub` with a symlink to somewhere outside the root: the final component is
// then an honest regular file, and the containment check -- which resolved the
// root once and joined a cleaned relative path to it -- never looks at what
// `sub` actually is.
//
// The walk below descends one component at a time from a descriptor on the
// resolved root, each step refusing to follow a symlink. A replaced parent is
// therefore rejected at the moment it is traversed, not inferred from a
// pathname that no longer describes the filesystem.
func Open(root, name string, allowSubdirs bool) (*os.File, Entry, error) {
	if strings.HasPrefix(filepath.Base(name), ".") {
		return nil, Entry{}, ErrHidden
	}
	if _, err := safeJoin(root, name, allowSubdirs); err != nil {
		return nil, Entry{}, err
	}

	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, Entry{}, fmt.Errorf("resolve root: %w", err)
	}

	parts := strings.Split(filepath.ToSlash(name), "/")
	dir, err := os.OpenFile(realRoot, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, Entry{}, fmt.Errorf("open root: %w", err)
	}
	defer func() { _ = dir.Close() }()

	for i, part := range parts[:len(parts)-1] {
		next, derr := openatDir(dir, part)
		if derr != nil {
			if errors.Is(derr, syscall.ELOOP) || errors.Is(derr, syscall.ENOTDIR) {
				return nil, Entry{}, fmt.Errorf("%w: component %d of the path is not a directory this root contains",
					ErrEscapesRoot, i)
			}
			return nil, Entry{}, derr
		}
		_ = dir.Close()
		dir = next
	}

	f, e, err := openRegularAt(dir, parts[len(parts)-1])
	if err != nil {
		return nil, Entry{}, err
	}
	e.Name = name
	e.Path = filepath.Join(realRoot, filepath.Join(parts...))
	return f, e, nil
}

// openatDir opens a subdirectory relative to an open directory, refusing a
// symlink.
func openatDir(dir *os.File, name string) (*os.File, error) {
	fd, err := syscall.Openat(int(dir.Fd()), name,
		syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

// openRegularAt opens a file relative to an open directory and confirms from
// the descriptor that it is a regular file.
func openRegularAt(dir *os.File, name string) (*os.File, Entry, error) {
	fd, err := syscall.Openat(int(dir.Fd()), name,
		syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, Entry{}, ErrSymlink
		}
		return nil, Entry{}, err
	}
	f := os.NewFile(uintptr(fd), name)

	fi, serr := f.Stat()
	if serr != nil {
		f.Close()
		return nil, Entry{}, serr
	}
	if !fi.Mode().IsRegular() {
		f.Close()
		return nil, Entry{}, fmt.Errorf("%w: mode %s", ErrNotRegular, fi.Mode().Type())
	}

	e := Entry{Name: name, Size: fi.Size(), ModTime: fi.ModTime()}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		e.Inode = uint64(st.Ino)
		e.Device = uint64(st.Dev)
	}
	return f, e, nil
}

// Inspect reports an entry's identity without reading it.
//
// It opens the file rather than calling Lstat, so the entry it describes is a
// file that could actually be opened as a regular file at that moment. A
// caller that then reads by pathname still races; callers that must read
// should use Open or OpenExpected.
func Inspect(root, name string) (Entry, error) {
	f, e, err := Open(root, name, false)
	if err != nil {
		return Entry{}, err
	}
	f.Close()
	return e, nil
}

// InspectRel is Inspect for a relative subpath below the root.
func InspectRel(root, name string) (Entry, error) {
	f, e, err := Open(root, name, true)
	if err != nil {
		return Entry{}, err
	}
	f.Close()
	return e, nil
}

// OpenExpected opens an entry and requires it to be the file the caller
// already observed.
//
// This closes the window between discovery and use: if the name now leads to a
// different file -- replaced, relinked, or rewritten -- the open is refused
// with ErrMutated instead of reading bytes that belong to something else.
func OpenExpected(root, name string, expect Entry, allowSubdirs bool) (*os.File, Entry, error) {
	f, got, err := Open(root, name, allowSubdirs)
	if err != nil {
		return nil, Entry{}, err
	}
	if got.Inode != expect.Inode || got.Device != expect.Device {
		f.Close()
		return nil, Entry{}, fmt.Errorf("%w: identity changed", ErrMutated)
	}
	return f, got, nil
}

// ---------------------------------------------------------------------------
// Content
// ---------------------------------------------------------------------------

// Fingerprint streams a file and returns its SHA-256 and size.
//
// It refuses a final symlink and anything that is not a regular file, and it
// reads the descriptor it opened rather than re-opening the path.
func Fingerprint(path string) ([]byte, int64, error) {
	f, _, err := openRegular(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	return hashReader(f)
}

// FingerprintFile hashes an already-open descriptor from its start.
func FingerprintFile(f *os.File) ([]byte, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	return hashReader(f)
}

func hashReader(r io.Reader) ([]byte, int64, error) {
	h := sha256.New()
	n, err := io.CopyBuffer(h, r, make([]byte, copyBufferSize))
	if err != nil {
		return nil, 0, err
	}
	return h.Sum(nil), n, nil
}

// HexFingerprint renders a fingerprint for restricted diagnostics.
func HexFingerprint(sum []byte) string { return hex.EncodeToString(sum) }

// CopyResult describes a completed working copy.
type CopyResult struct {
	Path        string
	Size        int64
	Fingerprint []byte
}

// CopyFrom copies an already-open source into a newly created file and returns
// the fingerprint of the bytes actually written.
//
// The source is a descriptor, not a pathname, so the bytes copied are the
// bytes of the file the caller verified. dst is created exclusively; an
// existing file is never opened or truncated.
func CopyFrom(src *os.File, dst string) (CopyResult, error) {
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		return CopyResult{}, fmt.Errorf("rewind source: %w", err)
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o640)
	if err != nil {
		return CopyResult{}, fmt.Errorf("create working copy: %w", err)
	}
	committed := false
	defer func() {
		out.Close()
		if !committed {
			_ = os.Remove(dst)
		}
	}()

	h := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(out, h), src, make([]byte, copyBufferSize))
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

// CopyVerified copies one path to another, refusing symlinks on both sides.
func CopyVerified(src, dst string) (CopyResult, error) {
	in, _, err := openRegular(src)
	if err != nil {
		return CopyResult{}, fmt.Errorf("open source: %w", err)
	}
	defer in.Close()
	return CopyFrom(in, dst)
}

// ---------------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------------

// PublishExclusive makes src visible at dst under a name that must not already
// exist, and never overwrites.
//
// link(2) plus unlink(2), not rename(2). rename replaces an existing
// destination silently, so "check that it is absent, then rename" is a race
// the contract rejects. link fails with EEXIST, which makes the absence check
// and the publication one atomic step decided by the kernel.
//
// src and dst must be on the same filesystem, which is why the caller stages
// the temporary inside the destination directory. The publication step
// therefore never crosses a filesystem boundary, whatever the topology.
// It reports `published` separately from `err` because the three operations it
// performs are not equivalent. The link either created the directory entry or
// it did not; everything after that happens to a document the consumer can
// already see. Returning all three through one error value meant the caller
// could only ask "did this fail", and it answered by inspecting the errno --
// so an EACCES from the post-link unlink was read as a refusal that proved
// nothing had been published, the publication claim was withdrawn, and a
// retry could publish the document a second time after a consumer had taken
// the first. `published` is the only thing that can answer that question, and
// only this function knows it.
func PublishExclusive(src, dst string) (published bool, err error) {
	if err := os.Link(src, dst); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, fmt.Errorf("%w: %s", ErrDestinationExists, filepath.Base(dst))
		}
		return false, fmt.Errorf("link into place: %w", err)
	}
	// From here the document IS published: it is visible at dst under a name
	// nothing else may take. Failures below are reported, never reclassified.
	if err := SyncDir(filepath.Dir(dst)); err != nil {
		// The link exists; report the failure without unlinking, because
		// removing a published document to tidy up an fsync error would be
		// worse than the error.
		return true, fmt.Errorf("sync destination directory: %w", err)
	}
	if err := os.Remove(src); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return true, fmt.Errorf("remove the staged link: %w", err)
	}
	return true, nil
}

// LinkExclusive links src to dst and reports whether dst already existed.
//
// It is how a verified working copy is promoted to its canonical name without
// any possibility of replacing another attempt's file.
func LinkExclusive(src, dst string) (existed bool, err error) {
	if err := os.Link(src, dst); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return true, nil
		}
		return false, err
	}
	return false, nil
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
func SameFilesystem(a, b string) (bool, error) {
	da, err := DeviceOf(a)
	if err != nil {
		return false, err
	}
	db, err := DeviceOf(b)
	if err != nil {
		return false, err
	}
	return da == db, nil
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
	case errors.Is(err, ErrRootChanged), errors.Is(err, ErrRootAlias):
		return "storage_unavailable"
	case errors.Is(err, fs.ErrPermission):
		return "permission_denied"
	case errors.Is(err, fs.ErrNotExist):
		return "source_absent"
	default:
		return "storage_error"
	}
}

// Identify reports the identity of a regular file without reading it.
//
// It refuses a final symlink and anything that is not a regular file, and it
// answers from the descriptor it opened rather than from a second lookup of
// the pathname.
//
// Identity is what proves ownership of a destination. Content cannot: two
// distinct submissions may legitimately hold identical bytes, and the contract
// requires them to stay distinct, so "the bytes match" must never be read as
// "this is my file".
func Identify(path string) (Entry, error) {
	f, e, err := openRegular(path)
	if err != nil {
		return Entry{}, err
	}
	f.Close()
	return e, nil
}
