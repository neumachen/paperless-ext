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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
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

// expectedRoots records the identity each accepted root had when it was
// validated, keyed by the configured path.
//
// It is package state on purpose: every read and write in this package goes
// through Open, and the check belongs with the operation rather than with a
// caller who might forget it.
var expectedRoots sync.Map // string -> RootID

// ExpectRoot records what a root's identity must be from now on. Called for
// each root once it has been validated; later opens are checked against it.
func ExpectRoot(id RootID) { expectedRoots.Store(id.Path, id) }

// ForgetRoots drops every expectation. It exists for tests.
func ForgetRoots() {
	expectedRoots.Range(func(k, _ any) bool {
		expectedRoots.Delete(k)
		return true
	})
}

// verifyRootDescriptor checks an opened root directory against its recorded
// identity. A root with no expectation recorded is not checked: this package is
// also used before configuration has been validated, and inventing an
// expectation there would refuse legitimate work.
func verifyRootDescriptor(root string, dir *os.File) error {
	v, ok := expectedRoots.Load(root)
	if !ok {
		return nil
	}
	want := v.(RootID)
	fi, err := dir.Stat()
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrRootChanged, want.Role, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if uint64(st.Dev) != want.Device || uint64(st.Ino) != want.Inode {
		return fmt.Errorf("%w: %s was device %d inode %d and the open reached device %d inode %d",
			ErrRootChanged, want.Role, want.Device, want.Inode, uint64(st.Dev), uint64(st.Ino))
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
	return openWithin(root, name, allowSubdirs)
}

// OpenOwnTemp opens a temporary file THIS process staged, and requires it to be
// the file the caller observed.
//
// Every attempt-private temporary is a dotfile on purpose -- the destination
// directory is watched by a consumer, and a leading dot is the conventional
// signal for "not a submission" -- so Open refuses them, correctly, because a
// dotfile appearing in incoming is somebody else's business. Verifying our own
// staged bytes still has to read one, and it is not the same question: the name
// carries a per-attempt nonce this process generated, and the identity is
// checked against the entry the caller already saw.
func OpenOwnTemp(root, name string, expect Entry) (*os.File, Entry, error) {
	f, got, err := openWithin(root, name, false)
	if err != nil {
		return nil, Entry{}, err
	}
	if got.Inode != expect.Inode || got.Device != expect.Device {
		f.Close()
		return nil, Entry{}, fmt.Errorf("%w: identity changed", ErrMutated)
	}
	return f, got, nil
}

func openWithin(root, name string, allowSubdirs bool) (*os.File, Entry, error) {
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

	// The root this open actually reached must be the root that was accepted.
	//
	// Verifying roots on their own, before the work, leaves a gap between the
	// check and every use: a root repointed afterwards redirects the reads and
	// writes that follow, and a periodic Verify call cannot close a window it
	// is not inside. The identity is therefore checked HERE, against the
	// descriptor this call is about to walk from, so a replaced root is caught
	// by the operation it would have redirected.
	if err := verifyRootDescriptor(root, dir); err != nil {
		return nil, Entry{}, err
	}

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

// PublishFromDescriptor makes the bytes behind an open descriptor visible under
// dstName in dir, and never overwrites.
//
// It is PublishExclusive with the check/use gap removed. The old form took two
// PATHNAMES: the caller verified the temporary's content through a descriptor,
// closed it, and then asked the kernel to resolve that name again for the link.
// Whatever the name led to at that second resolution is what became visible, so
// the bytes that were verified and the bytes that were published were only
// probably the same file. Linking from the descriptor makes them the same file
// by construction.
//
// `published` is reported separately from `err` for the same reason as before:
// the link either created the directory entry or it did not, and everything
// after it happens to a document a consumer can already see. An errno cannot
// answer that question -- EACCES from the post-link unlink and EACCES from the
// link are the same value and opposite facts.
func PublishFromDescriptor(dir *Dir, src *os.File, srcName, dstName string) (published bool, err error) {
	if src == nil {
		return false, errors.New("publish: no verified descriptor")
	}
	if err := dir.LinkFromDescriptor(src, dstName); err != nil {
		if errors.Is(err, ErrDestinationExists) {
			return false, err
		}
		return false, fmt.Errorf("link into place: %w", err)
	}
	// From here the document IS published: it is visible at dstName under a
	// name nothing else may take. Failures below are reported, never
	// reclassified.
	if err := dir.Sync(); err != nil {
		return true, fmt.Errorf("sync destination directory: %w", err)
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

// ---------------------------------------------------------------------------
// Dir: operations that happen to a verified directory, not to a pathname
// ---------------------------------------------------------------------------

// Dir is an open, identity-verified directory.
//
// # Why this exists
//
// Verifying a root and then acting on a pathname underneath it are two
// different operations on two different things. `Open` already closed that gap
// for reads: it walks from the root a component at a time and checks the root's
// identity against the descriptor it is about to walk from. Everything that
// CREATES, LINKS or REMOVES still went through a pathname, so a root -- or a
// component of it -- replaced after the verification redirected exactly the
// operations the verification existed to protect. A window between a check and
// an operation cannot be closed by checking more often; it is closed by making
// the check and the operation use the same descriptor.
//
// Every method here is relative to that descriptor. Nothing re-resolves the
// root, so replacing the root's pathname after OpenDir returns cannot redirect
// a create, a link or a removal issued through this handle.
type Dir struct {
	f    *os.File
	root string
}

// OpenDir opens a root and verifies it is the directory that was accepted.
func OpenDir(root string) (*Dir, error) {
	real, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}
	f, err := os.OpenFile(real, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open root: %w", err)
	}
	if err := verifyRootDescriptor(root, f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Dir{f: f, root: root}, nil
}

// Close releases the directory descriptor.
func (d *Dir) Close() error { return d.f.Close() }

// Sync flushes the directory so a link survives a crash.
func (d *Dir) Sync() error { return d.f.Sync() }

// Identify reports the identity of one entry, without following a symlink and
// without resolving the name through the filesystem root.
func (d *Dir) Identify(name string) (Entry, error) {
	if err := checkLeaf(name); err != nil {
		return Entry{}, err
	}
	var st unix.Stat_t
	if err := unix.Fstatat(int(d.f.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return Entry{}, &os.PathError{Op: "fstatat", Path: filepath.Join(d.root, name), Err: err}
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return Entry{}, fmt.Errorf("%w: %s", ErrNotRegular, name)
	}
	return Entry{
		Name: name, Path: filepath.Join(d.root, name),
		Size: st.Size, ModTime: time.Unix(st.Mtim.Sec, st.Mtim.Nsec),
		Inode: uint64(st.Ino), Device: uint64(st.Dev),
	}, nil
}

// CreateExclusive creates a new file that must not already exist and returns it
// open for writing.
func (d *Dir) CreateExclusive(name string, perm os.FileMode) (*os.File, error) {
	if err := checkLeaf(name); err != nil {
		return nil, err
	}
	fd, err := syscall.Openat(int(d.f.Fd()), name,
		syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC,
		uint32(perm.Perm()))
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: filepath.Join(d.root, name), Err: err}
	}
	return os.NewFile(uintptr(fd), filepath.Join(d.root, name)), nil
}

// OpenOwn opens an entry this process created and requires it to still be the
// file the caller observed.
//
// The dotfile refusal that guards submissions does not apply: these are this
// attempt's own temporaries, named with a nonce this process generated, and the
// identity is checked rather than assumed.
func (d *Dir) OpenOwn(name string, expect Entry) (*os.File, Entry, error) {
	if err := checkLeaf(name); err != nil {
		return nil, Entry{}, err
	}
	fd, err := syscall.Openat(int(d.f.Fd()), name,
		syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, Entry{}, &os.PathError{Op: "openat", Path: filepath.Join(d.root, name), Err: err}
	}
	f := os.NewFile(uintptr(fd), filepath.Join(d.root, name))
	got, err := identifyFile(f, name, d.root)
	if err != nil {
		_ = f.Close()
		return nil, Entry{}, err
	}
	if got.Inode != expect.Inode || got.Device != expect.Device {
		_ = f.Close()
		return nil, Entry{}, fmt.Errorf("%w: identity changed", ErrMutated)
	}
	return f, got, nil
}

// LinkFromDescriptor publishes the bytes behind an OPEN DESCRIPTOR under name.
//
// This is the difference between publishing what was verified and publishing
// whatever the verified name happens to lead to now. The old sequence opened
// the staged temporary, hashed it, closed it, and then linked its PATHNAME:
// anything that replaced the temporary in between was published instead, and
// the receipt described bytes that were never at the destination. linkat from
// /proc/self/fd/N links the inode the descriptor holds, so the file that
// appears at the destination is the file that was verified, whatever happened
// to its name.
//
// It returns ErrDestinationExists for EEXIST, which is the kernel deciding the
// absence check and the claim in one step.
func (d *Dir) LinkFromDescriptor(src *os.File, name string) error {
	if err := checkLeaf(name); err != nil {
		return err
	}
	err := linkat(-1, "/proc/self/fd/"+strconv.Itoa(int(src.Fd())),
		int(d.f.Fd()), name, atSymlinkFollow)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, syscall.EEXIST):
		return fmt.Errorf("%w: %s", ErrDestinationExists, name)
	default:
		return &os.PathError{Op: "linkat", Path: filepath.Join(d.root, name), Err: err}
	}
}

// RemoveOwned removes an entry only while it is still the file the caller
// identified, and reports whether the file it removed was that one.
//
// The identity check and the removal are issued against the same directory
// descriptor, so neither re-resolves the root or the parent. That leaves one
// unavoidable window -- the kernel has no "unlink this inode" -- so the file is
// held open across the removal and its link count is read afterwards: if the
// descriptor still has links, the name led to a different file by then and this
// call removed somebody else's. That cannot be undone, and it is reported as a
// failure rather than counted as a successful cleanup.
func (d *Dir) RemoveOwned(name string, expect Entry) (bool, error) {
	f, _, err := d.OpenOwn(name, expect)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer func() { _ = f.Close() }()

	if err := unlinkat(int(d.f.Fd()), name); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return false, nil
		}
		return false, &os.PathError{Op: "unlinkat", Path: filepath.Join(d.root, name), Err: err}
	}
	var st unix.Stat_t
	if ferr := unix.Fstat(int(f.Fd()), &st); ferr == nil && st.Nlink > 0 {
		return false, fmt.Errorf("%w: %s was replaced between the identity check and the removal; a file this process does not own was removed",
			ErrMutated, name)
	}
	return true, nil
}

// RemoveIfOurs removes an entry created by this process, tolerating a name that
// is already gone. It is the cleanup counterpart of CreateExclusive.
func (d *Dir) RemoveIfOurs(name string, expect Entry) error {
	_, err := d.RemoveOwned(name, expect)
	return err
}

// checkLeaf refuses anything but a single, contained path component.
func checkLeaf(name string) error {
	if name == "" || strings.ContainsRune(name, '/') {
		return fmt.Errorf("%w: %q is not a single path component", ErrUnsafeName, name)
	}
	switch name {
	case ".", "..":
		return fmt.Errorf("%w: unsafe path component", ErrUnsafeName)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Directory-relative syscalls
// ---------------------------------------------------------------------------

// atSymlinkFollow asks linkat to resolve /proc/self/fd/N to the file the
// descriptor holds rather than to the magic symlink itself.
const atSymlinkFollow = unix.AT_SYMLINK_FOLLOW

func linkat(oldDirFd int, oldPath string, newDirFd int, newPath string, flags int) error {
	return unix.Linkat(oldDirFd, oldPath, newDirFd, newPath, flags)
}

func unlinkat(dirfd int, name string) error {
	return unix.Unlinkat(dirfd, name, 0)
}

// identifyFile describes an open descriptor.
func identifyFile(f *os.File, name, root string) (Entry, error) {
	fi, err := f.Stat()
	if err != nil {
		return Entry{}, err
	}
	if !fi.Mode().IsRegular() {
		return Entry{}, fmt.Errorf("%w: %s", ErrNotRegular, name)
	}
	e := Entry{Name: name, Path: filepath.Join(root, name), Size: fi.Size(), ModTime: fi.ModTime()}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		e.Inode, e.Device = uint64(st.Ino), uint64(st.Dev)
	}
	return e, nil
}
