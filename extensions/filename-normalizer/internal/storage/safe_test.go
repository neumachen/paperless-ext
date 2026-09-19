package storage

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Regression tests for the third review round's filesystem findings, against
// a real temporary filesystem.

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// FN-N005: eligibility was checked with Lstat on a pathname, and the bytes
// were read later by re-opening the same pathname.
//
// Before: replacing the checked file with a symlink between the two operations
// caused a read of whatever the link pointed at -- outside the authorized root
// -- because the later open followed it. The check and the read must be the
// same file.
func TestReadingFollowsNoSymlinkPlantedAfterTheCheck(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.pdf")
	writeFile(t, outside, "OUTSIDE THE ROOT")

	name := "doc.pdf"
	path := filepath.Join(root, name)
	writeFile(t, path, "inside the root")

	// The check happens now.
	entry, err := Inspect(root, name)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}

	// The name is repointed at a symlink leading outside the root.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Skipf("symlinks are not available here: %v", err)
	}

	// A plain fingerprint of the pathname must refuse the symlink outright.
	if _, _, err := Fingerprint(path); !errors.Is(err, ErrSymlink) {
		t.Errorf("Fingerprint followed a planted symlink (err=%v)", err)
	}

	// And the identity-checked open must refuse it as a changed file.
	f, _, err := OpenExpected(root, name, entry, false)
	if err == nil {
		f.Close()
		t.Fatal("OpenExpected accepted a name that now leads to a different file")
	}
	if !errors.Is(err, ErrSymlink) && !errors.Is(err, ErrMutated) {
		t.Errorf("err = %v, want a symlink or mutation refusal", err)
	}
}

// FN-N005: replacing the checked file with a FIFO could block processing
// forever, because the later open had no O_NONBLOCK and a FIFO's open waits
// for a writer.
func TestOpeningAFifoDoesNotBlockAndIsRefused(t *testing.T) {
	root := t.TempDir()
	name := "pipe.pdf"
	path := filepath.Join(root, name)
	if err := syscall.Mkfifo(path, 0o644); err != nil {
		t.Skipf("named pipes are not available here: %v", err)
	}

	// The bound matters as much as the error: a blocking open would hang the
	// worker rather than fail it.
	done := make(chan error, 1)
	go func() {
		_, _, err := Fingerprint(path)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrNotRegular) {
			t.Errorf("err = %v, want a not-a-regular-file refusal", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("opening a FIFO blocked; a single special file would stall processing")
	}
}

// FN-N005: a source replaced by a different regular file between discovery
// and the read must be refused rather than silently read.
func TestOpenExpectedRefusesAReplacedFile(t *testing.T) {
	root := t.TempDir()
	name := "doc.pdf"
	path := filepath.Join(root, name)
	writeFile(t, path, "original")

	entry, err := Inspect(root, name)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}

	// Replaced by a different file of the same length, so size alone would
	// not notice.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	writeFile(t, path, "replaced")

	f, _, err := OpenExpected(root, name, entry, false)
	if err == nil {
		f.Close()
		t.Fatal("OpenExpected accepted a replaced file")
	}
	if !errors.Is(err, ErrMutated) {
		t.Errorf("err = %v, want ErrMutated", err)
	}
}

// TestOpenExpectedAcceptsTheSameFile is the other direction: the check must
// not reject an unchanged file, or nothing would ever be processed.
func TestOpenExpectedAcceptsTheSameFile(t *testing.T) {
	root := t.TempDir()
	name := "doc.pdf"
	writeFile(t, filepath.Join(root, name), "stable")

	entry, err := Inspect(root, name)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	f, got, err := OpenExpected(root, name, entry, false)
	if err != nil {
		t.Fatalf("OpenExpected on an unchanged file: %v", err)
	}
	defer f.Close()
	if got.Inode != entry.Inode {
		t.Errorf("identity changed without the file changing")
	}
	sum, n, err := FingerprintFile(f)
	if err != nil || n != int64(len("stable")) || len(sum) != 32 {
		t.Errorf("reading the opened descriptor failed: n=%d err=%v", n, err)
	}
}

// FN-N005: two roots that resolve to one directory must be detectable.
func TestDistinctRootsDetectsAnAlias(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	alias := filepath.Join(base, "alias")
	if err := os.MkdirAll(real, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Symlink(real, alias); err != nil {
		t.Skipf("symlinks are not available here: %v", err)
	}

	a, err := IdentifyRoot("staging", real)
	if err != nil {
		t.Fatalf("identify: %v", err)
	}
	b, err := IdentifyRoot("consume", alias)
	if err != nil {
		t.Fatalf("identify: %v", err)
	}
	if err := DistinctRoots([]RootID{a, b}); err == nil {
		t.Error("two spellings of one directory were accepted as distinct roots")
	}

	// Genuinely different directories must still be accepted.
	other := filepath.Join(base, "other")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	c, err := IdentifyRoot("consume", other)
	if err != nil {
		t.Fatalf("identify: %v", err)
	}
	if err := DistinctRoots([]RootID{a, c}); err != nil {
		t.Errorf("two different directories were rejected: %v", err)
	}
}

// FN-N005: a root that is replaced after validation must be caught before it
// is used again.
func TestRootIdentityDetectsAReplacedRoot(t *testing.T) {
	base := t.TempDir()
	link := filepath.Join(base, "root")
	first := filepath.Join(base, "first")
	second := filepath.Join(base, "second")
	for _, d := range []string{first, second} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	if err := os.Symlink(first, link); err != nil {
		t.Skipf("symlinks are not available here: %v", err)
	}

	id, err := IdentifyRoot("staging", link)
	if err != nil {
		t.Fatalf("identify: %v", err)
	}
	if err := id.Verify(); err != nil {
		t.Fatalf("an unchanged root failed verification: %v", err)
	}

	// Repoint the root at a different directory, as a remount would.
	if err := os.Remove(link); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := os.Symlink(second, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := id.Verify(); err == nil {
		t.Error("a root repointed at a different directory passed verification")
	} else if !errors.Is(err, ErrRootChanged) {
		t.Errorf("err = %v, want ErrRootChanged", err)
	}
}

// FN-N007: recursive discovery yields relative subpaths, which the
// non-recursive join refuses by design. The subpath-aware join must accept a
// contained subpath and still refuse traversal.
func TestSafeJoinRelAcceptsContainedSubpathsAndRefusesEscapes(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sub", "deeper"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	got, err := SafeJoinRel(root, "sub/deeper/doc.pdf")
	if err != nil {
		t.Fatalf("SafeJoinRel on a contained subpath: %v", err)
	}
	if filepath.Base(got) != "doc.pdf" {
		t.Errorf("joined to %q", got)
	}

	for _, bad := range []string{
		"../escape.pdf", "sub/../../escape.pdf", "/absolute.pdf",
		"sub//doc.pdf", "sub/./doc.pdf", "sub/../doc.pdf",
	} {
		if _, err := SafeJoinRel(root, bad); err == nil {
			t.Errorf("SafeJoinRel accepted %q", bad)
		}
	}

	// And the non-recursive form still refuses any separator at all.
	if _, err := SafeJoin(root, "sub/doc.pdf"); err == nil {
		t.Error("SafeJoin accepted a subpath")
	}
}

// FN-N004: publication must not follow a symlink planted at the destination.
func TestFingerprintRefusesASymlinkedDestination(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "source.pdf")
	writeFile(t, target, "the source bytes")

	link := filepath.Join(dir, "destination.pdf")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks are not available here: %v", err)
	}

	// Following the link would report the source's own fingerprint, which is
	// exactly the content the job is about -- so a content comparison would
	// "match" and the job would adopt a symlink as its delivery.
	if _, _, err := Fingerprint(link); !errors.Is(err, ErrSymlink) {
		t.Errorf("Fingerprint followed a symlinked destination (err=%v)", err)
	}
}

// ---------------------------------------------------------------------------
// FN-R5-02 (R7): cleanup must not delete or overwrite a foreign entry, and the
// rollback of a failed write must remove the FILE it created, not the NAME it
// used.
// ---------------------------------------------------------------------------

// A failed CreateFrom takes its own file away again.
func TestCreateFromRollbackRemovesItsOwnFile(t *testing.T) {
	root := t.TempDir()
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	defer dir.Close()

	// A closed source fails at the first Seek, after the entry has been
	// created exclusively -- which is the rollback path under test.
	srcPath := filepath.Join(t.TempDir(), "src")
	writeFile(t, srcPath, "hello")
	src, err := os.Open(srcPath)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	_ = src.Close()

	if _, _, _, _, err := dir.CreateFrom(src, "target", 0o640); err == nil {
		t.Fatal("CreateFrom must fail with a closed source")
	}
	if _, err := os.Lstat(filepath.Join(root, "target")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the rolled-back file is still there: %v", err)
	}
}

// A rollback whose name now resolves to somebody else's file leaves it alone.
//
// The interval this closes is between the exclusive create and the failure, so
// the replacement is staged directly: what is asserted is the rule the rollback
// applies -- remove only while the name is still this file -- not the timing.
func TestCreateFromRollbackLeavesAReplacementAlone(t *testing.T) {
	root := t.TempDir()
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	defer dir.Close()

	writeFile(t, filepath.Join(root, "occupied"), "a stranger's bytes")
	stranger, err := dir.Identify("occupied")
	if err != nil {
		t.Fatalf("identify: %v", err)
	}

	// Some other file's identity, standing in for the one CreateFrom recorded
	// before the entry was swapped.
	writeFile(t, filepath.Join(root, "ours"), "ours")
	ours, err := dir.Identify("ours")
	if err != nil {
		t.Fatalf("identify: %v", err)
	}

	removed, err := dir.RemoveOwned("occupied", ours)
	if removed {
		t.Error("a file this caller does not own was reported as removed")
	}
	if !errors.Is(err, ErrMutated) {
		t.Errorf("expected ErrMutated, got %v", err)
	}
	got, err := dir.Identify("occupied")
	if err != nil {
		t.Fatalf("the stranger's file is gone: %v", err)
	}
	if got.Inode != stranger.Inode || got.Device != stranger.Device {
		t.Error("the stranger's file was replaced")
	}
	body, err := os.ReadFile(filepath.Join(root, "occupied"))
	if err != nil || string(body) != "a stranger's bytes" {
		t.Errorf("the stranger's bytes changed: %q %v", body, err)
	}
}

// The ordinary case still works and still reports accurately: one owned hard
// link removed while other legitimate links to the same inode remain.
func TestRemoveOwnedStillReportsAnOrdinaryOwnedLinkRemoval(t *testing.T) {
	root := t.TempDir()
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	defer dir.Close()

	writeFile(t, filepath.Join(root, "published"), "one document")
	if err := os.Link(filepath.Join(root, "published"), filepath.Join(root, ".fn-staged")); err != nil {
		t.Fatalf("link: %v", err)
	}
	staged, err := dir.Identify(".fn-staged")
	if err != nil {
		t.Fatalf("identify: %v", err)
	}

	removed, err := dir.RemoveOwned(".fn-staged", staged)
	if err != nil {
		t.Fatalf("removing an owned link failed: %v", err)
	}
	if !removed {
		t.Error("an owned link that was removed must be reported as removed")
	}
	if _, err := os.Lstat(filepath.Join(root, "published")); err != nil {
		t.Errorf("the published document was removed with its staged link: %v", err)
	}
}

// renameatNoReplace refuses rather than destroying an occupant.
func TestRenameNoReplaceRefusesAnOccupiedName(t *testing.T) {
	root := t.TempDir()
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	defer dir.Close()

	writeFile(t, filepath.Join(root, "from"), "moving")
	writeFile(t, filepath.Join(root, "to"), "already here")

	err = renameatNoReplace(int(dir.f.Fd()), "from", int(dir.f.Fd()), "to")
	if !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("expected EEXIST, got %v", err)
	}
	body, rerr := os.ReadFile(filepath.Join(root, "to"))
	if rerr != nil || string(body) != "already here" {
		t.Errorf("the occupant was overwritten: %q %v", body, rerr)
	}
}
