package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// These cover the move a delivered original takes out of the drop folder,
// against a real temporary filesystem. The properties that matter are the ones
// a person would lose a document over: nothing is replaced, and only the file
// the caller identified moves.

// archiveFixture is a drop folder with its archive directory inside it.
func archiveFixture(t *testing.T) (root string, src, dst *Dir) {
	t.Helper()
	root = t.TempDir()
	mustMkdir(t, filepath.Join(root, "processed"), 0o750)
	var err error
	if src, err = OpenDir(root); err != nil {
		t.Fatalf("open the drop folder: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	if dst, err = src.OpenSubdir("processed"); err != nil {
		t.Fatalf("open the archive directory: %v", err)
	}
	t.Cleanup(func() { _ = dst.Close() })
	return root, src, dst
}

func readBody(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func TestMoveOwnedMovesTheIdentifiedFileAndKeepsItsIdentity(t *testing.T) {
	root, src, dst := archiveFixture(t)
	writeFile(t, filepath.Join(root, "scan.pdf"), "the original")
	before, err := src.Identify("scan.pdf")
	if err != nil {
		t.Fatal(err)
	}

	moved, err := src.MoveOwned("scan.pdf", before, dst, "scan.pdf")
	if !moved || err != nil {
		t.Fatalf("moved=%v err=%v, want the original moved", moved, err)
	}
	if _, err := os.Lstat(filepath.Join(root, "scan.pdf")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the original is still in the drop folder: %v", err)
	}
	after, err := dst.Identify("scan.pdf")
	if err != nil {
		t.Fatalf("the original is not in the archive: %v", err)
	}
	// A move, not a copy: the archived file IS the original, which is what
	// lets a later pass recognise it by identity.
	if !SameIdentity(before, after) {
		t.Errorf("the archived file is a different file: %+v, was %+v", after, before)
	}
	if got := readBody(t, filepath.Join(root, "processed", "scan.pdf")); got != "the original" {
		t.Errorf("archived content %q", got)
	}
}

// A scanner writes scan.pdf every morning. Monday's archived original must
// survive Tuesday's being archived.
func TestMoveOwnedNeverReplacesAnArchivedOriginal(t *testing.T) {
	root, src, dst := archiveFixture(t)
	writeFile(t, filepath.Join(root, "processed", "scan.pdf"), "monday")
	writeFile(t, filepath.Join(root, "scan.pdf"), "tuesday")
	tuesday, err := src.Identify("scan.pdf")
	if err != nil {
		t.Fatal(err)
	}

	moved, err := src.MoveOwned("scan.pdf", tuesday, dst, "scan.pdf")
	if moved || !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("moved=%v err=%v, want ErrDestinationExists and nothing moved", moved, err)
	}
	if got := readBody(t, filepath.Join(root, "processed", "scan.pdf")); got != "monday" {
		t.Errorf("the archived original was replaced: %q", got)
	}
	if got := readBody(t, filepath.Join(root, "scan.pdf")); got != "tuesday" {
		t.Errorf("the new original was disturbed: %q", got)
	}
}

// The name was given to a different file after the caller identified it. That
// file is somebody's new submission and must stay exactly where it is.
func TestMoveOwnedLeavesAReplacementWhereItIs(t *testing.T) {
	root, src, dst := archiveFixture(t)
	path := filepath.Join(root, "scan.pdf")
	writeFile(t, path, "delivered yesterday")
	stale, err := src.Identify("scan.pdf")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	// A birth-time tick apart, so the replacement is distinguishable even on
	// a filesystem that hands the inode number straight back.
	time.Sleep(20 * time.Millisecond)
	writeFile(t, path, "dropped today")

	moved, err := src.MoveOwned("scan.pdf", stale, dst, "scan.pdf")
	if moved || !errors.Is(err, ErrMutated) {
		t.Fatalf("moved=%v err=%v, want ErrMutated and nothing moved", moved, err)
	}
	if got := readBody(t, path); got != "dropped today" {
		t.Errorf("the replacement was disturbed: %q", got)
	}
	if _, err := os.Lstat(filepath.Join(root, "processed", "scan.pdf")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("something reached the archive: %v", err)
	}
}

func TestMoveOwnedReportsAnAbsentOriginal(t *testing.T) {
	_, src, dst := archiveFixture(t)
	moved, err := src.MoveOwned("gone.pdf", Entry{Inode: 1}, dst, "gone.pdf")
	if moved || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("moved=%v err=%v, want fs.ErrNotExist", moved, err)
	}
}

func TestMoveOwnedRefusesNamesThatAreNotOneComponent(t *testing.T) {
	root, src, dst := archiveFixture(t)
	writeFile(t, filepath.Join(root, "scan.pdf"), "x")
	e, err := src.Identify("scan.pdf")
	if err != nil {
		t.Fatal(err)
	}
	for _, to := range []string{"../scan.pdf", "sub/scan.pdf", "..", ".", ""} {
		if moved, err := src.MoveOwned("scan.pdf", e, dst, to); moved || !errors.Is(err, ErrUnsafeName) {
			t.Errorf("to %q: moved=%v err=%v, want ErrUnsafeName", to, moved, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(root, "scan.pdf")); err != nil {
		t.Errorf("the original moved on a refused name: %v", err)
	}
}

func TestOpenSubdirNeverCreatesOrFollows(t *testing.T) {
	root := t.TempDir()
	d, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Missing is missing. Creating it would hide that the directory an
	// operator set up is not the one this process is looking at.
	if _, err := d.OpenSubdir("processed"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a missing directory: %v, want fs.ErrNotExist", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "processed")); !errors.Is(err, fs.ErrNotExist) {
		t.Error("OpenSubdir created the directory")
	}

	// A symlink out of the root is refused, not followed.
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.OpenSubdir("linked"); !errors.Is(err, ErrEscapesRoot) {
		t.Errorf("a symlinked directory: %v, want ErrEscapesRoot", err)
	}

	writeFile(t, filepath.Join(root, "afile"), "x")
	if _, err := d.OpenSubdir("afile"); !errors.Is(err, ErrNotDirectory) {
		t.Errorf("a file: %v, want ErrNotDirectory", err)
	}
	if _, err := d.OpenSubdir("../x"); !errors.Is(err, ErrUnsafeName) {
		t.Errorf("a path: %v, want ErrUnsafeName", err)
	}
}
