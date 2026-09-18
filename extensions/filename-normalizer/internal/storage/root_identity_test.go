package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A root that is repointed AFTER it was validated must be caught by the
// operation it would have redirected, not only by a separate check that ran
// earlier.
//
// Before: roots were verified on their own, before the work. Everything after
// that check reached the root by pathname, so replacing the directory in
// between redirected the reads and writes the check was supposed to protect,
// and no interval between checks is short enough to close that window.
func TestAnOpenRefusesARootThatWasReplacedAfterValidation(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	base := t.TempDir()
	root := filepath.Join(base, "incoming")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a.pdf"), []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}

	id, err := IdentifyRoot("incoming", root)
	if err != nil {
		t.Fatalf("identify: %v", err)
	}
	ExpectRoot(id)

	// It works while the root is the directory that was accepted.
	f, _, err := Open(root, "a.pdf", false)
	if err != nil {
		t.Fatalf("open before replacement: %v", err)
	}
	f.Close()

	// Now the same pathname leads somewhere else entirely.
	impostor := filepath.Join(base, "impostor")
	if err := os.Mkdir(impostor, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(impostor, "a.pdf"), []byte("substituted"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(root, filepath.Join(base, "incoming.moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(impostor, root); err != nil {
		t.Fatal(err)
	}

	if _, _, err := Open(root, "a.pdf", false); !errors.Is(err, ErrRootChanged) {
		t.Fatalf("open after replacement: err = %v, want ErrRootChanged", err)
	}
}

// A root nothing has accepted is not checked. The package is used before
// configuration has been validated, and inventing an expectation there would
// refuse legitimate work.
func TestAnUnregisteredRootIsNotChecked(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.pdf"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, _, err := Open(root, "a.pdf", false)
	if err != nil {
		t.Fatalf("open with no expectation recorded: %v", err)
	}
	f.Close()
}
