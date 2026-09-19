package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The bytes that are published must be the bytes that were verified, even when
// the temporary's NAME is repointed after the verification.
//
// This is the window the previous design left open: the staged temporary was
// opened, hashed, closed, and then linked by pathname. Replacing the name in
// between published a file nobody had read, under a receipt describing content
// that was never at the destination. The replacement here happens exactly
// there -- after the descriptor is verified and hashed, before the link -- and
// the published file must still be the verified one.
func TestPublicationLinksTheVerifiedFileNotTheName(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".fn-tmp"), []byte("the verified bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	defer dir.Close()

	staged, err := dir.Identify(".fn-tmp")
	if err != nil {
		t.Fatalf("Identify: %v", err)
	}
	f, _, err := dir.OpenOwn(".fn-tmp", staged)
	if err != nil {
		t.Fatalf("OpenOwn: %v", err)
	}
	defer f.Close()
	sum, _, err := FingerprintFile(f)
	if err != nil {
		t.Fatalf("FingerprintFile: %v", err)
	}

	// The name now leads somewhere else entirely. The verified inode is kept
	// alive under a second link, which is what makes this the interesting case:
	// the file still exists, and the only question is whether publication
	// follows the descriptor it verified or the name it verified through.
	if err := os.Link(filepath.Join(root, ".fn-tmp"), filepath.Join(root, ".fn-keep")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, ".fn-tmp")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".fn-tmp"), []byte("substituted"), 0o600); err != nil {
		t.Fatal(err)
	}

	if published, err := PublishFromDescriptor(dir, f, ".fn-tmp", "doc.pdf"); err != nil || !published {
		t.Fatalf("PublishFromDescriptor: published=%v err=%v", published, err)
	}

	got, err := os.ReadFile(filepath.Join(root, "doc.pdf"))
	if err != nil {
		t.Fatalf("read the published document: %v", err)
	}
	if string(got) != "the verified bytes" {
		t.Fatalf("published %q, want the verified bytes", got)
	}
	// And its digest is the one the receipt would carry.
	again, _, err := Fingerprint(filepath.Join(root, "doc.pdf"))
	if err != nil {
		t.Fatal(err)
	}
	if HexFingerprint(again) != HexFingerprint(sum) {
		t.Fatal("the published document does not match the verified digest")
	}
}

// Publication never overwrites: the kernel decides, through EEXIST.
func TestPublicationRefusesAnOccupiedName(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".fn-tmp"), []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "doc.pdf"), []byte("someone else's"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	staged, _ := dir.Identify(".fn-tmp")
	f, _, err := dir.OpenOwn(".fn-tmp", staged)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	published, err := PublishFromDescriptor(dir, f, ".fn-tmp", "doc.pdf")
	if published || !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("published=%v err=%v, want ErrDestinationExists", published, err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, "doc.pdf")); string(got) != "someone else's" {
		t.Fatalf("the occupant was changed: %q", got)
	}
}

// Cleanup removes what it owns and refuses a replacement.
//
// The old form identified a pathname and then removed that pathname, so a file
// that took the name in between was deleted by a cleanup that believed it was
// removing its own. Here the name is repointed after the identity is taken, and
// the foreign file must survive.
func TestRemoveOwnedLeavesAReplacementAlone(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "doc.pdf"), []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	mine, err := dir.Identify("doc.pdf")
	if err != nil {
		t.Fatal(err)
	}

	// Somebody replaces it between the identification and the removal.
	if err := os.Remove(filepath.Join(root, "doc.pdf")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "doc.pdf"), []byte("not yours"), 0o600); err != nil {
		t.Fatal(err)
	}

	removed, err := dir.RemoveOwned("doc.pdf", mine)
	if removed {
		t.Fatal("a file this caller does not own was removed")
	}
	if !errors.Is(err, ErrMutated) {
		t.Fatalf("err = %v, want ErrMutated", err)
	}
	if got, rerr := os.ReadFile(filepath.Join(root, "doc.pdf")); rerr != nil || string(got) != "not yours" {
		t.Fatalf("the replacement did not survive: %q %v", got, rerr)
	}
}

// The ordinary case still removes the file.
func TestRemoveOwnedRemovesItsOwnFile(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "doc.pdf"), []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	mine, _ := dir.Identify("doc.pdf")

	removed, err := dir.RemoveOwned("doc.pdf", mine)
	if err != nil || !removed {
		t.Fatalf("removed=%v err=%v", removed, err)
	}
	if _, serr := os.Stat(filepath.Join(root, "doc.pdf")); !os.IsNotExist(serr) {
		t.Fatal("the file is still there")
	}
}

// A root replaced after it was accepted cannot redirect a CREATE either.
//
// Root identity was already checked on opens. Creation, linking and removal
// went through pathnames, so the operations that write were the ones the
// verification did not cover.
func TestOpenDirRefusesARootReplacedAfterValidation(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	base := t.TempDir()
	root := filepath.Join(base, "consume")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	id, err := IdentifyRoot("consume", root)
	if err != nil {
		t.Fatal(err)
	}
	ExpectRoot(id)

	if d, err := OpenDir(root); err != nil {
		t.Fatalf("OpenDir on the accepted root: %v", err)
	} else {
		d.Close()
	}

	impostor := filepath.Join(base, "impostor")
	if err := os.Mkdir(impostor, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(root, filepath.Join(base, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(impostor, root); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenDir(root); !errors.Is(err, ErrRootChanged) {
		t.Fatalf("OpenDir after replacement: err = %v, want ErrRootChanged", err)
	}
}

// When the verified file is gone entirely, publication FAILS rather than
// publishing whatever now answers to its name.
//
// linkat from the descriptor cannot resurrect an inode with no links left, so
// this direction is closed by construction: the operation errors and nothing
// appears at the destination. That is the outcome the contract wants -- only
// verified bytes become visible -- and it is worth asserting, because the
// pathname form would happily have published the substitute.
func TestPublicationFailsRatherThanPublishingASubstitute(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".fn-tmp"), []byte("the verified bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	staged, _ := dir.Identify(".fn-tmp")
	f, _, err := dir.OpenOwn(".fn-tmp", staged)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := os.Remove(filepath.Join(root, ".fn-tmp")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".fn-tmp"), []byte("substituted"), 0o600); err != nil {
		t.Fatal(err)
	}

	published, perr := PublishFromDescriptor(dir, f, ".fn-tmp", "doc.pdf")
	if published || perr == nil {
		t.Fatalf("published=%v err=%v: a substitute must not be publishable", published, perr)
	}
	if _, serr := os.Stat(filepath.Join(root, "doc.pdf")); !os.IsNotExist(serr) {
		t.Fatal("something was published at the destination")
	}
}

// Removing one owned hard link is a removal, not a mishap.
//
// The staged temporary and the published document are hard links to ONE inode:
// that is what publication does. Cleanup then removes the temporary and the
// inode still has a link, which the previous implementation read as "a file
// this process does not own was removed" -- so every successful publication
// reported a failed cleanup. The count of links says nothing about whose file
// it is.
func TestRemovingOneOwnedLinkOfSeveralIsReportedAsRemoved(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "doc.pdf"), []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "doc.pdf"), filepath.Join(root, ".fn-tmp")); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	staged, err := dir.Identify(".fn-tmp")
	if err != nil {
		t.Fatal(err)
	}

	removed, err := dir.RemoveOwned(".fn-tmp", staged)
	if err != nil || !removed {
		t.Fatalf("removed=%v err=%v, want true/nil: one owned link of two was removed", removed, err)
	}
	if _, serr := os.Stat(filepath.Join(root, ".fn-tmp")); !os.IsNotExist(serr) {
		t.Fatal("the temporary is still there")
	}
	if got, rerr := os.ReadFile(filepath.Join(root, "doc.pdf")); rerr != nil || string(got) != "mine" {
		t.Fatalf("the published document did not survive: %q %v", got, rerr)
	}
}

// A removal leaves no intermediate behind.
func TestRemoveOwnedLeavesNoTombstone(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "doc.pdf"), []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	mine, _ := dir.Identify("doc.pdf")
	if removed, rerr := dir.RemoveOwned("doc.pdf", mine); !removed || rerr != nil {
		t.Fatalf("removed=%v err=%v", removed, rerr)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), tempPrefixRemoval) {
			t.Fatalf("a removal intermediate was left behind: %s", e.Name())
		}
	}
	if len(entries) != 0 {
		t.Fatalf("directory is not empty: %v", entries)
	}
}

// A name that is already gone is not an error and not a removal.
func TestRemoveOwnedOnAnAbsentNameIsNeitherErrorNorRemoval(t *testing.T) {
	ForgetRoots()
	defer ForgetRoots()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "doc.pdf"), []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	mine, _ := dir.Identify("doc.pdf")
	if err := os.Remove(filepath.Join(root, "doc.pdf")); err != nil {
		t.Fatal(err)
	}
	removed, rerr := dir.RemoveOwned("doc.pdf", mine)
	if removed || rerr != nil {
		t.Fatalf("removed=%v err=%v, want false/nil for a name that is already gone", removed, rerr)
	}
}
