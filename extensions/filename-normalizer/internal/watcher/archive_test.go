package watcher

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

const testJobID = "3f2a1b4c-5d6e-4f70-8a9b-0c1d2e3f4a5b"

func TestArchiveCandidatesKeepTheNameAndTagCollisionsWithTheJob(t *testing.T) {
	got := archiveCandidates("scan.pdf", testJobID)
	want := []string{"scan.pdf", "scan.3f2a1b4c.pdf", "scan." + testJobID + ".pdf"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("candidates %q, want %q", got, want)
	}
}

func TestArchiveCandidatesForNamesWithoutAnOrdinaryExtension(t *testing.T) {
	for name, second := range map[string]string{
		"README":            "README.3f2a1b4c",
		"archive.tar.gz":    "archive.tar.3f2a1b4c.gz",
		"Rechnung März.PDF": "Rechnung März.3f2a1b4c.PDF",
	} {
		got := archiveCandidates(name, testJobID)
		if got[0] != name {
			t.Errorf("%q: first candidate %q, want the name itself", name, got[0])
		}
		if got[1] != second {
			t.Errorf("%q: second candidate %q, want %q", name, got[1], second)
		}
	}
}

// Every candidate must be a usable directory entry: at most 255 bytes, valid
// UTF-8, and distinct from the others.
func TestArchiveCandidatesFitADirectoryEntry(t *testing.T) {
	for _, name := range []string{
		strings.Repeat("a", 251) + ".pdf",
		strings.Repeat("ü", 125) + ".pdf",
		"x." + strings.Repeat("e", 200),
		strings.Repeat("日", 83) + ".tiff",
	} {
		got := archiveCandidates(name, testJobID)
		seen := map[string]bool{}
		for _, c := range got[1:] {
			if len(c) > maxArchiveName {
				t.Errorf("%d-byte name: candidate is %d bytes", len(name), len(c))
			}
			if !utf8.ValidString(c) {
				t.Errorf("%d-byte name: candidate %q is not valid UTF-8", len(name), c)
			}
			if !strings.Contains(c, "3f2a1b4c") {
				t.Errorf("%d-byte name: candidate %q lost the job tag", len(name), c)
			}
			if seen[c] {
				t.Errorf("%d-byte name: duplicate candidate %q", len(name), c)
			}
			seen[c] = true
		}
	}
}

func TestArchiveBackoffDoublesUpToAnHour(t *testing.T) {
	for attempts, want := range map[int]time.Duration{
		0:  10 * time.Second,
		1:  20 * time.Second,
		3:  80 * time.Second,
		20: time.Hour,
	} {
		if got := archiveBackoff(10*time.Second, attempts); got != want {
			t.Errorf("attempts %d: %s, want %s", attempts, got, want)
		}
	}
}
