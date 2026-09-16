package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// These tests run against the real filesystem in a temporary directory. No
// filesystem is substituted: the distinction between an unavailable root and
// an empty one only means something if the real syscalls produce it.

func TestProbeDistinguishesEmptyFromUnavailable(t *testing.T) {
	base := t.TempDir()

	empty := filepath.Join(base, "empty")
	mustMkdir(t, empty, 0o770)

	populated := filepath.Join(base, "populated")
	mustMkdir(t, populated, 0o770)
	mustWrite(t, filepath.Join(populated, "synthetic.pdf"), "synthetic")

	missing := filepath.Join(base, "never-mounted")

	notDir := filepath.Join(base, "file-where-a-mount-belongs")
	mustWrite(t, notDir, "not a directory")

	report := Probe([]Root{
		{Role: "empty", Path: empty, WriteRequired: true},
		{Role: "populated", Path: populated, WriteRequired: true},
		{Role: "missing", Path: missing, WriteRequired: true},
		{Role: "notdir", Path: notDir, WriteRequired: true},
	})

	got := statusByRole(report)

	if got["empty"] != StatusEmpty {
		t.Errorf("empty directory reported %q", got["empty"])
	}
	if got["populated"] != StatusOK {
		t.Errorf("populated directory reported %q", got["populated"])
	}
	if got["missing"] != StatusMissing {
		t.Errorf("absent path reported %q", got["missing"])
	}
	if got["notdir"] != StatusNotADirectory {
		t.Errorf("a file reported %q", got["notdir"])
	}

	// The core requirement: an unmounted root must never be readable as file
	// absence, so the two statuses cannot coincide and only one is healthy.
	if got["missing"] == got["empty"] {
		t.Fatal("an unmounted root and an empty directory share a status")
	}
	if got["missing"].Available() {
		t.Error("an unmounted root reports as available")
	}
	if !got["empty"].Available() {
		t.Error("an empty but healthy directory reports as unavailable")
	}
	if report.Available() {
		t.Error("a report containing unavailable roles claims availability")
	}
	if len(report.Unavailable()) != 2 {
		t.Errorf("expected two unavailable roles, got %d", len(report.Unavailable()))
	}
}

func TestProbeReportsAvailableWhenEveryRoleIsHealthy(t *testing.T) {
	base := t.TempDir()
	a := filepath.Join(base, "a")
	b := filepath.Join(base, "b")
	mustMkdir(t, a, 0o770)
	mustMkdir(t, b, 0o770)
	mustWrite(t, filepath.Join(b, "x"), "x")

	report := Probe([]Root{
		{Role: "a", Path: a, WriteRequired: true},
		{Role: "b", Path: b, WriteRequired: true},
	})
	if !report.Available() {
		t.Errorf("report should be available: %+v", report.Results)
	}
	if len(report.Unavailable()) != 0 {
		t.Errorf("unexpected unavailable roles: %+v", report.Unavailable())
	}
}

func TestEmptyRootListIsNotAvailable(t *testing.T) {
	// Probing nothing must not read as "everything is fine".
	if Probe(nil).Available() {
		t.Error("an empty report claims availability")
	}
}

func TestReadOnlyRoleIsNotReportedUnwritable(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "read-only-role")
	mustMkdir(t, dir, 0o500)
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	// A role this process only reads is intended least privilege, not a fault.
	report := Probe([]Root{{Role: "consume", Path: dir, WriteRequired: false}})
	if !report.Available() {
		t.Errorf("a read-only role that is only read reported unavailable: %+v", report.Results)
	}

	if os.Geteuid() == 0 {
		t.Skip("running as uid 0 bypasses permission bits, so the write fixture cannot be built here")
	}
	report = Probe([]Root{{Role: "queued", Path: dir, WriteRequired: true}})
	if report.Available() {
		t.Error("a role that must be written reported available on a read-only directory")
	}
	if got := statusByRole(report)["queued"]; got != StatusNotWritable {
		t.Errorf("status = %q, want %q", got, StatusNotWritable)
	}
}

func TestPermissionDeniedIsItsOwnStatus(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as uid 0 bypasses permission bits, so the fixture cannot be built here")
	}
	base := t.TempDir()
	dir := filepath.Join(base, "denied")
	mustMkdir(t, dir, 0o000)
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	report := Probe([]Root{{Role: "incoming", Path: dir, WriteRequired: false}})
	if got := statusByRole(report)["incoming"]; got != StatusPermissionDenied {
		t.Errorf("status = %q, want %q", got, StatusPermissionDenied)
	}
	if report.Available() {
		t.Error("an unreadable root reports as available")
	}
}

func TestSymlinkedRootIsResolvedAndValidated(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "real")
	mustMkdir(t, target, 0o770)
	link := filepath.Join(base, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	report := Probe([]Root{{Role: "incoming", Path: link, WriteRequired: true}})
	if got := statusByRole(report)["incoming"]; got != StatusEmpty {
		t.Errorf("a symlink to an empty directory reported %q", got)
	}

	// A dangling link points at nothing: that is unavailable storage, not an
	// empty directory.
	dangling := filepath.Join(base, "dangling")
	if err := os.Symlink(filepath.Join(base, "gone"), dangling); err != nil {
		t.Fatal(err)
	}
	report = Probe([]Root{{Role: "incoming", Path: dangling, WriteRequired: true}})
	if got := statusByRole(report)["incoming"]; got != StatusMissing {
		t.Errorf("a dangling symlink reported %q, want %q", got, StatusMissing)
	}

	// A link pointing at a file is an incorrect mount, not a directory.
	file := filepath.Join(base, "afile")
	mustWrite(t, file, "x")
	toFile := filepath.Join(base, "to-file")
	if err := os.Symlink(file, toFile); err != nil {
		t.Fatal(err)
	}
	report = Probe([]Root{{Role: "incoming", Path: toFile, WriteRequired: true}})
	if got := statusByRole(report)["incoming"]; got != StatusNotADirectory {
		t.Errorf("a symlink to a file reported %q, want %q", got, StatusNotADirectory)
	}
}

func TestProbeDoesNotCreateRoots(t *testing.T) {
	base := t.TempDir()
	missing := filepath.Join(base, "should-not-be-created")

	Probe([]Root{{Role: "incoming", Path: missing, WriteRequired: true}})

	// Creating a missing root would mask an incorrect mount by making the
	// stack look healthy while pointing at local container storage.
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Errorf("the probe created %s", missing)
	}
}

func TestProbeLeavesNoArtefacts(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "root")
	mustMkdir(t, dir, 0o770)

	Probe([]Root{{Role: "queued", Path: dir, WriteRequired: true}})

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	// The write check must clean up after itself, or it would be indexed by a
	// later discovery pass as a document.
	if len(entries) != 0 {
		t.Errorf("the probe left %d entries behind: %v", len(entries), entries)
	}
}

func TestStatusesAreStable(t *testing.T) {
	// Status values are metric label values, so the strings are part of the
	// exposed contract.
	for status, want := range map[Status]string{
		StatusOK: "ok", StatusEmpty: "empty", StatusMissing: "missing",
		StatusNotADirectory: "not_a_directory", StatusPermissionDenied: "permission_denied",
		StatusNotWritable: "not_writable", StatusProbeError: "probe_error",
	} {
		if string(status) != want {
			t.Errorf("status %v rendered as %q, want %q", status, string(status), want)
		}
	}
}

func statusByRole(r Report) map[string]Status {
	out := make(map[string]Status, len(r.Results))
	for _, res := range r.Results {
		out[res.Role] = res.Status
	}
	return out
}

func mustMkdir(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(path, mode); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
}

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o640); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
