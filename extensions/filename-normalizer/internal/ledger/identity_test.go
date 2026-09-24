package ledger

import (
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
)

// The source-identity rules, as pure functions of a job row and an observed
// file. The ledger queries that apply the same rule are covered by the
// integration suite against the real database.

func registered(inode, device int64, birth *time.Time) Job {
	size := int64(42)
	mod := time.Date(2026, 9, 24, 10, 0, 0, 123456000, time.UTC)
	return Job{
		SizeBytes: &size, SourceInode: &inode, SourceDevice: &device,
		SourceModifiedAt: &mod, SourceBirthTime: birth,
	}
}

func observed(inode, device uint64, birth time.Time, known bool) storage.Entry {
	return storage.Entry{
		Size: 42, ModTime: time.Date(2026, 9, 24, 10, 0, 0, 123456789, time.UTC),
		Inode: inode, Device: device, Btime: birth, BtimeKnown: known,
	}
}

// A NAS share is remounted and gets a new device number. The file in the drop
// folder is the same file; calling it mutated would hold every job whose
// renamer reached it through a different mount.
func TestTheBirthTimeStandsInForTheDevice(t *testing.T) {
	born := time.Date(2026, 9, 24, 9, 59, 0, 987654321, time.UTC)
	stored := born.Truncate(time.Microsecond)
	job := registered(1001, 51, &stored)

	if !job.RegisteredSourceIs(observed(1001, 77, born, true)) {
		t.Error("the same file seen through a different mount was not recognised")
	}
	if !job.RegisteredFileIs(observed(1001, 77, born, true)) {
		t.Error("the same file seen through a different mount was not recognised as the file")
	}
}

// ext4 hands a freed inode number straight back. A different file on the reused
// number is told apart by its birth time.
func TestAReusedInodeIsADifferentFile(t *testing.T) {
	born := time.Date(2026, 9, 24, 9, 59, 0, 0, time.UTC)
	job := registered(1001, 51, &born)
	later := born.Add(24 * time.Hour)

	if job.RegisteredSourceIs(observed(1001, 51, later, true)) {
		t.Error("a new file on a reused inode passed as the registered source")
	}
	if job.RegisteredFileIs(observed(1001, 51, later, true)) {
		t.Error("a new file on a reused inode passed as the registered file")
	}
}

// Rows from before birth times were recorded, and filesystems that report
// none, keep the device rule.
func TestWithoutABirthTimeTheDeviceStillDecides(t *testing.T) {
	legacy := registered(1001, 51, nil)
	born := time.Now()

	if !legacy.RegisteredSourceIs(observed(1001, 51, born, true)) {
		t.Error("a legacy row did not recognise its own file")
	}
	if legacy.RegisteredSourceIs(observed(1001, 52, born, true)) {
		t.Error("a legacy row accepted a file on another device")
	}
	stored := born.Truncate(time.Microsecond)
	modern := registered(1001, 51, &stored)
	if modern.RegisteredFileIs(observed(1001, 52, time.Time{}, false)) {
		t.Error("an observation without a birth time was accepted on another device")
	}
}

// Same file, changed contents: RegisteredFileIs still says yes, and
// RegisteredSourceIs says no. The archive step needs the difference.
func TestAChangedFileIsStillTheSameFile(t *testing.T) {
	born := time.Date(2026, 9, 24, 9, 59, 0, 0, time.UTC)
	job := registered(1001, 51, &born)
	changed := observed(1001, 51, born, true)
	changed.Size = 43

	if !job.RegisteredFileIs(changed) {
		t.Error("a changed file was not recognised as the registered file")
	}
	if job.RegisteredSourceIs(changed) {
		t.Error("a changed file passed as the unchanged registered source")
	}
}
