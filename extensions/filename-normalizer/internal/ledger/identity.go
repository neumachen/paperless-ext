package ledger

import (
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
)

// RegisteredSourceIs reports whether an observed file is the source discovery
// registered for this job, unchanged.
//
// Size and modification time must agree, and so must the identity: the inode,
// and then the birth time wherever both sides know it, the device otherwise.
//
// The birth time stands in for the device for the reason AlreadyRegistered
// gives. The device names the mount a file was reached through, not the file,
// and an SMB mount gets a new one every time it is mounted: a renamer reaching
// a NAS drop folder through its own mount, or any process after the share was
// remounted, sees a different device for the very file the watcher registered.
// Comparing it would call every such source mutated. The birth time is a
// property of the file, and it is what tells a replacement that reused the
// inode number from the original.
func (j Job) RegisteredSourceIs(e storage.Entry) bool {
	if j.SizeBytes != nil && *j.SizeBytes != e.Size {
		return false
	}
	if j.SourceInode != nil && *j.SourceInode != int64(e.Inode) {
		return false
	}
	if j.SourceBirthTime != nil && e.BtimeKnown {
		// PostgreSQL keeps microseconds; the kernel reports nanoseconds.
		if !j.SourceBirthTime.Truncate(time.Microsecond).Equal(e.Btime.Truncate(time.Microsecond)) {
			return false
		}
	} else if j.SourceDevice != nil && *j.SourceDevice != int64(e.Device) {
		return false
	}
	// PostgreSQL stores timestamptz at microsecond resolution, so the value
	// read back is a truncation of the nanosecond modification time that was
	// written. Comparing them directly reports every source as mutated.
	if j.SourceModifiedAt != nil &&
		!j.SourceModifiedAt.Truncate(time.Microsecond).Equal(e.ModTime.Truncate(time.Microsecond)) {
		return false
	}
	return true
}

// RegisteredFileIs reports whether an observed file is the file discovery
// registered for this job, whatever has happened to its contents since.
//
// It is RegisteredSourceIs without the size and modification time: the
// question "is this still the same file?" rather than "is it unchanged?". The
// archive step needs the two apart, because they call for different answers:
// a different file at the name means this job's original is gone, and a
// changed one means it is still here but is no longer what was delivered.
func (j Job) RegisteredFileIs(e storage.Entry) bool {
	if j.SourceInode == nil || *j.SourceInode != int64(e.Inode) {
		return false
	}
	if j.SourceBirthTime != nil && e.BtimeKnown {
		return j.SourceBirthTime.Truncate(time.Microsecond).Equal(e.Btime.Truncate(time.Microsecond))
	}
	return j.SourceDevice != nil && *j.SourceDevice == int64(e.Device)
}
