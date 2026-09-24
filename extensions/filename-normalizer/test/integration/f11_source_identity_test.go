//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
)

// A source's identity, as discovery and the ledger decide it: the inode, and
// the birth time where the filesystem reports one, instead of the device.

// TestSourceIdentityInTheLedger pins the identity rule in the database itself,
// where it does not depend on what the filesystem under the test volume
// happens to do with inode numbers.
//
//   - a new file on a reused inode, under a reused name, is NOT already
//     registered, and registering it succeeds: the device-keyed unique index
//     no longer refuses it.
//   - the same file seen through a new mount -- a new device, as every SMB
//     remount gives -- IS already registered, so it is not delivered twice.
//   - a row without a birth time keeps the device rule.
//   - each of the two identities is unique, over exactly its own rows.
func TestSourceIdentityInTheLedger(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// A root no process scans or reads: these rows exist for their identity
	// only, and a renamer that is handed one holds it as source_absent.
	root := filepath.Join(e.Cfg.Storage.Incoming, "identity-"+sanitizeForTemp(e.RunID))
	name := "scan.pdf"
	const inode, device, remounted = 424242, 51, 77
	monday := time.Date(2026, 9, 21, 7, 30, 0, 123456000, time.UTC)
	tuesday := monday.Add(24 * time.Hour)

	register := func(dev int64, birth *time.Time) error {
		size, algo := int64(1), storage.FingerprintAlgorithm
		ino, d := int64(inode), dev
		mod := time.Now()
		_, err := led.RegisterJob(ctx, ledger.RegisterInput{
			SourceRoot: root, SourceName: name, SizeBytes: &size, FingerprintAlgo: &algo,
			Fingerprint: []byte{1}, PolicyIdentity: e.Cfg.Policy.Identity,
			SourceInode: &ino, SourceDevice: &d, SourceModifiedAt: &mod, SourceBirthTime: birth,
			DestinationRoot: e.Cfg.Storage.Consume,
		})
		return err
	}
	known := func(dev uint64, birth *time.Time) bool {
		t.Helper()
		_, ok, err := led.AlreadyRegistered(ctx, root, name, inode, dev, birth)
		if err != nil {
			t.Fatalf("identity lookup: %v", err)
		}
		return ok
	}

	if err := register(device, &monday); err != nil {
		t.Fatalf("register monday's document: %v", err)
	}
	if !known(remounted, &monday) {
		t.Error("the same file seen through a new mount was not recognised; a remount would deliver it twice")
	}
	if known(device, &tuesday) {
		t.Error("a new file on the reused inode was taken for monday's; it would never be registered")
	}
	if err := register(device, &tuesday); err != nil {
		t.Errorf("registering the new file on the reused inode was refused: %v", err)
	}

	// Uniqueness is read from the catalogue rather than provoked. A refused
	// duplicate makes PostgreSQL write the rejected key -- a path and a file
	// name -- into its own error log, which is exactly the leak the stack
	// privacy phase exists to catch.
	pool := appPool(t, e)
	rows, err := pool.Query(ctx, `
		SELECT indexname, indexdef FROM pg_indexes
		 WHERE tablename = 'jobs'
		   AND indexname IN ('jobs_source_identity_idx', 'jobs_source_birth_identity_idx')`)
	if err != nil {
		t.Fatalf("read the identity indexes: %v", err)
	}
	defs := map[string]string{}
	for rows.Next() {
		var name, def string
		if err := rows.Scan(&name, &def); err != nil {
			t.Fatal(err)
		}
		defs[name] = def
	}
	rows.Close()
	for name, predicate := range map[string]string{
		"jobs_source_birth_identity_idx": "source_birth_time IS NOT NULL",
		"jobs_source_identity_idx":       "source_birth_time IS NULL",
	} {
		def := defs[name]
		if !strings.Contains(def, "UNIQUE") || !strings.Contains(def, predicate) {
			t.Errorf("%s is %q; want a unique index over the rows where %s", name, def, predicate)
		}
	}

	// A row from before birth times were recorded keeps the device rule.
	legacyRoot := root + "-legacy"
	size, algo := int64(1), storage.FingerprintAlgorithm
	ino, d := int64(inode), int64(device)
	mod := time.Now()
	if _, err := led.RegisterJob(ctx, ledger.RegisterInput{
		SourceRoot: legacyRoot, SourceName: name, SizeBytes: &size, FingerprintAlgo: &algo,
		Fingerprint: []byte{1}, PolicyIdentity: e.Cfg.Policy.Identity,
		SourceInode: &ino, SourceDevice: &d, SourceModifiedAt: &mod,
		DestinationRoot: e.Cfg.Storage.Consume,
	}); err != nil {
		t.Fatalf("register a row without a birth time: %v", err)
	}
	if _, ok, _ := led.AlreadyRegistered(ctx, legacyRoot, name, inode, device, &monday); !ok {
		t.Error("a row without a birth time did not recognise its file on the same device")
	}
	if _, ok, _ := led.AlreadyRegistered(ctx, legacyRoot, name, inode, remounted, &monday); ok {
		t.Error("a row without a birth time accepted a file on another device")
	}
}

// TestAReusedNameAndInodeIsRegisteredAgain is the case discovery used to
// miss: a document is delivered, its file is removed, and a new document is
// dropped under the same name. ext4 hands the freed inode number straight
// back, and the old identity check -- root, name, device, inode -- matched the
// new file against the old job and never registered it.
//
// This runs through the stack's real watcher. Whether the inode number was
// actually reused depends on the filesystem under the volume, so the evidence
// records it; the assertion is the same either way: the new document is a new
// job and it is delivered.
func TestAReusedNameAndInodeIsRegisteredAgain(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	name := "reused-name-" + e.RunID + ".pdf"
	path := filepath.Join(e.Cfg.Storage.Incoming, name)
	place(t, e, name, []byte("%PDF-1.4 the first document under this name "+e.RunID+"\n"))
	first := awaitJob(t, e, led, name)
	if first.State != jobs.StateDelivered {
		t.Fatalf("the first document ended %q (%s)", first.State, derefCategory(first))
	}
	_, firstInode := statIdentity(t, path)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	place(t, e, name, []byte("%PDF-1.4 a different document, same name "+e.RunID+"\n"))
	_, secondInode := statIdentity(t, path)

	ctx, cancel := context.WithTimeout(context.Background(), normalizeDeadline)
	defer cancel()
	var second ledger.Job
	for deadline := time.Now().Add(normalizeDeadline); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		j, err := jobByName(ctx, led, e.Cfg.Storage.Incoming, name)
		if err == nil && j.JobID != first.JobID && jobs.IsTerminal(j.State) {
			second = j
			break
		}
	}
	if second.JobID == "" {
		t.Fatalf("the second document under a reused name was never registered (inode reused: %t)",
			firstInode == secondInode)
	}
	if second.State != jobs.StateDelivered {
		t.Errorf("the second document ended %q (%s)", second.State, derefCategory(second))
	}
	if second.SourceBirthTime == nil {
		t.Errorf("the second document was registered without a birth time")
	}

	e.WriteEvidence(t, "reused-name-and-inode.txt", []byte(fmt.Sprintf(
		"first job:      %s (%s)\nsecond job:     %s (%s)\ninode reused:   %t\nbirth recorded: %t\n",
		first.JobID, first.State, second.JobID, second.State,
		firstInode == secondInode, second.SourceBirthTime != nil)))
}
