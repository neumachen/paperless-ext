//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
)

// TestRecoveryReconcilesItsOwnVisibleDocument covers a worker that dies after
// revealing a document and before closing its job.
//
// Publication commits the receipt, recording the inode it is about to make
// visible, then reveals that inode, and only afterwards removes its staged
// link and marks the job delivered. A worker lost in between leaves the
// document visible to the consumer, its staged link beside it, a receipt
// naming it, and the job still `publishing`. Recovery has to recognise that
// document as the job's own delivery. While the receipt was read back without
// the identity it recorded, recovery could not: it held the job as a
// destination conflict and deleted the receipt, so the consumer's directory
// held a document no receipt described.
//
// The interrupted state is rebuilt from a real delivery. No fault point kills
// a worker inside that window -- hold_after_link only pauses there, and an
// operator does the killing -- so the job row is returned to what a dead
// attempt leaves behind. The decision under test is made by a real renamer
// against the real ledger and filesystem.
func TestRecoveryReconcilesItsOwnVisibleDocument(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	name := "recovery-own-" + e.RunID + ".pdf"
	place(t, e, name, []byte("%PDF-1.4 recovery recognises its own visible document\n"))
	job := awaitJob(t, e, led, name)
	if job.State != jobs.StateDelivered {
		t.Fatalf("state %q (%s), expected delivered", job.State, derefCategory(job))
	}
	receipt, err := led.GetReceipt(ctx, job.JobID)
	if err != nil {
		t.Fatalf("no receipt: %v", err)
	}
	path := filepath.Join(receipt.DestinationRoot, receipt.DeliveredName)
	dev, ino := statIdentity(t, path)

	// The receipt must read back the identity publication recorded for it.
	if receipt.PublishedDevice != int64(dev) || receipt.PublishedInode != int64(ino) {
		t.Fatalf("the receipt reads back published identity %d/%d; the delivered file is %d/%d",
			receipt.PublishedDevice, receipt.PublishedInode, dev, ino)
	}

	// The staged link the dead attempt had not yet removed ...
	staged := filepath.Join(receipt.DestinationRoot, ".fn-"+job.JobID+"."+testNonce(t)+".tmp")
	if err := os.Link(path, staged); err != nil {
		t.Fatalf("recreate the staged link: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(staged) })

	// ... and the job row it had not yet closed, under a claim long expired.
	pool := appPool(t, e)
	var rewound time.Time
	if err := pool.QueryRow(ctx, `
		UPDATE jobs
		   SET state = 'publishing', terminal_at = NULL, failure_category = NULL,
		       publish_claimed_by = 'interrupted-attempt',
		       publish_claim_token = 'interrupted-attempt',
		       publish_attempted_at = now() - interval '1 hour'
		 WHERE job_id = $1
		RETURNING now()`, job.JobID).Scan(&rewound); err != nil {
		t.Fatalf("return the job to its interrupted state: %v", err)
	}

	republishJob(t, e, job.JobID)
	final := awaitJobByID(t, led, job.JobID)

	after, err := led.GetReceipt(ctx, job.JobID)
	receiptKept := err == nil
	if final.State != jobs.StateDelivered {
		t.Errorf("recovery ended the job %q (%s); its own visible document must reconcile as delivered",
			final.State, derefCategory(final))
	}
	if !receiptKept {
		t.Errorf("recovery deleted the receipt of a document that is visible: %v", err)
	} else if after.DeliveredName != receipt.DeliveredName ||
		after.PublishedDevice != int64(dev) || after.PublishedInode != int64(ino) {
		t.Errorf("the receipt changed: %s %d/%d, was %s %d/%d", after.DeliveredName,
			after.PublishedDevice, after.PublishedInode, receipt.DeliveredName, dev, ino)
	}
	nowDev, nowIno := statIdentity(t, path)
	if nowDev != dev || nowIno != ino {
		t.Errorf("the delivered document was replaced: %d/%d, was %d/%d", nowDev, nowIno, dev, ino)
	}

	events, err := led.Events(ctx, job.JobID)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	var since []string
	reconciled := false
	for _, ev := range events {
		if ev.OccurredAt.Before(rewound) {
			continue
		}
		since = append(since, string(ev.EventType))
		switch ev.EventType {
		case jobs.EventReconciled:
			reconciled = true
		case jobs.EventPublishAbandoned, jobs.EventHeld:
			t.Errorf("recovery recorded %q against the job's own visible document", ev.EventType)
		}
	}
	if !reconciled {
		t.Errorf("no reconciliation was recorded; events since the interruption: %v", since)
	}

	e.WriteEvidence(t, "recovery-own-visible-document.txt", []byte(fmt.Sprintf(
		"a delivered document returned to the state a worker lost between reveal and close leaves\n"+
			"published identity recorded: %d/%d (delivered file %d/%d)\n"+
			"staged link recreated:       %s\n"+
			"final state:                 %s (expected delivered)\n"+
			"receipt kept:                %t (expected true)\n"+
			"events since interruption:   %v\n",
		receipt.PublishedDevice, receipt.PublishedInode, dev, ino, filepath.Base(staged),
		final.State, receiptKept, since)))
}

// statIdentity returns a file's device and inode.
func statIdentity(t *testing.T, path string) (uint64, uint64) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s: no device/inode on this platform", path)
	}
	return uint64(st.Dev), st.Ino
}

func testNonce(t *testing.T) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("nonce: %v", err)
	}
	return hex.EncodeToString(b)
}

// appPool opens a one-connection pool as the application's own role.
func appPool(t *testing.T, e *Env) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(e.Cfg.Database.DSN())
	if err != nil {
		t.Fatalf("parse the ledger DSN: %v", err)
	}
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to the ledger: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
