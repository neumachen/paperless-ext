//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
)

// FN-F008.4 — restart persistence and crash recovery are different claims.
//
// An orderly `compose restart` sends SIGTERM, so PostgreSQL shuts down
// cleanly and comes back without any recovery work. That establishes that
// committed data survives a restart; it does not establish crash recovery.
//
// This phase runs after the orchestrator has sent SIGKILL to the primary
// container, which kills the postmaster outright. PostgreSQL then has to
// perform WAL recovery on startup, and says so in its own log. The assertion
// requires that evidence to be present, so the phase cannot be satisfied by an
// orderly restart.

// TestF3PrimarySurvivesAnUngracefulKill asserts real crash recovery.
func TestF3PrimarySurvivesAnUngracefulKill(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhasePrimaryKilled)
	led := e.Ledger(t)

	jobID := e.LoadState(t, "crash-job")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// The row committed before the kill must be readable afterwards.
	var job ledger.Job
	if err := waitForErr(120*time.Second, func() error {
		var gerr error
		job, gerr = led.GetJob(ctx, jobID)
		return gerr
	}); err != nil {
		t.Fatalf("a row committed before the primary was killed is not readable afterwards: %v", err)
	}
	if len(job.Fingerprint) != sha256.Size {
		t.Errorf("the persisted fingerprint did not survive the kill (%d bytes)", len(job.Fingerprint))
	}
	events, err := led.Events(ctx, jobID)
	if err != nil {
		t.Fatalf("read retained history after the kill: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("the retained history for %s is empty after the kill", jobID)
	}

	// The schema must still be usable, not merely the connection alive.
	if _, serr := led.SchemaReady(ctx); serr != nil {
		t.Errorf("the ledger schema is not usable after the kill: %v", serr)
	}

	// The current fault must be shown to have taken effect. The orchestrator
	// records the killed container's exit status; SIGKILL is 137. Without
	// this, a kill command that silently failed would leave the assertions
	// below free to pass on an earlier crash's records.
	killStatus := strings.TrimSpace(e.LoadState(t, "primary-kill-exit-code"))
	if killStatus != "137" {
		t.Fatalf("the primary's exit status after SIGKILL is %q, expected 137; the fault was not injected, "+
			"so nothing in this phase can be attributed to it", killStatus)
	}

	// The distinguishing evidence: PostgreSQL's own log must show that it
	// recovered rather than started from a clean shutdown — and it must show
	// that *for this fault*. The collected log holds the whole run, including
	// earlier restarts, so every line is filtered by the fault instant before
	// it is allowed to count.
	since := e.Since(t, "primary_killed")
	pgLines := readServiceLog(t, e, "postgres-primary")

	var (
		sawNotShutDown  bool
		sawRedo         bool
		sawRecoveryDone bool
		matched         []string
		before          int
		undated         int
	)
	for _, ln := range pgLines {
		ts, ok := postgresLogTime(ln)
		if !ok {
			// Continuation lines carry no timestamp of their own and cannot be
			// attributed to an interval, so they are not allowed to count.
			undated++
			continue
		}
		if ts.Before(since) {
			before++
			continue
		}
		low := strings.ToLower(ln)
		switch {
		case strings.Contains(low, "was not properly shut down"):
			sawNotShutDown = true
			matched = append(matched, strings.TrimSpace(ln))
		case strings.Contains(low, "redo starts at"), strings.Contains(low, "redo done at"):
			sawRedo = true
			matched = append(matched, strings.TrimSpace(ln))
		case strings.Contains(low, "database system is ready to accept connections"):
			sawRecoveryDone = true
		}
	}
	t.Logf("postgres log: %d line(s) before the fault instant excluded, %d undated line(s) ignored",
		before, undated)

	if !sawNotShutDown {
		t.Errorf("PostgreSQL reported no improper shutdown at or after the fault instant %s, so this phase "+
			"did not exercise crash recovery; an orderly restart would look like this", since.Format(time.RFC3339Nano))
	}
	if !sawRedo {
		t.Logf("note: no explicit redo line was logged; an instance killed at a checkpoint boundary can recover " +
			"with no WAL to replay, which is still crash recovery")
	}
	if !sawRecoveryDone {
		t.Errorf("PostgreSQL never reported itself ready to accept connections after the kill")
	}

	// And the applications must recover readiness on their own.
	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		u := url
		if werr := waitForErr(120*time.Second, func() error {
			r, _, body, gerr := GetReadiness(u)
			if gerr != nil {
				return gerr
			}
			if !r.Ready {
				return fmt.Errorf("%s is still not ready: %s", u, body)
			}
			return nil
		}); werr != nil {
			t.Errorf("%v", werr)
		}
	}

	report := fmt.Sprintf(
		"kill_signal=SIGKILL primary_exit_code=%s fault_interval_start=%s\n"+
			"postgres_log_lines_excluded_as_earlier=%d undated_lines_ignored=%d\n"+
			"job_readable_after_kill=%s state=%s retained_history_rows=%d\n"+
			"postgres_reported_improper_shutdown=%t redo_logged=%t ready_again=%t\n"+
			"all recovery lines below are timestamped at or after the fault instant:\n%s\n",
		killStatus, since.Format(time.RFC3339Nano), before, undated,
		job.JobID, job.State, len(events),
		sawNotShutDown, sawRedo, sawRecoveryDone, strings.Join(matched, "\n"))
	t.Logf("%s", report)
	e.WriteEvidence(t, "f3-crash-recovery.txt", []byte(report))
}

// TestF3SeedCrashRow commits the row the crash phase reads back.
func TestF3SeedCrashRow(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	job := registerSyntheticJob(t, led, e, "f3-crash-recovery")
	e.SaveState(t, "crash-job", job.JobID)

	// A checkpoint makes the durability claim about committed WAL rather than
	// about whatever happened to be flushed.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := led.Checkpoint(ctx); err != nil {
		t.Logf("could not force a checkpoint (%v); the assertion still holds on committed WAL", err)
	}
	t.Logf("seeded job %s for the crash-recovery phase", job.JobID)
}
