//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
)

// FN-F006 — privacy across the whole stack's logs, not only the application's.
//
// Job registration binds the original filename, its absolute path and the
// content fingerprint as statement parameters. PostgreSQL's default logs bind
// parameter values alongside a slow statement, so a registration that happened
// to exceed the slow-statement threshold would publish document identity into
// ordinary database logs, bypassing the application's log-key allow-list
// entirely. The application's own privacy test cannot see that, because it
// scans only application logs.

// slowStatement matches PostgreSQL's slow-statement log prefix and captures
// the duration in milliseconds.
var slowStatement = regexp.MustCompile(`duration: ([0-9.]+) ms`)

// TestF6SlowRegistrationDoesNotLeakIntoDatabaseLogs forces a genuinely slow
// registration and then requires that the slow statement was logged while its
// parameter values were not.
func TestF6SlowRegistrationDoesNotLeakIntoDatabaseLogs(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseTelemetry)
	led := e.Ledger(t)

	// Distinctive synthetic markers. They are not real document identities;
	// they exist so that a leak is unambiguous when scanning.
	marker := "FNLEAKPROBE" + strings.ToUpper(strings.ReplaceAll(e.RunID, "-", ""))
	sourceName := fmt.Sprintf("Überweisung %s Straße 請求書.pdf", marker)
	fingerprintBytes := sha256.Sum256([]byte("synthetic-content-" + marker))
	fingerprintHex := hex.EncodeToString(fingerprintBytes[:])

	e.SaveState(t, "leak-marker", marker)
	e.SaveState(t, "leak-fingerprint", fingerprintHex)

	// Make the registration slow by holding a conflicting lock on the jobs
	// table for longer than the slow-statement threshold. The INSERT blocks,
	// so its recorded duration exceeds the threshold and PostgreSQL logs it.
	const holdFor = 3 * time.Second

	lockCtx, lockCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer lockCancel()

	var wg sync.WaitGroup
	wg.Add(1)
	lockHeld := make(chan struct{})
	var lockErr error
	go func() {
		defer wg.Done()
		// A separate transaction on the real primary, taking a lock that
		// conflicts with INSERT.
		lockErr = led.HoldTableLock(lockCtx, "jobs", holdFor, func() { close(lockHeld) })
	}()

	select {
	case <-lockHeld:
	case <-time.After(20 * time.Second):
		lockCancel()
		wg.Wait()
		t.Fatalf("could not acquire the conflicting lock, so no slow statement could be produced")
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer regCancel()

	size := int64(len("synthetic-content-" + marker))
	algo := "sha256"
	start := time.Now()
	job, err := led.RegisterJob(regCtx, ledger.RegisterInput{
		SourceRoot:      e.Cfg.Storage.Incoming,
		SourceName:      sourceName,
		SizeBytes:       &size,
		FingerprintAlgo: &algo,
		Fingerprint:     fingerprintBytes[:],
	})
	elapsed := time.Since(start)
	wg.Wait()
	if lockErr != nil {
		t.Logf("the lock holder reported: %v", lockErr)
	}
	if err != nil {
		t.Fatalf("the slow registration failed: %v", err)
	}
	if elapsed < time.Duration(e.SlowStatementThresholdMS())*time.Millisecond {
		t.Fatalf("the registration took only %s, below the %dms slow-statement threshold, "+
			"so PostgreSQL would not have logged it and this test proves nothing",
			elapsed.Round(time.Millisecond), e.SlowStatementThresholdMS())
	}
	t.Logf("registration of job %s blocked for %s, above the slow-statement threshold",
		job.JobID, elapsed.Round(time.Millisecond))
	e.SaveState(t, "leak-job-id", job.JobID)
	e.SaveState(t, "leak-elapsed-ms", fmt.Sprintf("%d", elapsed.Milliseconds()))
}

// TestF6NoStackLogLeaksDocumentIdentityOrCredentials scans every collected
// stack log — applications, both database nodes and the broker — against the
// real values in play.
func TestF6NoStackLogLeaksDocumentIdentityOrCredentials(t *testing.T) {
	e := Suite()
	// Run twice: once mid-run, where the forced slow statement is the subject,
	// and once at the very end, where the logs additionally contain every
	// outage, recovery, kill and termination the run produced. The mid-run
	// pass alone cannot cover records that did not exist when it ran.
	e.OnlyIn(t, PhaseStackPrivacy, PhaseFinalPrivacy)

	marker := e.LoadState(t, "leak-marker")
	fingerprint := e.LoadState(t, "leak-fingerprint")

	forbidden := map[string]string{
		"slow-registration marker":      marker,
		"slow-registration fingerprint": fingerprint,
		"database password":             e.Cfg.Database.Password,
		"broker password":               e.Cfg.Broker.Password,
		"database DSN":                  e.Cfg.Database.DSN(),
		"broker URI":                    e.Cfg.Broker.URI(),
	}
	names, err := listFixtureNames(e.Cfg.Storage.Incoming)
	if err != nil {
		t.Fatalf("read the synthetic fixture names: %v", err)
	}
	for i, n := range names {
		forbidden[fmt.Sprintf("document name %d", i)] = n
	}
	for _, role := range e.Cfg.Storage.RolesFor(config.AppRenamer) {
		forbidden["storage path "+role.Name] = role.Path
	}

	services := []string{
		"watcher", "renamer-1", "renamer-2",
		"postgres-primary", "postgres-replica", "rabbitmq",
	}

	var (
		report  strings.Builder
		scanned int
		leaks   int
	)
	for _, service := range services {
		lines := readServiceLog(t, e, service)
		for _, ln := range lines {
			scanned++
			for label, needle := range forbidden {
				if needle == "" {
					continue
				}
				if strings.Contains(ln, needle) {
					leaks++
					t.Errorf("%s leaked %s into its ordinary log: %s",
						service, label, redactForReport(ln, needle))
				}
			}
		}
		fmt.Fprintf(&report, "%s: %d lines scanned\n", service, len(lines))
	}

	fmt.Fprintf(&report, "total lines scanned=%d forbidden values=%d leaks=%d\n",
		scanned, len(forbidden), leaks)

	// The safeguard must be load-bearing, not vacuous. Three things have to
	// hold together: a slow statement was actually logged, the registration
	// statement's own text is among what was logged, and PostgreSQL logged no
	// parameter values for any of it.
	pgLines := readServiceLog(t, e, "postgres-primary")
	var (
		slowLogged      int
		slowestMS       float64
		insertTextSeen  bool
		parameterDetail int
	)
	for _, ln := range pgLines {
		if m := slowStatement.FindStringSubmatch(ln); m != nil {
			slowLogged++
			if ms, perr := strconv.ParseFloat(m[1], 64); perr == nil && ms > slowestMS {
				slowestMS = ms
			}
		}
		// The statement text spans several lines, so the INSERT is matched on
		// its own line rather than on the duration line.
		if strings.Contains(ln, "INSERT INTO jobs") {
			insertTextSeen = true
		}
		// This is the decisive check. Bind parameter values are logged in a
		// "DETAIL:  parameters:" continuation, and registration binds the
		// original filename, its absolute path and the content fingerprint.
		// With log_parameter_max_length = 0 there must be none at all.
		if strings.Contains(ln, "DETAIL:  parameters:") || strings.Contains(ln, "DETAIL: parameters:") {
			parameterDetail++
			t.Errorf("PostgreSQL logged bind parameter values, which is the leak channel this "+
				"assertion exists to close: %s", ln)
		}
	}
	fmt.Fprintf(&report,
		"postgres slow-statement log lines=%d slowest=%.0fms registration_statement_text_logged=%t "+
			"bind_parameter_detail_lines=%d\n",
		slowLogged, slowestMS, insertTextSeen, parameterDetail)

	// The slow-statement positive control belongs to the mid-run pass, whose
	// subject is the forced slow registration. The final pass covers a much
	// larger log and asserts the absence of leaks across it; requiring a fresh
	// slow statement there would be asserting something that phase does not
	// arrange.
	if e.Phase == PhaseStackPrivacy {
		if slowLogged == 0 {
			t.Errorf("PostgreSQL logged no slow statement at all, so the absence of parameter values " +
				"does not establish that parameter logging is suppressed")
		}
		if slowestMS < float64(e.SlowStatementThresholdMS()) {
			t.Errorf("the slowest logged statement was %.0fms, below the %dms threshold: the forced "+
				"slow registration was not captured", slowestMS, e.SlowStatementThresholdMS())
		}
		if !insertTextSeen {
			t.Errorf("the registration statement's text was never logged, so this run does not show " +
				"a registration being logged slowly with its values suppressed")
		}
	}
	if parameterDetail == 0 && insertTextSeen && slowLogged > 0 {
		t.Logf("PostgreSQL logged the registration statement as slow (%.0fms) with $1-style "+
			"placeholders and no parameter values at all", slowestMS)
	}

	// And a positive control: the marker must really be in the database, so a
	// clean scan cannot be explained by the row never having been written.
	led := e.Ledger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	jobID := e.LoadState(t, "leak-job-id")
	stored, err := led.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("the slow-registration job is not in the ledger: %v", err)
	}
	if !strings.Contains(stored.SourceName, marker) {
		t.Fatalf("the stored source name does not contain the marker; the scan above had nothing to find")
	}
	if !jobs.IsJobID(stored.JobID) {
		t.Errorf("stored job identity is not canonical: %q", stored.JobID)
	}
	fmt.Fprintf(&report, "positive control: marker is present in the ledger row for job %s\n", jobID)

	// The final pass has to be shown to actually cover the run's later events,
	// otherwise it is just the mid-run scan repeated.
	if e.Phase == PhaseFinalPrivacy {
		want := map[string]string{
			"a primary outage":        "postgres_primary",
			"a broker outage":         "rabbitmq",
			"an application shutdown": "shutdown_complete",
			"a readiness withdrawal":  "readiness_withdrawn",
		}
		appLines := 0
		seen := map[string]bool{}
		for _, service := range []string{"watcher", "renamer-1", "renamer-2"} {
			for _, ln := range readServiceLog(t, e, service) {
				appLines++
				for label, needle := range want {
					if strings.Contains(ln, needle) {
						seen[label] = true
					}
				}
			}
		}
		var missing []string
		for label := range want {
			if !seen[label] {
				missing = append(missing, label)
			}
		}
		sort.Strings(missing)
		fmt.Fprintf(&report, "final pass: %d application log lines; evidence of %d/%d expected run events\n",
			appLines, len(want)-len(missing), len(want))
		if len(missing) > 0 {
			t.Errorf("the final privacy pass does not cover: %s; it is not scanning the whole run",
				strings.Join(missing, ", "))
		}
		// PostgreSQL's crash recovery must be inside what was scanned too.
		pgRecovery := 0
		for _, ln := range pgLines {
			if strings.Contains(strings.ToLower(ln), "was not properly shut down") {
				pgRecovery++
			}
		}
		fmt.Fprintf(&report, "final pass: postgres crash-recovery lines within the scanned log: %d\n", pgRecovery)
		if pgRecovery == 0 {
			t.Errorf("the final privacy pass does not cover the run's crash recovery")
		}
	}

	t.Logf("%s", report.String())
	name := "f6-stack-log-privacy.txt"
	if e.Phase == PhaseFinalPrivacy {
		name = "f6-stack-log-privacy-whole-run.txt"
	}
	e.WriteEvidence(t, name, []byte(report.String()))
}
