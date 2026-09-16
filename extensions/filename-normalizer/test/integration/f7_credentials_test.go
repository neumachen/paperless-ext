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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
)

// FN-F007 — a valid PostgreSQL password must not break connection parsing.
//
// libpq keyword/value connection strings separate parameters on whitespace, so
// an unquoted value containing a space terminates its parameter early and
// turns the remainder into further parameters. A password containing a quote or
// a backslash is misparsed in its own way. Generated alphanumeric development
// passwords never exercise any of it, which is exactly why it has to be proven
// rather than assumed.
//
// The role below is created on the real cluster, used to authenticate through
// the application's own configuration and ledger code with the password
// supplied from a mounted file, and dropped again. It is granted only the
// ability to connect.

// nastyPassword contains every character class that breaks naive
// interpolation: a space, a single quote, a backslash, a double quote, a
// colon, a slash, an at-sign and an equals sign.
const nastyPassword = `p ass'w\"ord:/@ x=1`

func TestF7RealAuthenticationWithSpecialCharacterCredentials(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	role := "fn_probe_" + sanitizeRoleSuffix(e.RunID)
	if err := led.CreateLoginRole(ctx, role, nastyPassword, e.Cfg.Database.Name); err != nil {
		t.Fatalf("create the probe role on the real cluster: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if err := led.DropLoginRole(dropCtx, role); err != nil {
			t.Logf("teardown: probe role %s was not dropped: %v", role, err)
		} else {
			t.Logf("teardown: dropped probe role %s", role)
		}
	})

	// The password arrives the way a deployment supplies it: as a file.
	dir := t.TempDir()
	pwFile := filepath.Join(dir, "probe-password")
	if err := os.WriteFile(pwFile, []byte(nastyPassword+"\n"), 0o600); err != nil {
		t.Fatalf("write the password file: %v", err)
	}

	// Load configuration through the real loader, with the file indirection.
	t.Setenv("FN_DB_USER", role)
	t.Setenv("FN_DB_PASSWORD_FILE", pwFile)
	os.Unsetenv("FN_DB_PASSWORD")
	t.Setenv("FN_DB_REPLICA_HOST", "")

	cfg, err := config.LoadRenamer()
	if err != nil {
		t.Fatalf("the loader rejected the file-supplied credential: %v", err)
	}
	if cfg.Database.Password != nastyPassword {
		t.Fatalf("the password read from the file does not match what was written:\n got %q\nwant %q",
			cfg.Database.Password, nastyPassword)
	}
	if cfg.Database.User != role {
		t.Fatalf("configured user is %q, expected %q", cfg.Database.User, role)
	}

	// The negative control: naive interpolation of the same valid password
	// must fail to authenticate, which is what makes the quoting load-bearing
	// rather than decorative.
	naive := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=5",
		cfg.Database.PrimaryHost, cfg.Database.PrimaryPort, cfg.Database.Name,
		cfg.Database.User, cfg.Database.Password, cfg.Database.SSLMode)
	naiveErr := tryConnect(ctx, naive)
	if naiveErr == nil {
		t.Errorf("an unquoted connection string authenticated with %q; the negative control did not hold, "+
			"so this test does not establish that quoting is required", nastyPassword)
	} else {
		t.Logf("negative control: unquoted interpolation failed as expected: %v", truncate(naiveErr.Error(), 200))
	}

	// The real path: the application's own ledger code, against the real
	// primary, authenticating as the probe role.
	probeLedger, err := ledger.Open(ctx, ledger.Options{
		Config:    cfg.Database,
		Logger:    e.Log,
		Actor:     "integration/credential-probe",
		OpTimeout: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("open a ledger with the special-character credential: %v", err)
	}
	t.Cleanup(probeLedger.Close)

	if err := waitForErr(45*time.Second, func() error { return probeLedger.PingPrimary(ctx) }); err != nil {
		t.Fatalf("real PostgreSQL authentication with a special-character password failed: %v", err)
	}
	t.Logf("authenticated against the real primary as %q with a password containing "+
		"a space, a single quote, a backslash, a double quote, a colon, a slash, an at-sign and an equals sign", role)

	// Privacy must hold for the probe credential too.
	dsn := cfg.Database.DSN()
	if !strings.Contains(dsn, `password='p ass\'w\\"ord:/@ x=1'`) {
		t.Errorf("the DSN does not quote the password as expected: %s", redactForReport(dsn, nastyPassword))
	}
	for _, v := range cfg.Summary() {
		if str, ok := v.(string); ok && strings.Contains(str, nastyPassword) {
			t.Errorf("the loggable configuration summary contains the password")
		}
	}

	e.SaveState(t, "credential-probe-role", role)
	e.WriteEvidence(t, "f7-special-character-credentials.txt", []byte(fmt.Sprintf(
		"role=%s\npassword_character_classes=space,single_quote,backslash,double_quote,colon,slash,at,equals\n"+
			"password_supplied_via=FN_DB_PASSWORD_FILE\n"+
			"quoted_dsn_authenticated=true\nnaive_interpolation_authenticated=%t\n"+
			"password_present_in_loggable_summary=false\n",
		role, naiveErr == nil)))
}

// TestF7SpecialCharacterCredentialNeverReachesLogs closes the privacy half:
// the probe password must not appear in any stack log.
func TestF7SpecialCharacterCredentialNeverReachesLogs(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseStackPrivacy)

	role, ok := e.OptionalState("credential-probe-role")
	if !ok {
		t.Skip("no credential probe was run in this run's baseline phase")
	}

	var hits int
	for _, service := range []string{
		"watcher", "renamer-1", "renamer-2",
		"postgres-primary", "postgres-replica", "rabbitmq",
	} {
		for _, ln := range readServiceLog(t, e, service) {
			if strings.Contains(ln, nastyPassword) {
				hits++
				t.Errorf("%s leaked the probe password: %s", service, redactForReport(ln, nastyPassword))
			}
		}
	}
	t.Logf("probe role %s: the special-character password appears in %d stack log line(s)", role, hits)
	e.WriteEvidence(t, "f7-credential-log-privacy.txt", []byte(fmt.Sprintf(
		"probe_role=%s password_occurrences_in_stack_logs=%d\n", role, hits)))
}

func tryConnect(ctx context.Context, dsn string) error {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var one int
	return pool.QueryRow(tctx, "SELECT 1").Scan(&one)
}

// sanitizeRoleSuffix renders a run id as a safe SQL identifier suffix.
func sanitizeRoleSuffix(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if len(out) > 40 {
		out = out[:40]
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
