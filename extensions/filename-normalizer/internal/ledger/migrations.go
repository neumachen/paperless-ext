package ledger

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationLockKey is a fixed advisory-lock key. Every instance that applies
// migrations takes it, so concurrently starting watchers serialise instead of
// racing on DDL.
const migrationLockKey int64 = 7_412_003_119_004_551

type migration struct {
	version int
	name    string
	body    string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, err
	}
	out := make([]migration, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		parts := strings.SplitN(strings.TrimSuffix(e.Name(), ".sql"), "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("migration %q does not follow <version>_<name>.sql", e.Name())
		}
		v, convErr := strconv.Atoi(parts[0])
		if convErr != nil {
			return nil, fmt.Errorf("migration %q has a non-numeric version", e.Name())
		}
		body, readErr := migrationFS.ReadFile("migrations/" + e.Name())
		if readErr != nil {
			return nil, readErr
		}
		out = append(out, migration{version: v, name: parts[1], body: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// Migrate brings the primary up to the embedded schema version.
//
// Migrations are applied only against the primary. Each migration runs inside
// its own transaction together with its bookkeeping row, so a failure leaves
// the recorded version consistent with what is actually installed.
func (l *Ledger) Migrate(ctx context.Context) error {
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	conn, err := l.primary.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("take migration lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.opTimeout)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, migrationLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    integer PRIMARY KEY,
			name       text        NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	applied := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.version] {
			l.log.Debug("migration already applied",
				slog.String("event", "migration_skipped"),
				slog.String("migration", m.name),
				slog.Int("count", m.version))
			continue
		}
		txErr := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.body); err != nil {
				return err
			}
			_, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.version, m.name)
			return err
		})
		if txErr != nil {
			l.log.Error("migration failed",
				slog.String("event", "migration_failed"),
				slog.String("migration", m.name),
				slog.String("error_kind", logging.ErrorKind(txErr)))
			return fmt.Errorf("apply migration %d_%s: %w", m.version, m.name, txErr)
		}
		l.log.Info("migration applied",
			slog.String("event", "migration_applied"),
			slog.String("migration", m.name),
			slog.Int("count", m.version))
	}
	return nil
}

// ExpectedSchemaVersion is the highest migration version embedded in this
// build. Readiness compares the installed version against it, so a binary
// newer than its database reports itself unusable rather than failing later on
// a column that does not exist yet.
func ExpectedSchemaVersion() (int, error) {
	migrations, err := loadMigrations()
	if err != nil {
		return 0, err
	}
	if len(migrations) == 0 {
		return 0, errors.New("no migrations are embedded in this build")
	}
	return migrations[len(migrations)-1].version, nil
}

// ErrSchemaUnusable reports a reachable database whose ledger schema cannot
// serve this build.
var ErrSchemaUnusable = errors.New("ledger schema is not usable")

// SchemaState describes what a readiness probe found.
type SchemaState struct {
	InstalledVersion int
	ExpectedVersion  int
	JobsPresent      bool
	EventsPresent    bool
}

// SchemaReady verifies the ledger schema is actually usable.
//
// Connectivity is not readiness: a reachable database whose migration never
// ran, or was blocked, would otherwise let an instance advertise itself as
// ready and then fail on its first durable write. The probe therefore checks
// the recorded version and that the relations this build writes exist.
func (l *Ledger) SchemaReady(ctx context.Context) (SchemaState, error) {
	expected, err := ExpectedSchemaVersion()
	if err != nil {
		return SchemaState{}, err
	}
	st := SchemaState{ExpectedVersion: expected}

	var installed *int
	// A missing schema_migrations table makes this query fail, which is the
	// correct answer: the schema has never been applied.
	err = l.primary.QueryRow(ctx, `
		SELECT (SELECT max(version) FROM schema_migrations),
		       to_regclass('jobs') IS NOT NULL,
		       to_regclass('job_events') IS NOT NULL`).
		Scan(&installed, &st.JobsPresent, &st.EventsPresent)
	if err != nil {
		return st, fmt.Errorf("%w: %w", ErrSchemaUnusable, err)
	}
	if installed != nil {
		st.InstalledVersion = *installed
	}

	switch {
	case !st.JobsPresent || !st.EventsPresent:
		return st, fmt.Errorf("%w: a required relation does not exist", ErrSchemaUnusable)
	case st.InstalledVersion < expected:
		return st, fmt.Errorf("%w: installed version %d is below the expected %d",
			ErrSchemaUnusable, st.InstalledVersion, expected)
	}
	return st, nil
}

// SchemaVersion reports the highest applied migration version, or 0.
func (l *Ledger) SchemaVersion(ctx context.Context) (int, error) {
	var v *int
	err := l.primary.QueryRow(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&v)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}
	return *v, nil
}
