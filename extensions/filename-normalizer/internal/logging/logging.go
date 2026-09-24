// Package logging provides the structured JSON logger both applications use.
//
// The requirements forbid document identities in ordinary output, so this
// package is the single place where that rule is enforced: attributes are
// filtered against an allow-list of safe keys before they reach the handler.
// Code that wants to log something new must add a key here deliberately.
package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

// Safe attribute keys. Anything not listed is dropped and replaced by a
// marker, which makes an accidental leak loud in tests rather than silent.
var safeKeys = map[string]struct{}{}

func init() {
	keys := []string{
		// process identity
		"application", "instance", "version", "revision", "build_date",
		"go_version", "policy_version", "contract_version", "pid",
		// lifecycle
		"event", "component", "worker", "state", "phase", "signal",
		"duration_ms", "timeout_ms", "interval_ms", "attempt", "attempts",
		"max_attempts", "concurrency", "prefetch", "shutdown_timeout_ms",
		// dependency identity (host/port only; never credentials)
		"dependency", "host", "port", "database", "vhost", "exchange",
		"queue", "routing_key", "dlx", "dead_letter_queue", "tls", "ready",
		"replica_host", "replica_port", "replica_required", "in_recovery",
		// job identity: opaque IDs are permitted in logs, never as metric labels
		"job_id", "delivery_tag", "redelivered", "consumer_tag",
		// sanitized outcomes
		"category", "outcome", "reason", "error_kind", "storage_role",
		"storage_status", "http_status", "addr", "path", "count",
		"oldest_pending_age_ms", "lag_bytes", "migration", "applied",
		// discovery and publication counters. These are aggregate numbers and
		// booleans about the run, never document-derived text: a size in bytes
		// is a property of a submission, not an identifier of it, and the four
		// scan counters say how much work a scan saw rather than what it saw.
		"size_bytes", "limit_bytes", "examined", "registered", "reconciling",
		"collision_sequence", "used_fallback", "shortened", "recursive",
		"completion_contract", "interval_seconds", "stability_seconds",
		"max_sequence", "job_policy", "process_policy",
		// Aggregate booleans and closed-set identifiers about an operation.
		// None is document-derived: `held_by` is an instance name, which is
		// process identity and already logged under `instance`; `fault_point`
		// names a configured injection point from a closed set; the rest are
		// booleans about what this process did. `delivered_as`,
		// `destination_path` and `source_name` are deliberately NOT here --
		// those are document names.
		"held_by", "fault_point", "content_matches", "removed",
		"destination_already_absent",
		// What the watcher does with a delivered original: "move" or
		// "remove". A configured closed-set value, never a name.
		"archive_action",
	}
	for _, k := range keys {
		safeKeys[k] = struct{}{}
	}
}

// Redacted replaces the value of any attribute outside the allow-list.
const Redacted = "[redacted:unapproved-log-key]"

// countPrefix marks keys whose value is a COUNT of jobs in one closed-set
// category, such as would_hold_policy_transliterates.
//
// The dry run's whole output is aggregate counts by category, and every one of
// them was redacted: the key is built from the category, the allow-list matches
// exact strings, and so the report an operator reads to decide whether a
// configuration change is safe printed "[redacted]" where every number should
// have been. The suffix is bounded to the closed-set identifier shape the
// caller has already filtered on, and the VALUE is an integer, so this admits
// counts without admitting anything document-derived.
const countPrefix = "would_hold_"

// IsSafeKey reports whether key may carry a value into ordinary output.
func IsSafeKey(key string) bool {
	if _, ok := safeKeys[key]; ok {
		return true
	}
	return isCategoryCount(key)
}

func isCategoryCount(key string) bool {
	rest, ok := strings.CutPrefix(key, countPrefix)
	if !ok || rest == "" || len(rest) > 64 {
		return false
	}
	for _, r := range rest {
		if (r < 'a' || r > 'z') && r != '_' {
			return false
		}
	}
	return true
}

// Options configures the process logger.
type Options struct {
	Level       slog.Level
	Application string
	Instance    string
}

// New builds the process-wide JSON logger.
func New(w io.Writer, opts Options) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:       opts.Level,
		ReplaceAttr: replaceAttr,
	})
	return slog.New(&guard{inner: handler}).With(
		slog.String("application", opts.Application),
		slog.String("instance", opts.Instance),
	)
}

// replaceAttr normalizes the built-in keys and redacts unapproved ones.
func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 {
		switch a.Key {
		case slog.TimeKey, slog.LevelKey, slog.MessageKey, slog.SourceKey:
			return a
		}
	}
	if IsSafeKey(a.Key) {
		return a
	}
	return slog.String(a.Key, Redacted)
}

// guard forces every record through a single handler and keeps the error text
// of wrapped errors out of the output. Errors frequently embed a filesystem
// path or a DSN, so callers log an error_kind instead of the error string.
type guard struct{ inner slog.Handler }

func (g *guard) Enabled(ctx context.Context, l slog.Level) bool { return g.inner.Enabled(ctx, l) }

func (g *guard) Handle(ctx context.Context, r slog.Record) error { return g.inner.Handle(ctx, r) }

func (g *guard) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &guard{inner: g.inner.WithAttrs(attrs)}
}

func (g *guard) WithGroup(name string) slog.Handler { return &guard{inner: g.inner.WithGroup(name)} }

// ErrorKind renders an error as a coarse, non-revealing classification.
//
// Error strings from the database driver, the AMQP client and the filesystem
// routinely contain DSNs, credentials and paths. Callers therefore log
// ErrorKind(err) and keep the full error for a debug-only sink.
func ErrorKind(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "context canceled"):
		return "canceled"
	case strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "connection refused"):
		return "connection_refused"
	case strings.Contains(msg, "is not established"), strings.Contains(msg, "not connected"):
		// No connection was held in the first place, which reads differently
		// from a connection that was lost mid-use.
		return "not_connected"
	case strings.Contains(msg, "connection reset"), strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "unexpected eof"), strings.Contains(msg, "closed"):
		return "connection_lost"
	case strings.Contains(msg, "no such host"), strings.Contains(msg, "lookup"):
		return "name_resolution"
	case strings.Contains(msg, "permission denied"), strings.Contains(msg, "access denied"):
		return "permission_denied"
	case strings.Contains(msg, "no space left"):
		return "storage_full"
	case strings.Contains(msg, "read-only"), strings.Contains(msg, "readonly"):
		return "read_only"
	case strings.Contains(msg, "no such file"):
		return "not_found"
	case strings.Contains(msg, "authentication"), strings.Contains(msg, "password"):
		return "authentication"
	case strings.Contains(msg, "does not exist") && strings.Contains(msg, "relation"):
		// The schema has not been applied yet. Distinguishing this from a
		// generic failure is what lets an operator tell a starting stack apart
		// from a broken one.
		return "schema_missing"
	case strings.Contains(msg, "does not exist"):
		return "not_found"
	case strings.Contains(msg, "in recovery"):
		return "in_recovery"
	case strings.Contains(msg, "administrator command"),
		strings.Contains(msg, "shutting down"),
		strings.Contains(msg, "shutdown"):
		// The server went away mid-statement. Distinguishing an orderly
		// shutdown from a network fault matters when reading why a durable
		// write did not land.
		return "server_shutdown"
	default:
		return "unclassified"
	}
}

// ErrorKinds is the closed set ErrorKind can return. Metrics pre-initialise
// from this list so a new classification cannot silently appear as a new
// series without being declared here.
func ErrorKinds() []string {
	return []string{
		"canceled", "timeout", "connection_refused", "connection_lost",
		"name_resolution", "permission_denied", "storage_full", "read_only",
		"not_found", "authentication", "schema_missing", "in_recovery",
		"server_shutdown", "not_connected", "unclassified",
	}
}

// ParseLevel maps a configured level name onto slog levels.
func ParseLevel(s string) (slog.Level, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, true
	case "info", "":
		return slog.LevelInfo, true
	case "warn", "warning":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	default:
		return slog.LevelInfo, false
	}
}
