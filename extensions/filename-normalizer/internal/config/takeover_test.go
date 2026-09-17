package config

import (
	"testing"
	"time"
)

// loadRenamer builds a renamer configuration from a minimal valid environment
// plus whatever the caller has set, and fails the test if it will not load.
func loadRenamer(t *testing.T) RenamerConfig {
	t.Helper()
	cfg, err := LoadRenamer()
	if err != nil {
		t.Fatalf("configuration did not load: %v", err)
	}
	return cfg
}

// The publication takeover window used to be the shutdown timeout, borrowed.
//
// Those answer different questions. "How long do we wait for a graceful stop"
// and "how long before we presume the worker holding this publication is
// gone" have no reason to move together, and an operator who lengthened one
// had no way to avoid lengthening the other. The test pins the separation:
// the window follows the shutdown timeout only until somebody sets it.
func TestPublishTakeoverDefaultsToShutdownTimeoutButIsSeparable(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_SHUTDOWN_TIMEOUT", "45s")
	cfg := loadRenamer(t)
	if cfg.PublishTakeoverAfter != 45*time.Second {
		t.Fatalf("with no explicit setting the window should follow the shutdown timeout, got %s", cfg.PublishTakeoverAfter)
	}

	t.Setenv("FN_PUBLISH_TAKEOVER_AFTER", "3m")
	cfg = loadRenamer(t)
	if cfg.PublishTakeoverAfter != 3*time.Minute {
		t.Fatalf("an explicit window must win, got %s", cfg.PublishTakeoverAfter)
	}
	if cfg.ShutdownTimeout != 45*time.Second {
		t.Fatalf("setting the takeover window must not move the shutdown timeout, got %s", cfg.ShutdownTimeout)
	}
}

// An empty value is not a value. Compose passes variables it names even when
// the caller left them unset, so an empty string reaches the process as an
// ordinary environment entry; reading it as a duration would fail the whole
// configuration for a variable nobody set.
func TestEmptyTakeoverWindowIsTreatedAsUnset(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_SHUTDOWN_TIMEOUT", "25s")
	t.Setenv("FN_PUBLISH_TAKEOVER_AFTER", "")
	cfg := loadRenamer(t)
	if cfg.PublishTakeoverAfter != 25*time.Second {
		t.Fatalf("an empty setting must fall back to the default, got %s", cfg.PublishTakeoverAfter)
	}
}

// The effective-configuration view is what an operator reads when diagnosing a
// stranded or a twice-attempted publication. The window has to be in it: it is
// no longer implied by the shutdown timeout, so it cannot be inferred.
func TestEffectiveViewReportsTheTakeoverWindow(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FN_PUBLISH_TAKEOVER_AFTER", "90s")
	cfg := loadRenamer(t)
	if got := cfg.Effective().Processing.PublishTakeoverMS; got != 90_000 {
		t.Fatalf("effective view reports publish_takeover_ms = %d, want 90000", got)
	}
}
