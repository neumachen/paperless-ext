package config

import (
	"strings"
	"testing"
	"time"
)

// A fault point that is armed and does nothing is worse than one that does not
// exist: the scenario depending on it runs, publishes normally, and its oracle
// reads the ordinary result as a product failure. That happened once already,
// when a rewrite dropped the `before_link` call site. These tests pin the
// cheap half of the contract -- that every named point is recognised by the
// loader -- so a point cannot be silently absent from the accepted set.
func TestEveryNamedFaultPointIsAccepted(t *testing.T) {
	for _, p := range []FaultPoint{
		FaultBeforeLink, FaultAfterLink, FaultAfterReceipt,
		FaultHoldAfterClaim, FaultHoldAfterLink,
	} {
		t.Setenv("FN_FAULT_POINTS", string(p))
		points, names := LoadFaultPoints()
		if !points[p] {
			t.Errorf("%q was not armed by the loader", p)
		}
		for _, n := range names {
			if strings.HasPrefix(n, "unknown:") {
				t.Errorf("%q came back as %q", p, n)
			}
		}
	}
}

// A typo must be visible rather than quietly disabling the scenario a test
// depends on.
func TestAnUnknownFaultPointIsNamedBack(t *testing.T) {
	t.Setenv("FN_FAULT_POINTS", "hold_after_lnik")
	points, names := LoadFaultPoints()
	if points.Enabled() {
		t.Error("a misspelled point must not arm anything")
	}
	if len(names) != 1 || !strings.HasPrefix(names[0], "unknown:") {
		t.Fatalf("the typo was not reported back: %v", names)
	}
}

// Nothing is armed unless the deployment asks for it.
func TestFaultPointsAreInertByDefault(t *testing.T) {
	// Set to empty rather than unset: that is what Compose actually delivers
	// for a variable it names but nobody assigned, and it must read as "off"
	// exactly like an absent one. t.Setenv also restores it afterwards, so the
	// check cannot leak into another test.
	t.Setenv("FN_FAULT_POINTS", "")
	points, names := LoadFaultPoints()
	if points.Enabled() || names != nil {
		t.Fatalf("faults armed with nothing set: %v", names)
	}
}

// The hold duration is bounded in both directions. An unparseable, absent,
// negative or absurd value falls back to the default rather than pausing a
// worker for an hour or not at all -- a hold that silently did not happen
// would make an overlap scenario pass without an overlap.
func TestHoldDurationFallsBackOutsideItsBounds(t *testing.T) {
	for _, raw := range []string{"", "  ", "nonsense", "-5s", "10m"} {
		t.Setenv("FN_FAULT_HOLD", raw)
		if got := HoldFor(); got != 10*time.Second {
			t.Errorf("FN_FAULT_HOLD=%q gave %s, want the 10s default", raw, got)
		}
	}
	t.Setenv("FN_FAULT_HOLD", "45s")
	if got := HoldFor(); got != 45*time.Second {
		t.Errorf("an explicit hold must be honoured, got %s", got)
	}
}
