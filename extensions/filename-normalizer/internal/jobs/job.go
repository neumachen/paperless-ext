// Package jobs holds the durable job vocabulary shared by both applications.
package jobs

import (
	"errors"
	"fmt"
	"strings"
)

// State is the durable lifecycle state of a job in the ledger.
//
// The set is intentionally small and closed: it is used as a Prometheus label
// value, so it must never grow with per-document data.
type State string

const (
	// StatePendingDispatch means the job is durably registered and still owes a
	// broker publication. It is the only state the dispatcher picks up.
	StatePendingDispatch State = "pending_dispatch"
	// StateDispatching means a dispatcher has claimed the row and is publishing.
	// A dispatcher that dies here leaves the row recoverable by the reaper.
	StateDispatching State = "dispatching"
	// StateDispatched means a publisher confirm was received for the job.
	StateDispatched State = "dispatched"
	// StateProcessing means a renamer has taken ownership of a delivery.
	StateProcessing State = "processing"
	// StateHeld means a durably recorded hold that requires intervention. It is
	// a safe terminal outcome for this increment: it never claims delivery.
	StateHeld State = "held"
	// StateDelivered means a durable delivery receipt exists. No code path in
	// this increment produces this state; it is reserved for the normalization
	// milestone and asserted to remain empty by the integration suite.
	StateDelivered State = "delivered"
	// StateUncertain means the outcome could not be established. No code path in
	// this increment produces this state either.
	StateUncertain State = "uncertain"
)

// States lists every valid state, in lifecycle order. Metrics iterate this so
// that a gauge exists (at zero) even for states nothing has reached yet.
func States() []State {
	return []State{
		StatePendingDispatch,
		StateDispatching,
		StateDispatched,
		StateProcessing,
		StateHeld,
		StateDelivered,
		StateUncertain,
	}
}

// ErrUnknownState is returned when a stored value is outside the closed set.
var ErrUnknownState = errors.New("unknown job state")

// ParseState validates a stored state value.
func ParseState(s string) (State, error) {
	for _, known := range States() {
		if string(known) == s {
			return known, nil
		}
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownState, s)
}

// Category is a sanitized, closed-set reason code. Categories appear in logs
// and in metric labels, so they must never embed document-derived text.
type Category string

const (
	// CategoryNormalizationUnimplemented records that a delivery reached a
	// renamer but normalization does not exist yet in this increment. It is an
	// honest hold, not a success.
	CategoryNormalizationUnimplemented Category = "normalization_unimplemented"
	// CategoryUnknownJob records a broker message referencing no ledger row.
	CategoryUnknownJob Category = "unknown_job"
	// CategoryUnsupportedContract records an unparseable or future message.
	CategoryUnsupportedContract Category = "unsupported_contract"
	// CategoryDispatchReclaimed records the reaper returning a stranded row.
	CategoryDispatchReclaimed Category = "dispatch_reclaimed"
	// CategoryLedgerUnavailable records a durable-write failure.
	CategoryLedgerUnavailable Category = "ledger_unavailable"
)

// Categories lists every category emitted by this increment.
func Categories() []Category {
	return []Category{
		CategoryNormalizationUnimplemented,
		CategoryUnknownJob,
		CategoryUnsupportedContract,
		CategoryDispatchReclaimed,
		CategoryLedgerUnavailable,
	}
}

// EventType names an append-only ledger history entry.
type EventType string

const (
	// EventRegistered is written when a job first becomes durable.
	EventRegistered EventType = "registered"
	// EventDispatchClaimed is written when a dispatcher claims the row.
	EventDispatchClaimed EventType = "dispatch_claimed"
	// EventDispatchConfirmed is written after a publisher confirm.
	EventDispatchConfirmed EventType = "dispatch_confirmed"
	// EventDispatchFailed is written when publication or confirmation failed.
	EventDispatchFailed EventType = "dispatch_failed"
	// EventDispatchReclaimed is written when a stranded claim is returned.
	EventDispatchReclaimed EventType = "dispatch_reclaimed"
	// EventDeliveryReceived is written when a renamer accepts a delivery.
	EventDeliveryReceived EventType = "delivery_received"
	// EventHeld is written when a job reaches a durably recorded hold.
	EventHeld EventType = "held"
)

// SafeIdentifier reports whether a string is safe to use as a metric label
// value: short, ASCII, and drawn from an identifier alphabet. Document-derived
// text never satisfies this, which is what keeps label cardinality bounded.
func SafeIdentifier(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_', r == '-', r == '.':
		default:
			return false
		}
	}
	return !strings.HasPrefix(s, "-")
}
