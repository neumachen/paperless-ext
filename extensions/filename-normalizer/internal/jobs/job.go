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
	// StatePublishing means a destination is reserved and the link into the
	// consume directory has been attempted. A job found here after a crash may
	// or may not have been published, which is exactly why the state exists.
	StatePublishing State = "publishing"
	// StateHeld means a durably recorded hold that requires intervention. It
	// never claims delivery.
	StateHeld State = "held"
	// StateDelivered means a durable delivery receipt exists.
	StateDelivered State = "delivered"
	// StateUncertain means a publication may have happened but no receipt
	// exists to prove it. It is terminal without intervention, and it never
	// triggers redelivery: the document may already be with the consumer.
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
		StatePublishing,
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
	// CategorySourceMutated records a submission whose bytes or identity
	// changed after it was registered.
	CategorySourceMutated Category = "source_mutated"
	// CategorySourceAbsent records a submission that disappeared before it
	// could be copied.
	CategorySourceAbsent Category = "source_absent"
	// CategoryMissingExtension and the three that follow come from the naming
	// policy refusing to name a file rather than guessing.
	CategoryMissingExtension Category = "missing_extension"
	// CategoryInvalidExtension records an extension outside the accepted shape.
	CategoryInvalidExtension Category = "invalid_extension"
	// CategoryNameTooLong records a name that cannot be shortened to fit.
	CategoryNameTooLong Category = "name_too_long"
	// CategoryPolicyNotIdempotent records configured rules that do not converge.
	CategoryPolicyNotIdempotent Category = "policy_not_idempotent"
	// CategoryNormalizationFailed is the residual naming failure.
	CategoryNormalizationFailed Category = "normalization_failed"
	// CategoryPolicyMismatch records a delivery whose job was accepted under a
	// different naming policy than this process is running. Renaming it here
	// would silently reinterpret queued work.
	CategoryPolicyMismatch Category = "policy_version_mismatch"
	// CategoryRetryExhausted records a job that used its delivery budget.
	CategoryRetryExhausted Category = "retry_exhausted"
	// CategoryCollisionExhausted records a collision sequence that ran out.
	CategoryCollisionExhausted Category = "collision_exhausted"
	// CategoryDestinationConflict records a destination occupied by content
	// that is not this job's. Nothing is overwritten.
	CategoryDestinationConflict Category = "destination_conflict"
	// CategoryDestinationMismatch records a job accepted for a different
	// destination root than this process is configured with. Publishing it
	// here would redirect work that was already accepted elsewhere.
	CategoryDestinationMismatch Category = "destination_mismatch"
	// CategoryUnsupportedConfiguration records a job that cannot be processed
	// because the running configuration does not support what it needs.
	CategoryUnsupportedConfiguration Category = "unsupported_configuration"
	// CategoryStorageUnavailable records an unreadable or unmounted root. It is
	// never interpreted as an empty directory.
	CategoryStorageUnavailable Category = "storage_unavailable"
	// CategoryPermissionDenied records a permission failure.
	CategoryPermissionDenied Category = "permission_denied"
	// CategoryStorageError is the residual filesystem failure.
	CategoryStorageError Category = "storage_error"
	// CategoryPublicationUncertain records that a publication was attempted but
	// no receipt exists. Redelivery is refused.
	CategoryPublicationUncertain Category = "publication_uncertain"
	// CategoryDryRun records a job a dry run examined without acting.
	CategoryDryRun Category = "dry_run"
	// CategoryUnknownJob records a broker message referencing no ledger row.
	CategoryUnknownJob Category = "unknown_job"
	// CategoryUnsupportedContract records an unparseable or future message.
	CategoryUnsupportedContract Category = "unsupported_contract"
	// CategoryDispatchReclaimed records the reaper returning a stranded row.
	CategoryDispatchReclaimed Category = "dispatch_reclaimed"
	// CategoryLedgerUnavailable records a durable-write failure.
	CategoryLedgerUnavailable Category = "ledger_unavailable"
)

// String renders the category for logs and metric labels.
func (c Category) String() string { return string(c) }

// Categories lists every category emitted by this increment.
func Categories() []Category {
	return []Category{
		CategorySourceMutated,
		CategorySourceAbsent,
		CategoryMissingExtension,
		CategoryInvalidExtension,
		CategoryNameTooLong,
		CategoryPolicyNotIdempotent,
		CategoryNormalizationFailed,
		CategoryPolicyMismatch,
		CategoryRetryExhausted,
		CategoryCollisionExhausted,
		CategoryDestinationConflict,
		CategoryDestinationMismatch,
		CategoryUnsupportedConfiguration,
		CategoryStorageUnavailable,
		CategoryPermissionDenied,
		CategoryStorageError,
		CategoryPublicationUncertain,
		CategoryDryRun,
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
	// EventDiscovered is written when discovery registers a submission.
	EventDiscovered EventType = "discovered"
	// EventNormalized is written when a name has been computed.
	EventNormalized EventType = "normalized"
	// EventReserved is written when a destination name becomes exclusive.
	EventReserved EventType = "reserved"
	// EventReservationBlocked is written when a reserved name turned out to be
	// occupied by a file this job did not publish.
	EventReservationBlocked EventType = "reservation_blocked"
	// EventPublishAttempted is written immediately before the destination link.
	EventPublishAttempted EventType = "publish_attempted"
	// EventDelivered is written with the durable delivery receipt.
	EventDelivered EventType = "delivered"
	// EventReconciled is written when recovery established an existing
	// destination as this job's own work, without republishing.
	EventReconciled EventType = "reconciled"
	// EventUncertain is written when a publication may have happened.
	EventUncertain EventType = "uncertain"
	// EventDryRun is written when a dry run examined a job without acting.
	EventDryRun EventType = "dry_run"
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
