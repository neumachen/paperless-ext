package config

import (
	"log/slog"
	"os"
	"strings"
)

// FaultPoint names a place in the processing path where a deployment may ask
// the process to die.
//
// # Why this exists
//
// Some required guarantees are about what happens when a worker stops
// *between* two durable writes. The window between linking a document into
// place and committing its receipt is the important one: a process that dies
// there leaves a job in `publishing`, and the whole recovery design turns on
// reconciling that correctly. Reasoning about it is not the same as observing
// it, and there is no way to observe it from outside the process -- the window
// is microseconds wide and the two operations are not separately signalable.
//
// So the process is given a way to really stop there. This is not a mock: the
// filesystem and ledger effects that precede the fault are real, and the
// process really exits. What is synthetic is only the timing of the crash,
// which is the one thing a test cannot otherwise control.
//
// It is inert unless FN_FAULT_POINTS names a point, it is never set in a
// normal deployment, and the applications log loudly at startup when it is.
type FaultPoint string

const (
	// FaultBeforeLink stops the process after the publish intent is committed
	// and before the destination link is attempted. Recovery must then find no
	// destination and report uncertainty rather than republishing.
	FaultBeforeLink FaultPoint = "before_link"
	// FaultAfterLink stops the process after the document is linked into place
	// and before the receipt is committed. Recovery must find the destination,
	// recognise it as this job's own work, and reconcile without republishing.
	FaultAfterLink FaultPoint = "after_link"
	// FaultAfterReceipt stops the process after the receipt is committed and
	// before the delivery is acknowledged, which is an ordinary at-least-once
	// redelivery.
	FaultAfterReceipt FaultPoint = "after_receipt"
)

// FaultPoints is the set a process will act on.
type FaultPoints map[FaultPoint]bool

// LoadFaultPoints reads FN_FAULT_POINTS, a comma-separated list.
func LoadFaultPoints() (FaultPoints, []string) {
	raw := strings.TrimSpace(os.Getenv("FN_FAULT_POINTS"))
	if raw == "" {
		return nil, nil
	}
	out := FaultPoints{}
	var names []string
	for _, part := range strings.Split(raw, ",") {
		name := FaultPoint(strings.TrimSpace(part))
		switch name {
		case "":
			continue
		case FaultBeforeLink, FaultAfterLink, FaultAfterReceipt:
			out[name] = true
			names = append(names, string(name))
		default:
			// An unknown fault point is ignored rather than fatal, but it is
			// named back to the caller so a typo is visible instead of
			// silently disabling the scenario a test depends on.
			names = append(names, "unknown:"+string(name))
		}
	}
	return out, names
}

// Enabled reports whether any fault point is armed.
func (f FaultPoints) Enabled() bool { return len(f) > 0 }

// Names lists the armed points, for the startup warning.
func (f FaultPoints) Names() []string {
	out := make([]string, 0, len(f))
	for k := range f {
		out = append(out, string(k))
	}
	return out
}

// Fire terminates the process if the named point is armed.
//
// os.Exit, not panic: a panic would run deferred cleanup and could be
// recovered, which would make the interruption something the program handled
// rather than something that happened to it. Exit code 90 distinguishes an
// injected stop from an ordinary failure.
func (f FaultPoints) Fire(point FaultPoint, log *slog.Logger) {
	if !f[point] {
		return
	}
	if log != nil {
		log.Error("stopping at an injected fault point",
			slog.String("event", "fault_point_fired"),
			slog.String("fault_point", string(point)))
	}
	os.Exit(90)
}
