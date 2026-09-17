package renamer

import (
	"context"
	"testing"
	"time"
)

// An attempt whose budget has run out has still established whatever it
// established. Before, every ledger write in that state failed with the
// attempt's own deadline: a link the kernel had refused could not be recorded,
// the job stayed `publishing`, and the next delivery read that as "a
// publication may have happened" and settled it `uncertain`. A refusal we were
// certain about became an outcome needing a person.
func TestADeterminedOutcomeSurvivesAnExpiredAttempt(t *testing.T) {
	expired, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-expired.Done()

	ctx, done := durably(expired)
	defer done()

	if err := ctx.Err(); err != nil {
		t.Fatalf("the recording context is already dead: %v", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("the recording context has no deadline; it must stay bounded")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > recordGrace+time.Second {
		t.Fatalf("recording grace is %s, want about %s", remaining, recordGrace)
	}
}

// While the attempt is still live nothing changes: the write stays on the
// attempt's context and remains cancellable with it.
func TestALiveAttemptKeepsItsOwnContext(t *testing.T) {
	live, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, done := durably(live)
	defer done()

	if ctx != live {
		t.Fatal("a live attempt must keep its own context")
	}
	cancel()
	if ctx.Err() == nil {
		t.Fatal("cancelling the attempt no longer cancels the write")
	}
}
