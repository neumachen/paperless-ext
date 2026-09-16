//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/broker"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// TestMain enforces the suite's dependency contract before any test runs.
//
// F5 requires that the suite fail when a required dependency is absent, and
// that it never silently skip or substitute an implementation. This checks
// both directions: a dependency that should be up but is not fails the run,
// and a dependency the orchestrator claims to have interrupted but which is
// still answering also fails the run, because the fault was not actually
// injected and any conclusion drawn from that phase would be false.
func TestMain(m *testing.M) {
	e := Suite()
	fmt.Fprintf(os.Stderr, "integration suite: phase=%s run_id=%s expect_down=%v\n",
		e.Phase, e.RunID, keys(e.ExpectDown))

	if code := verifyDependencyContract(e); code != 0 {
		os.Exit(code)
	}
	os.Exit(m.Run())
}

func verifyDependencyContract(e *Env) int {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	type dep struct {
		name  string
		probe func(context.Context) error
	}

	deps := []dep{{
		name: telemetry.DepPostgresPrimary,
		probe: func(ctx context.Context) error {
			led, err := ledger.Open(ctx, ledger.Options{
				Config: e.Cfg.Database, Logger: e.Log, Actor: "contract", OpTimeout: 5 * time.Second,
			})
			if err != nil {
				return err
			}
			defer led.Close()
			pctx, c := context.WithTimeout(ctx, 5*time.Second)
			defer c()
			return led.PingPrimary(pctx)
		},
	}, {
		name: telemetry.DepRabbitMQ,
		probe: func(ctx context.Context) error {
			conn := broker.Dial(broker.Options{Config: e.Cfg.Broker, Logger: e.Log, Name: "contract"})
			cctx, c := context.WithCancel(ctx)
			go conn.Run(cctx)
			defer func() { c(); conn.Shutdown() }()
			if !waitFor(10*time.Second, conn.Connected) {
				return fmt.Errorf("no broker connection")
			}
			return nil
		},
	}}

	failed := false
	for _, d := range deps {
		shouldBeDown := e.ExpectedDown(d.name)
		var err error
		if shouldBeDown {
			// A single probe is enough: it must not succeed.
			err = d.probe(ctx)
		} else {
			// Allow the dependency a bounded window to come up; the
			// orchestrator has already waited for container health.
			err = waitForErr(40*time.Second, func() error { return d.probe(ctx) })
		}

		switch {
		case shouldBeDown && err == nil:
			fmt.Fprintf(os.Stderr,
				"DEPENDENCY CONTRACT VIOLATION: %s was expected to be interrupted in phase %q but it answered. "+
					"The fault was not injected, so this phase cannot produce valid evidence.\n", d.name, e.Phase)
			failed = true
		case shouldBeDown && err != nil:
			fmt.Fprintf(os.Stderr, "dependency %s is interrupted as expected for phase %q\n", d.name, e.Phase)
		case !shouldBeDown && err != nil:
			fmt.Fprintf(os.Stderr,
				"REQUIRED DEPENDENCY ABSENT: %s did not answer within the startup window in phase %q. "+
					"The suite fails rather than skipping or substituting an implementation.\n", d.name, e.Phase)
			failed = true
		default:
			fmt.Fprintf(os.Stderr, "dependency %s is available\n", d.name)
		}
	}

	if failed {
		return 3
	}
	return 0
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestF0DependencyContractIsEnforced documents, as an executed assertion, that
// the contract check above ran and matched the orchestrator's declaration.
func TestF0DependencyContractIsEnforced(t *testing.T) {
	e := Suite()
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := led.PingPrimary(ctx)

	if e.ExpectedDown(telemetry.DepPostgresPrimary) {
		if err == nil {
			t.Fatalf("phase %q declares postgres_primary interrupted, but it answered", e.Phase)
		}
		t.Logf("postgres_primary is interrupted as declared for phase %q", e.Phase)
		return
	}
	if err != nil {
		t.Fatalf("postgres_primary is required in phase %q and must not be skipped: %v", e.Phase, err)
	}
}
