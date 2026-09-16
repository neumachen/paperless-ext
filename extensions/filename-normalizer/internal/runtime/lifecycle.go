// Package runtime supervises an application's workers and its shutdown.
//
// Shutdown order matters and is fixed here, in three phases:
//
//  1. Readiness is withdrawn, so nothing new is routed to this instance and
//     the withdrawal is observable before anything else changes.
//  2. Service workers are cancelled and waited for. A service worker stops
//     accepting new work and lets whatever it already holds finish.
//  3. Only then are infrastructure workers cancelled. Infrastructure — the
//     broker connection, the database pools — therefore stays usable for the
//     whole drain, so a worker finishing its last durable write is not cut off
//     from the store it must write to.
//
// The ordering is the point: cancelling one shared context would close the
// broker connection underneath a consumer that was still settling a delivery.
package runtime

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// Worker is a named long-running goroutine.
type Worker struct {
	Name string
	Run  func(ctx context.Context)
}

// Supervisor runs workers and coordinates graceful termination.
type Supervisor struct {
	log     *slog.Logger
	timeout time.Duration

	services []Worker
	infra    []Worker

	preStop  []func(ctx context.Context)
	postStop []func(ctx context.Context)
}

// New builds a supervisor. shutdownTimeout is the whole drain budget, shared
// between the service phase and the infrastructure phase.
func New(log *slog.Logger, shutdownTimeout time.Duration) *Supervisor {
	return &Supervisor{log: log, timeout: shutdownTimeout}
}

// Add registers a service worker: one that accepts or produces work and must
// finish draining before infrastructure is torn down.
func (s *Supervisor) Add(name string, run func(ctx context.Context)) {
	s.services = append(s.services, Worker{Name: name, Run: run})
}

// AddInfrastructure registers a worker that other workers depend on. It is
// cancelled only after every service worker has returned.
func (s *Supervisor) AddInfrastructure(name string, run func(ctx context.Context)) {
	s.infra = append(s.infra, Worker{Name: name, Run: run})
}

// OnDrain registers a hook that runs before any worker is cancelled. It is
// where readiness is withdrawn.
func (s *Supervisor) OnDrain(fn func(ctx context.Context)) { s.preStop = append(s.preStop, fn) }

// OnClose registers a hook that runs after every worker has returned.
func (s *Supervisor) OnClose(fn func(ctx context.Context)) { s.postStop = append(s.postStop, fn) }

// Run starts every worker and blocks until a termination signal arrives or the
// supplied context is cancelled. It returns after the shutdown sequence
// finishes, reporting whether every worker stopped within the budget.
func (s *Supervisor) Run(parent context.Context) (graceful bool) {
	// Two independent cancellation scopes, cancelled in order.
	svcCtx, cancelServices := context.WithCancel(context.WithoutCancel(parent))
	defer cancelServices()
	infraCtx, cancelInfra := context.WithCancel(context.WithoutCancel(parent))
	defer cancelInfra()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)

	var infraWG, svcWG sync.WaitGroup
	s.start(infraCtx, &infraWG, s.infra, "infrastructure")
	s.start(svcCtx, &svcWG, s.services, "service")

	var reason string
	select {
	case sig := <-sigCh:
		reason = sig.String()
		s.log.Info("termination signal received",
			slog.String("event", "shutdown_started"),
			slog.String("signal", reason),
			slog.Int64("shutdown_timeout_ms", s.timeout.Milliseconds()))
	case <-parent.Done():
		reason = "context"
		s.log.Info("shutdown requested",
			slog.String("event", "shutdown_started"),
			slog.String("signal", reason))
	}

	start := time.Now()
	deadline := start.Add(s.timeout)

	// Phase 1: withdraw readiness. Nothing has been cancelled yet, so the
	// withdrawal is observable while the instance is still draining.
	hookCtx, hookCancel := context.WithDeadline(context.WithoutCancel(parent), deadline)
	for _, fn := range s.preStop {
		fn(hookCtx)
	}
	hookCancel()

	// Phase 2: stop accepting work and wait for the service workers.
	cancelServices()
	drained := s.wait(&svcWG, time.Until(deadline))
	if !drained {
		s.log.Error("service workers did not finish draining within the shutdown timeout",
			slog.String("event", "shutdown_timeout"),
			slog.String("phase", "service"),
			slog.Int64("timeout_ms", s.timeout.Milliseconds()))
	} else {
		s.log.Info("service workers drained",
			slog.String("event", "drain_complete"),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()))
	}

	// Phase 3: only now tear down the infrastructure the drain depended on.
	cancelInfra()
	// Infrastructure teardown gets its own small floor, so a drain that used
	// the whole budget still closes its connections deliberately rather than
	// having them dropped by process exit.
	infraBudget := time.Until(deadline)
	if infraBudget < 5*time.Second {
		infraBudget = 5 * time.Second
	}
	infraStopped := s.wait(&infraWG, infraBudget)
	if !infraStopped {
		s.log.Error("infrastructure workers did not stop within the shutdown timeout",
			slog.String("event", "shutdown_timeout"),
			slog.String("phase", "infrastructure"))
	}

	graceful = drained && infraStopped

	closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
	defer closeCancel()
	for _, fn := range s.postStop {
		fn(closeCtx)
	}

	s.log.Info("shutdown complete",
		slog.String("event", "shutdown_complete"),
		slog.String("signal", reason),
		slog.Bool("ready", false),
		slog.Bool("applied", graceful),
		slog.Int64("duration_ms", time.Since(start).Milliseconds()))
	return graceful
}

func (s *Supervisor) start(ctx context.Context, wg *sync.WaitGroup, workers []Worker, phase string) {
	for _, w := range workers {
		wg.Add(1)
		go func(w Worker) {
			defer wg.Done()
			s.log.Info("worker started",
				slog.String("event", "worker_started"),
				slog.String("worker", w.Name),
				slog.String("phase", phase))
			defer s.log.Info("worker stopped",
				slog.String("event", "worker_stopped"),
				slog.String("worker", w.Name),
				slog.String("phase", phase))
			w.Run(ctx)
		}(w)
	}
}

func (s *Supervisor) wait(wg *sync.WaitGroup, budget time.Duration) bool {
	if budget <= 0 {
		budget = time.Millisecond
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	t := time.NewTimer(budget)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// LogFatal reports a startup failure in the structured format and returns the
// process exit code. The error string itself is not emitted: startup errors
// routinely embed a DSN or a path.
func LogFatal(log *slog.Logger, event string, err error) int {
	log.Error("startup failed",
		slog.String("event", event),
		slog.String("error_kind", logging.ErrorKind(err)))
	return 1
}
