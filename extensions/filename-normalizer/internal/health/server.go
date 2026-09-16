// Package health serves the liveness, readiness and metrics endpoints.
//
// Readiness is truthful: it reflects the last real probe of each dependency
// and reports which dependency is at fault, without revealing credentials.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo"
)

// Check is one named dependency probe.
type Check struct {
	// Name is a closed-set dependency identifier.
	Name string
	// Required marks a dependency whose failure makes the process not ready.
	Required bool
	// Probe returns a sanitized category on failure. It must not return an
	// error string that could embed a DSN, a credential or a path.
	Probe func(ctx context.Context) (ok bool, category string)
}

// State is the most recent evaluation of one check.
type State struct {
	Name      string `json:"name"`
	Required  bool   `json:"required"`
	OK        bool   `json:"ok"`
	Category  string `json:"category,omitempty"`
	CheckedAt string `json:"checked_at"`
}

// Server owns the HTTP surface and the background probe loop.
type Server struct {
	addr    string
	app     string
	inst    string
	log     *slog.Logger
	reg     *prometheus.Registry
	checks  []Check
	timeout time.Duration

	// publishMu serialises readiness publication against a withdrawal. It is
	// always acquired before mu.
	publishMu sync.Mutex

	mu      sync.RWMutex
	states  map[string]State
	ready   bool
	started bool
	// draining latches readiness off once shutdown begins. Without it a probe
	// pass already running concurrently with the withdrawal could publish a
	// ready:true snapshot afterwards, so an observer draining the instance
	// would see it advertise itself as available again.
	draining bool

	// onReadiness is invoked with each evaluation so the metric set stays in
	// step with the endpoint. It must not block.
	onReadiness func(ready bool, states []State)

	srv *http.Server
}

// Options configures the server.
type Options struct {
	Addr         string
	Application  string
	Instance     string
	Logger       *slog.Logger
	Registry     *prometheus.Registry
	Checks       []Check
	ProbeTimeout time.Duration
	OnReadiness  func(ready bool, states []State)
}

// New builds a server. It does not listen until Start is called.
func New(opts Options) *Server {
	timeout := opts.ProbeTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	s := &Server{
		addr:        opts.Addr,
		app:         opts.Application,
		inst:        opts.Instance,
		log:         opts.Logger,
		reg:         opts.Registry,
		checks:      opts.Checks,
		timeout:     timeout,
		states:      make(map[string]State, len(opts.Checks)),
		onReadiness: opts.OnReadiness,
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, c := range opts.Checks {
		s.states[c.Name] = State{Name: c.Name, Required: c.Required, OK: false, Category: "not_probed", CheckedAt: now}
	}
	return s
}

// Handler exposes the routes for direct use in tests.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))
	return mux
}

// Start binds the listener and begins probing. It returns once the listener is
// bound so callers can rely on the port being reachable afterwards.
func (s *Server) Start(ctx context.Context, interval time.Duration) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.srv = &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()

	go func() {
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http server stopped", slog.String("event", "http_server_error"))
		}
	}()
	s.log.Info("http server listening",
		slog.String("event", "http_listening"),
		slog.String("addr", ln.Addr().String()))

	go s.probeLoop(ctx, interval)
	return nil
}

// Addr reports the bound address.
func (s *Server) Addr() string { return s.addr }

func (s *Server) probeLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	s.Evaluate(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Evaluate(ctx)
		}
	}
}

// Evaluate runs every probe once and updates the reported readiness.
func (s *Server) Evaluate(ctx context.Context) bool {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	results := make([]State, 0, len(s.checks))
	ready := true

	for _, c := range s.checks {
		pctx, cancel := context.WithTimeout(ctx, s.timeout)
		ok, category := c.Probe(pctx)
		cancel()
		// A category is kept even on success. Some checks pass while reporting
		// something an operator must see — a standby that has been promoted
		// still answers queries, and discarding that category would hide it.
		if !ok && category == "" {
			category = "unavailable"
		}
		st := State{Name: c.Name, Required: c.Required, OK: ok, Category: category, CheckedAt: now}
		results = append(results, st)
		if c.Required && !ok {
			ready = false
		}
	}

	// publishMu makes "decide whether we may publish" and "publish" one
	// indivisible step, and SetNotReady takes the same lock.
	//
	// Checking the drain flag under s.mu and then releasing it before calling
	// onReadiness was not enough. A withdrawal could land in that gap: it
	// latched the endpoint false and published gauge 0, and then this pass —
	// already past its check — published gauge 1 on top. Later passes returned
	// early because of the latch, so the gauge could stay wrong until exit,
	// disagreeing with the endpoint.
	s.publishMu.Lock()
	defer s.publishMu.Unlock()

	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		// The withdrawal wins. This pass is discarded rather than allowed to
		// overwrite it.
		return false
	}
	for _, st := range results {
		s.states[st.Name] = st
	}
	s.ready = ready
	s.mu.Unlock()

	if s.onReadiness != nil {
		s.onReadiness(ready, results)
	}
	return ready
}

// SetNotReady latches readiness off for the rest of the process lifetime.
//
// It is called once, at the start of a graceful drain. The latch is what makes
// the withdrawal trustworthy: a probe pass that was already in flight cannot
// flip readiness back on afterwards.
func (s *Server) SetNotReady(category string) {
	// Same lock ordering as Evaluate: publishMu, then mu. An evaluation
	// already inside publishMu completes its publish first and this one
	// publishes last; an evaluation that arrives afterwards sees the latch and
	// publishes nothing. Either way the final published value is 0.
	s.publishMu.Lock()
	defer s.publishMu.Unlock()

	s.mu.Lock()
	s.draining = true
	s.ready = false
	for name, st := range s.states {
		st.OK = false
		st.Category = category
		s.states[name] = st
	}
	s.mu.Unlock()

	if s.onReadiness != nil {
		s.onReadiness(false, nil)
	}
}

// Draining reports whether readiness has been latched off.
func (s *Server) Draining() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.draining
}

func (s *Server) snapshot() (bool, []State) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]State, 0, len(s.states))
	for _, st := range s.states {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return s.ready, out
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	// The identity fields let an operator (and the integration suite) tell the
	// instances apart without correlating container names.
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "alive",
		"application":    s.app,
		"instance":       s.inst,
		"version":        buildinfo.Version,
		"revision":       buildinfo.Revision,
		"build_date":     buildinfo.BuildDate,
		"source_digest":  buildinfo.SourceDigest,
		"go_version":     buildinfo.GoVersion(),
		"policy_version": buildinfo.PolicyVersion,
		"draining":       s.Draining(),
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	ready, states := s.snapshot()
	status := http.StatusServiceUnavailable
	if ready {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"ready":  ready,
		"checks": states,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

// Shutdown stops the HTTP listener.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}
