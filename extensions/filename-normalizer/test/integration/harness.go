//go:build integration

// Package integration holds the containerized integration suite.
//
// Two rules shape everything here.
//
// No mocks. Every dependency is the real one: the real PostgreSQL cluster, the
// real RabbitMQ node, the real filesystem roles, and the real applications
// running in their own containers. The suite imports the applications' own
// packages, so what it exercises is the integration code that ships, not a
// reimplementation of it.
//
// No silent skipping. A required dependency that is absent fails the suite.
// Phases that deliberately take a dependency down declare it, and the suite
// also fails if a dependency that was supposed to be down is reachable.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/broker"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// Phase names the orchestrated stack state the current run executes against.
// The host orchestrator runs every phase in sequence; a test that does not
// apply to the active phase reports why instead of silently passing.
type Phase string

const (
	PhaseBaseline Phase = "baseline"
	// PhaseTelemetry runs after baseline with the logs re-collected, so the
	// log-content assertions see the records baseline produced.
	PhaseTelemetry        Phase = "telemetry"
	PhaseReplicaDown      Phase = "replica_down"
	PhaseReplicaRecovered Phase = "replica_recovered"
	PhasePrimaryRestarted Phase = "primary_restarted"
	PhasePrimaryDown      Phase = "primary_down"
	PhasePrimaryRecovered Phase = "primary_recovered"
	PhaseRabbitRestarted  Phase = "rabbit_restarted"
	PhaseRabbitDown       Phase = "rabbit_down"
	PhaseRabbitRecovered  Phase = "rabbit_recovered"
	PhaseStorageFault     Phase = "storage_fault"
	PhasePrimaryKilled    Phase = "primary_killed"
	PhaseDrained          Phase = "drained"
	PhaseDrainUnderLoad   Phase = "drain_under_load"
	PhaseSchemaFault      Phase = "schema_fault"
	PhaseBrokerTorn       Phase = "broker_torn"
	// PhaseWatcherDrained runs after the watcher has been terminated with
	// SIGTERM following the broker interruptions.
	PhaseWatcherDrained Phase = "watcher_drained"
	// PhaseStackPrivacy runs after the telemetry phase has produced a slow
	// database statement, with every stack log re-collected.
	// PhaseNormalization runs the end-to-end document tests: real synthetic
	// files placed in the real incoming volume and taken through the real
	// watcher, broker, renamers and destination.
	PhaseNormalization Phase = "normalization"

	PhaseStackPrivacy Phase = "stack_privacy"
	// PhaseFinalPrivacy runs last, after every outage, recovery and
	// termination in the run, with every stack log re-collected again. The
	// mid-run pass cannot cover logs that do not exist yet.
	PhaseFinalPrivacy Phase = "final_privacy"
	PhaseRequireDeps  Phase = "require_dependencies"
)

// Env is the resolved suite environment.
type Env struct {
	Cfg config.RenamerConfig

	Phase      Phase
	RunID      string
	ExpectDown map[string]bool

	WatcherURL  string
	RenamerURLs []string
	FaultURL    string
	NoSchemaURL string
	MgmtURL     string
	Evidence    string

	Log *slog.Logger
}

var (
	env     *Env
	envOnce sync.Once
)

// Suite returns the shared environment, constructing it once.
func Suite() *Env {
	envOnce.Do(func() { env = buildEnv() })
	return env
}

func buildEnv() *Env {
	cfg, err := config.LoadRenamer()
	if err != nil {
		fatalf("the suite's own configuration is invalid: %v", err)
	}

	phase := Phase(getenv("FN_TEST_PHASE", string(PhaseBaseline)))
	runID := getenv("FN_TEST_RUN_ID", "")
	if runID == "" {
		fatalf("FN_TEST_RUN_ID is required: the orchestrator must supply a stable run identifier so phases can find each other's rows")
	}

	expect := map[string]bool{}
	for _, d := range strings.Split(getenv("FN_TEST_EXPECT_DOWN", ""), ",") {
		if d = strings.TrimSpace(d); d != "" {
			expect[d] = true
		}
	}

	var renamers []string
	for _, u := range strings.Split(getenv("FN_TEST_RENAMER_URLS", ""), ",") {
		if u = strings.TrimSpace(u); u != "" {
			renamers = append(renamers, u)
		}
	}

	evidence := getenv("FN_TEST_EVIDENCE_DIR", "/evidence")
	if err := os.MkdirAll(filepath.Join(evidence, "state"), 0o755); err != nil {
		fatalf("cannot prepare the evidence directory: %v", err)
	}

	level, _ := logging.ParseLevel(cfg.LogLevel)
	return &Env{
		Cfg:         cfg,
		Phase:       phase,
		RunID:       runID,
		ExpectDown:  expect,
		WatcherURL:  getenv("FN_TEST_WATCHER_URL", "http://watcher:8080"),
		RenamerURLs: renamers,
		FaultURL:    getenv("FN_TEST_FAULT_URL", "http://watcher-storage-fault:8080"),
		NoSchemaURL: getenv("FN_TEST_NO_SCHEMA_URL", "http://renamer-no-schema:8080"),
		MgmtURL:     getenv("FN_TEST_RABBITMQ_MGMT_URL", "http://rabbitmq:15672"),
		Evidence:    evidence,
		Log: logging.New(os.Stderr, logging.Options{
			Level: level, Application: "integration", Instance: "suite",
		}),
	}
}

// OnlyIn reports the test as not applicable outside the given phases, naming
// the active phase so the omission is visible in the output.
func (e *Env) OnlyIn(t *testing.T, phases ...Phase) {
	t.Helper()
	for _, p := range phases {
		if e.Phase == p {
			return
		}
	}
	names := make([]string, len(phases))
	for i, p := range phases {
		names[i] = string(p)
	}
	t.Skipf("not applicable in phase %q; this assertion runs in phase(s) %s",
		e.Phase, strings.Join(names, ", "))
}

// ExpectedDown reports whether the orchestrator deliberately interrupted a
// dependency for this phase.
func (e *Env) ExpectedDown(dep string) bool { return e.ExpectDown[dep] }

// Ledger opens the real durable store using the application's own package.
func (e *Env) Ledger(t *testing.T) *ledger.Ledger {
	t.Helper()
	led, err := ledger.Open(context.Background(), ledger.Options{
		Config:    e.Cfg.Database,
		Logger:    e.Log.With(slog.String("component", "ledger")),
		Actor:     "integration/" + e.RunID,
		OpTimeout: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	t.Cleanup(led.Close)
	return led
}

// Broker starts the real connection supervisor using the application's own
// package and waits for it to connect.
func (e *Env) Broker(t *testing.T) (*broker.Connection, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	conn := broker.Dial(broker.Options{
		Config: e.Cfg.Broker,
		Logger: e.Log.With(slog.String("component", "broker")),
		Name:   "integration/" + e.RunID,
	})
	done := make(chan struct{})
	go func() { defer close(done); conn.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		conn.Shutdown()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	})
	if !waitFor(20*time.Second, conn.Connected) {
		t.Fatalf("broker did not connect within 20s; the suite requires a real broker and will not substitute one")
	}
	return conn, ctx
}

// IsolatedTopology names a durable topology owned by this test only, declared
// through the application's own Declare function. Teardown removes exactly
// these resources and nothing else.
func (e *Env) IsolatedTopology(t *testing.T, suffix string) broker.Topology {
	t.Helper()
	base := fmt.Sprintf("fn_test.%s.%s", e.RunID, suffix)
	return broker.Topology{
		Exchange:      base,
		Queue:         base + ".q",
		RoutingKey:    "normalize",
		DeadLetterX:   base + ".dlx",
		DeadLetterQ:   base + ".dead",
		DeliveryLimit: e.Cfg.Broker.DeliveryLimit,
	}
}

// DeclareIsolated declares an owned topology and registers its removal.
func (e *Env) DeclareIsolated(t *testing.T, conn *broker.Connection, suffix string) broker.Topology {
	t.Helper()
	top := e.IsolatedTopology(t, suffix)
	ch, _, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	if err := broker.Declare(ch, top); err != nil {
		_ = ch.Close()
		t.Fatalf("declare isolated topology: %v", err)
	}
	_ = ch.Close()

	t.Cleanup(func() {
		cleanup, _, cerr := conn.Channel()
		if cerr != nil {
			t.Logf("teardown: could not open a channel to remove %s (%v); it remains for inspection", top.Queue, cerr)
			return
		}
		defer func() { _ = cleanup.Close() }()
		for _, q := range []string{top.Queue, top.DeadLetterQ} {
			if _, derr := cleanup.QueueDelete(q, false, false, false); derr != nil {
				t.Logf("teardown: queue %s not removed: %v", q, derr)
			}
		}
		for _, x := range []string{top.Exchange, top.DeadLetterX} {
			if derr := cleanup.ExchangeDelete(x, false, false); derr != nil {
				t.Logf("teardown: exchange %s not removed: %v", x, derr)
			}
		}
	})
	return top
}

// --- cross-phase state -----------------------------------------------------

// SaveState records a value for a later phase to read. The evidence directory
// is a bind mount shared by every phase of one run.
func (e *Env) SaveState(t *testing.T, key, value string) {
	t.Helper()
	path := filepath.Join(e.Evidence, "state", e.RunID+"."+key)
	if err := os.WriteFile(path, []byte(value), 0o644); err != nil {
		t.Fatalf("save state %q: %v", key, err)
	}
}

// LoadState reads a value written by an earlier phase.
func (e *Env) LoadState(t *testing.T, key string) string {
	t.Helper()
	path := filepath.Join(e.Evidence, "state", e.RunID+"."+key)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("phase %q expected state %q from an earlier phase but it is missing: %v", e.Phase, key, err)
	}
	return strings.TrimSpace(string(data))
}

// SlowStatementThresholdMS is the primary's configured
// log_min_duration_statement, in milliseconds. The privacy evidence has to
// produce a statement slower than this for PostgreSQL to log it at all.
func (e *Env) SlowStatementThresholdMS() int {
	raw := getenv("FN_TEST_SLOW_STATEMENT_MS", "1000")
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n <= 0 {
		return 1000
	}
	return n
}

// LatchRaceIterations is how many evaluation/withdrawal races the readiness
// latch oracle runs. The window it targets is a few instructions wide, so the
// count has to be high enough for the oracle to be able to fail.
func (e *Env) LatchRaceIterations() int {
	raw := getenv("FN_TEST_LATCH_ITERATIONS", "3000")
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 100 {
		return 3000
	}
	return n
}

// DrainCopies is how many times each registered job's reference is published
// into the work queue for the drain exercise.
//
// Depth has to come from somewhere, and registration is the slow half.
// Duplicate messages are explicitly permitted by the contract and must be
// processed safely, so publishing each reference several times is both a
// legitimate way to build a backlog and an additional exercise of that rule.
func (e *Env) DrainCopies() int {
	raw := getenv("FN_TEST_DRAIN_COPIES", "25")
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 1 {
		return 25
	}
	return n
}

// DrainBatchSize is how many jobs the drain phase registers and publishes.
//
// It only needs to be deep enough that the renamers are still working through
// the queue when the orchestrator terminates one of them, and small enough
// that the whole batch reaches a durable outcome promptly afterwards.
func (e *Env) DrainBatchSize() int {
	raw := getenv("FN_TEST_DRAIN_BATCH", "400")
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 50 {
		return 400
	}
	return n
}

// Since returns the instant the orchestrator injected the named fault.
//
// Log assertions about a fault must read only that fault's own interval: the
// collected logs hold the whole run, so scanning all of them would let an
// error produced by an earlier phase satisfy a later phase's claim.
func (e *Env) Since(t *testing.T, label string) time.Time {
	t.Helper()
	raw := strings.TrimSpace(e.LoadState(t, "since-"+label))
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("fault interval marker %q is not an RFC3339 timestamp: %q (%v)", label, raw, err)
	}
	return ts
}

// --- RabbitMQ management API ------------------------------------------------

// QueueStats is the broker's own view of a queue.
//
// messages_unacknowledged is the only authoritative measure of how many
// deliveries the broker has handed out and not had settled. A count kept by
// the consumer measures running handlers, which its own concurrency semaphore
// already bounds, so it cannot establish that the AMQP prefetch window is
// being respected.
type QueueStats struct {
	Name           string `json:"name"`
	Ready          int    `json:"messages_ready"`
	Unacknowledged int    `json:"messages_unacknowledged"`
	Total          int    `json:"messages"`
	Consumers      int    `json:"consumers"`
	Type           string `json:"type"`
	Durable        bool   `json:"durable"`
}

// UnackedOrDerived returns the broker's unacknowledged count.
//
// The management API refreshes its per-queue statistics on an interval, and a
// freshly declared queue can report messages_unacknowledged as 0 while the
// total and ready counts are already populated. Both numbers come from the
// broker, and total minus ready is the same quantity, so the derivation is
// used when the dedicated field has not caught up. Nothing here substitutes a
// locally kept count: the oracle stays the broker's own view either way, and
// the report records which field was used.
func (q QueueStats) UnackedOrDerived() (int, string) {
	if q.Unacknowledged > 0 {
		return q.Unacknowledged, "messages_unacknowledged"
	}
	if derived := q.Total - q.Ready; derived > 0 {
		return derived, "messages_minus_messages_ready"
	}
	return 0, "messages_unacknowledged"
}

// QueueStats reads one queue from the broker's management API.
func (e *Env) QueueStats(t *testing.T, queue string) QueueStats {
	t.Helper()
	st, _, err := e.queueStatsRaw(queue)
	if err != nil {
		t.Fatalf("read broker queue stats for %q: %v", queue, err)
	}
	return st
}

// QueueStatsRaw also returns the raw management response, for evidence and for
// diagnosing a field the broker reports differently than expected.
func (e *Env) QueueStatsRaw(t *testing.T, queue string) (QueueStats, string) {
	t.Helper()
	st, raw, err := e.queueStatsRaw(queue)
	if err != nil {
		t.Fatalf("read broker queue stats for %q: %v", queue, err)
	}
	return st, raw
}

func (e *Env) queueStatsRaw(queue string) (QueueStats, string, error) {
	vhost := e.Cfg.Broker.VHost
	if vhost == "" {
		vhost = "/"
	}
	url := fmt.Sprintf("%s/api/queues/%s/%s",
		strings.TrimRight(e.MgmtURL, "/"), urlPathEscape(vhost), urlPathEscape(queue))

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return QueueStats{}, "", err
	}
	req.SetBasicAuth(e.Cfg.Broker.User, e.Cfg.Broker.Password)

	resp, err := httpClient.Do(req)
	if err != nil {
		return QueueStats{}, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return QueueStats{}, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return QueueStats{}, "", fmt.Errorf("management API returned status %d", resp.StatusCode)
	}
	var st QueueStats
	if err := json.Unmarshal(body, &st); err != nil {
		return QueueStats{}, string(body), fmt.Errorf("parse management response: %w", err)
	}
	return st, string(body), nil
}

// urlPathEscape percent-encodes a single path segment. The default vhost is
// "/" and queue names contain dots, so the segment must be escaped rather
// than concatenated.
func urlPathEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// OptionalState reads cross-phase state that may legitimately be absent,
// reporting whether it was found instead of failing.
func (e *Env) OptionalState(key string) (string, bool) {
	data, err := os.ReadFile(filepath.Join(e.Evidence, "state", e.RunID+"."+key))
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(data)), true
}

// --- evidence --------------------------------------------------------------

// WriteEvidence stores a named artefact for the return packet.
func (e *Env) WriteEvidence(t *testing.T, name string, body []byte) string {
	t.Helper()
	dir := filepath.Join(e.Evidence, string(e.Phase))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("prepare evidence dir: %v", err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write evidence %q: %v", name, err)
	}
	t.Logf("evidence: %s", path)
	return path
}

// --- HTTP ------------------------------------------------------------------

// Readiness is the parsed /readyz document.
type Readiness struct {
	Ready  bool `json:"ready"`
	Checks []struct {
		Name      string `json:"name"`
		Required  bool   `json:"required"`
		OK        bool   `json:"ok"`
		Category  string `json:"category"`
		CheckedAt string `json:"checked_at"`
	} `json:"checks"`
}

// Check finds one named readiness check.
func (r Readiness) Check(name string) (bool, string, bool) {
	for _, c := range r.Checks {
		if c.Name == name {
			return c.OK, c.Category, true
		}
	}
	return false, "", false
}

var httpClient = &http.Client{Timeout: 10 * time.Second}

// GetReadiness reads and parses an application's readiness endpoint.
func GetReadiness(url string) (Readiness, int, []byte, error) {
	body, status, err := httpGet(url + "/readyz")
	if err != nil {
		return Readiness{}, status, body, err
	}
	var r Readiness
	if err := json.Unmarshal(body, &r); err != nil {
		return Readiness{}, status, body, fmt.Errorf("parse readiness: %w", err)
	}
	return r, status, body, nil
}

// GetMetrics fetches an application's Prometheus exposition.
func GetMetrics(url string) (string, error) {
	body, status, err := httpGet(url + "/metrics")
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("metrics returned status %d", status)
	}
	return string(body), nil
}

// GetHealth fetches an application's liveness document.
func GetHealth(url string) ([]byte, int, error) {
	body, status, err := httpGet(url + "/healthz")
	return body, status, err
}

func httpGet(url string) ([]byte, int, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return body, resp.StatusCode, err
}

// --- helpers ---------------------------------------------------------------

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "integration suite: "+format+"\n", args...)
	os.Exit(1)
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return cond()
}

// waitForErr polls fn until it returns nil, reporting the last error.
func waitForErr(d time.Duration, fn func() error) error {
	deadline := time.Now().Add(d)
	var last error
	for time.Now().Before(deadline) {
		last = fn()
		if last == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	if last == nil {
		last = fmt.Errorf("condition never evaluated")
	}
	return last
}
