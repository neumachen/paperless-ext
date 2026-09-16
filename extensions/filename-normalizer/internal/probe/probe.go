// Package probe implements the health and readiness subcommands both binaries
// expose.
//
// These exist because the runtime images are built FROM scratch: there is no
// shell, no curl and no wget inside them. A container healthcheck therefore
// has to be the application binary itself, and — crucially — it has to ask the
// *running* process how it is, rather than re-running a command that only
// proves a binary can start.
package probe

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Observation is one readiness sample, written as a JSON line.
type Observation struct {
	At       string `json:"at"`
	Target   string `json:"target"`
	Status   int    `json:"http_status"`
	Ready    bool   `json:"ready"`
	Draining bool   `json:"draining,omitempty"`
	Error    string `json:"error,omitempty"`
	Checks   []struct {
		Name     string `json:"name"`
		Required bool   `json:"required"`
		OK       bool   `json:"ok"`
		Category string `json:"category,omitempty"`
	} `json:"checks,omitempty"`
}

var client = &http.Client{Timeout: 5 * time.Second}

// Healthcheck asks the locally running process for its liveness document.
//
// It is the container healthcheck: it opens a connection to the address this
// process is serving on, so it fails if the HTTP surface is gone even though
// the binary is perfectly capable of starting.
func Healthcheck(args []string) int {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", os.Getenv("FN_HTTP_ADDR"), "address the application serves on")
	requireReady := fs.Bool("require-ready", false, "fail unless the application also reports itself ready")
	timeout := fs.Duration("timeout", 4*time.Second, "overall timeout")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	target := localURL(*addr)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	path := "/healthz"
	if *requireReady {
		path = "/readyz"
	}
	body, status, err := get(ctx, target+path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %s is not answering: %v\n", target+path, err)
		return 1
	}
	if status != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: %s returned status %d: %s\n", target+path, status, trim(body))
		return 1
	}
	fmt.Printf("%s\n", trim(body))
	return 0
}

// Probe reads /readyz from one or more targets, optionally repeatedly.
//
// With --require-ready it exits non-zero unless every target's final
// observation is ready, so a Make target that reports ready:false also fails
// instead of printing a problem and succeeding.
func Probe(args []string) int {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	requireReady := fs.Bool("require-ready", false, "exit non-zero unless every target's last observation is ready")
	interval := fs.Duration("interval", 0, "sample repeatedly at this interval")
	duration := fs.Duration("duration", 0, "keep sampling for this long (requires -interval)")
	output := fs.String("output", "", "append JSON-line observations to this file as well as stdout")
	quiet := fs.Bool("quiet", false, "write observations only to the output file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	targets := fs.Args()
	if len(targets) == 0 {
		fmt.Fprintln(os.Stderr, "probe: at least one target URL is required")
		return 2
	}

	var sink io.WriteCloser
	if *output != "" {
		if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "probe: %v\n", err)
			return 2
		}
		f, err := os.OpenFile(*output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "probe: %v\n", err)
			return 2
		}
		sink = f
		defer func() { _ = f.Close() }()
	}

	deadline := time.Now()
	if *interval > 0 {
		if *duration <= 0 {
			*duration = 10 * time.Second
		}
		deadline = time.Now().Add(*duration)
	}

	last := map[string]Observation{}
	samples := 0
	for {
		for _, target := range targets {
			obs := sample(target)
			last[target] = obs
			samples++
			line, _ := json.Marshal(obs)
			if sink != nil {
				fmt.Fprintf(sink, "%s\n", line)
			}
			if !*quiet || sink == nil {
				fmt.Printf("%s\n", line)
			}
		}
		if *interval <= 0 || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(*interval)
	}

	if !*requireReady {
		return 0
	}
	code := 0
	for _, target := range targets {
		obs := last[target]
		if !obs.Ready {
			fmt.Fprintf(os.Stderr, "probe: %s is not ready (status %d)\n", target, obs.Status)
			code = 1
		}
	}
	if code == 0 {
		fmt.Fprintf(os.Stderr, "probe: all %d target(s) ready after %d observation(s)\n", len(targets), samples)
	}
	return code
}

func sample(target string) Observation {
	obs := Observation{
		At:     time.Now().UTC().Format(time.RFC3339Nano),
		Target: target,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body, status, err := get(ctx, strings.TrimRight(target, "/")+"/readyz")
	obs.Status = status
	if err != nil {
		// The classification stays coarse: this output is an artefact, and a
		// raw transport error can carry more than it needs to.
		obs.Error = "unreachable"
		return obs
	}
	if uerr := json.Unmarshal(body, &obs); uerr != nil {
		obs.Error = "unparseable"
	}
	// Unmarshalling into obs overwrites At/Target/Status, so restore them.
	obs.At = time.Now().UTC().Format(time.RFC3339Nano)
	obs.Target = target
	obs.Status = status
	return obs
}

func get(ctx context.Context, url string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

// localURL turns a listen address into a loopback URL.
func localURL(addr string) string {
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	if strings.HasPrefix(addr, ":") {
		return "http://127.0.0.1" + addr
	}
	host, port, found := strings.Cut(addr, ":")
	if !found {
		return "http://127.0.0.1:" + addr
	}
	if host == "" || host == "0.0.0.0" || host == "[::]" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + host + ":" + port
}

func trim(b []byte) string { return strings.TrimSpace(string(b)) }

// ErrUnknown is returned for an unrecognised subcommand.
var ErrUnknown = errors.New("unknown subcommand")
