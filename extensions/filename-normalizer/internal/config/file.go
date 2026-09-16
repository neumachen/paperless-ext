package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/naming"
)

// # Configuration precedence
//
// Four layers, lowest to highest:
//
//  1. Built-in defaults.
//  2. The configuration file named by FN_CONFIG_FILE.
//  3. An environment variable, FN_*.
//  4. The contents of FN_*_FILE, the secret indirection.
//
// So an environment variable overrides the file, and a *_FILE overrides its
// own plain variable. That ordering exists because the file is the deployment's
// declared intent, checked into configuration management, while environment
// variables are how one container instance is tuned differently from its
// siblings -- an instance-specific override must win over the shared file.
//
// Credentials are layer 4 only. They have no file-configuration keys at all:
// FileConfig below has nowhere to put a password, so a credential cannot be
// written into an ordinary configuration example by accident, and the
// effective-configuration surfaces have nothing to redact because there is
// nothing to hold.
//
// Structured policy -- transform rules, include/exclude patterns, temporary
// suffixes -- exists only in the file. There is no environment encoding of a
// rule list, because flattening ordered structure into one variable is how
// ordering bugs get shipped.
//
// # Activation
//
// Configuration is read once at startup and never reloaded. To activate a
// change: validate it (`fn-watcher validate-config`), then restart the
// applications. A rolling restart across differing configurations is safe
// because every accepted job carries the policy identity it was accepted
// under; see PolicyIdentity.

// FileConfigVersion is the only schema version this build accepts.
const FileConfigVersion = 1

// maxConfigBytes bounds the configuration file.
const maxConfigBytes = 1 << 20

// FileConfig is the declarative configuration file.
//
// Unknown fields are rejected rather than ignored, so a misspelled policy key
// fails the run instead of silently doing nothing.
type FileConfig struct {
	Version       int                `json:"version"`
	Storage       *StorageFile       `json:"storage,omitempty"`
	Discovery     *DiscoveryFile     `json:"discovery,omitempty"`
	Normalization *NormalizationFile `json:"normalization,omitempty"`
	Processing    *ProcessingFile    `json:"processing,omitempty"`
}

// StorageFile declares the filesystem roots.
type StorageFile struct {
	Incoming string `json:"incoming,omitempty"`
	Queued   string `json:"queued,omitempty"`
	Staging  string `json:"staging,omitempty"`
	Consume  string `json:"consume,omitempty"`
	Failed   string `json:"failed,omitempty"`
}

// DiscoveryFile declares discovery and completion behaviour.
type DiscoveryFile struct {
	Enabled *bool `json:"enabled,omitempty"`
	// Interval is how often the incoming root is scanned.
	Interval string `json:"interval,omitempty"`
	// StabilityInterval is how long size and modification time must hold still
	// before a submission is considered complete under the stability
	// heuristic. It is a heuristic, not proof.
	StabilityInterval string `json:"stability_interval,omitempty"`
	// Completion selects the completion contract: "stability" or "rename".
	Completion string `json:"completion,omitempty"`
	// Recursive walks subdirectories of the incoming root.
	Recursive *bool `json:"recursive,omitempty"`
	// Batch bounds how many submissions one scan registers.
	Batch int `json:"batch,omitempty"`
	// Include and Exclude SELECT files. They never transform a name.
	// Matching is against the base filename only, never the path.
	Include []string `json:"include,omitempty"`
	Exclude []string `json:"exclude,omitempty"`
	// MatchCaseInsensitive applies (?i) to every include and exclude pattern.
	MatchCaseInsensitive *bool `json:"match_case_insensitive,omitempty"`
	// TemporarySuffixes are recognized partial-upload markers, compared
	// case-insensitively.
	TemporarySuffixes []string `json:"temporary_suffixes,omitempty"`
	// MaxFileBytes rejects submissions larger than this. Zero means no limit.
	MaxFileBytes int64 `json:"max_file_bytes,omitempty"`
}

// NormalizationFile declares the naming policy.
type NormalizationFile struct {
	MaxNameBytes       int               `json:"max_name_bytes,omitempty"`
	MaxExtensionLength int               `json:"max_extension_length,omitempty"`
	MaxCollisionSuffix int               `json:"max_collision_suffix,omitempty"`
	RequireExtension   *bool             `json:"require_extension,omitempty"`
	Rules              []naming.RuleSpec `json:"rules,omitempty"`
}

// ProcessingFile declares operational controls.
type ProcessingFile struct {
	Concurrency         int `json:"concurrency,omitempty"`
	Prefetch            int `json:"prefetch,omitempty"`
	MaxDeliveryAttempts int `json:"max_delivery_attempts,omitempty"`
	// DryRun makes the renamer compute and record outcomes without claiming,
	// moving, renaming or publishing anything.
	DryRun *bool `json:"dry_run,omitempty"`
}

// ErrNoConfigFile reports that no configuration file was requested.
var ErrNoConfigFile = errors.New("no configuration file")

// ConfigFilePath returns the configured path, if any.
func ConfigFilePath() (string, bool) {
	p := strings.TrimSpace(os.Getenv("FN_CONFIG_FILE"))
	return p, p != ""
}

// LoadFileConfig reads and parses the configuration file at path.
//
// Parsing is strict: unknown fields, trailing content and a wrong schema
// version are all errors. Nothing is defaulted here; defaults are applied when
// the file is merged, so an absent field and a zero value stay distinguishable.
func LoadFileConfig(path string) (FileConfig, error) {
	var fc FileConfig

	f, err := os.Open(path)
	if err != nil {
		return fc, fmt.Errorf("configuration file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fc, fmt.Errorf("configuration file: %w", err)
	}
	if info.Size() > maxConfigBytes {
		return fc, fmt.Errorf("configuration file is larger than %d bytes", maxConfigBytes)
	}

	dec := json.NewDecoder(io.LimitReader(f, maxConfigBytes+1))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fc); err != nil {
		return fc, fmt.Errorf("configuration file: %w", err)
	}
	// Reject a second document or trailing junk rather than ignoring it.
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return fc, errors.New("configuration file: unexpected trailing content after the object")
	}
	if fc.Version != FileConfigVersion {
		return fc, fmt.Errorf("configuration file: version %d is not supported; this build accepts version %d",
			fc.Version, FileConfigVersion)
	}
	return fc, nil
}

// Matcher is the compiled selection matcher. It decides eligibility and never
// changes a name.
type Matcher struct {
	Include []*regexp.Regexp
	Exclude []*regexp.Regexp
	// TemporarySuffixes are lowercased.
	TemporarySuffixes []string
	includeSrc        []string
	excludeSrc        []string
}

// Selects reports whether a base filename is eligible, and why not when it is
// not. The reason is a closed-set category, never the filename.
//
// Order: an exclude match always wins; then, if any include pattern is
// configured, at least one must match. With no include patterns everything not
// excluded is eligible.
func (m Matcher) Selects(base string) (bool, string) {
	lower := strings.ToLower(base)
	for _, suffix := range m.TemporarySuffixes {
		if strings.HasSuffix(lower, suffix) {
			return false, "temporary_suffix"
		}
	}
	for _, re := range m.Exclude {
		if re.MatchString(base) {
			return false, "excluded_by_pattern"
		}
	}
	if len(m.Include) == 0 {
		return true, ""
	}
	for _, re := range m.Include {
		if re.MatchString(base) {
			return true, ""
		}
	}
	return false, "not_included"
}

// Patterns returns the configured patterns for the effective-configuration
// surface. They are operator-authored, not document-derived.
func (m Matcher) Patterns() (include, exclude []string) { return m.includeSrc, m.excludeSrc }

// CompletionContract names the accepted completion evidence.
type CompletionContract string

const (
	// CompletionStability accepts a submission whose size and modification
	// time have held still for the stability interval. This is a heuristic.
	CompletionStability CompletionContract = "stability"
	// CompletionRename accepts only a submission whose producer wrote a
	// temporary name and renamed it into place. The rename is the completion
	// signal; a file still carrying a temporary suffix is never eligible, and
	// the stability interval is still applied as a second gate.
	CompletionRename CompletionContract = "rename"
)

// Discovery is the compiled discovery configuration.
type Discovery struct {
	Enabled           bool
	Interval          time.Duration
	StabilityInterval time.Duration
	Completion        CompletionContract
	Recursive         bool
	Batch             int
	MaxFileBytes      int64
	Matcher           Matcher
}

// Policy bundles the naming policy with its identity.
type Policy struct {
	naming.Policy
	// Identity is the policy version plus a fingerprint of every
	// policy-affecting setting. It is stamped on each job at registration.
	Identity string
}

// defaults for the discovery layer.
const (
	defaultDiscoveryInterval = 10 * time.Second
	defaultStabilityInterval = 30 * time.Second
	defaultDiscoveryBatch    = 100
	minDiscoveryInterval     = time.Second
	maxDiscoveryInterval     = time.Hour
	minStabilityInterval     = 0
	maxStabilityInterval     = 24 * time.Hour
	maxSelectionPatternLen   = 512
	maxSelectionPatterns     = 64
	maxTemporarySuffixes     = 32
)

var defaultTemporarySuffixes = []string{".part", ".tmp", ".crdownload", ".partial", ".filepart"}

// compileDiscovery builds the discovery configuration from the file layer.
func compileDiscovery(l *loader, df *DiscoveryFile) Discovery {
	d := Discovery{
		Enabled:           false,
		Interval:          defaultDiscoveryInterval,
		StabilityInterval: defaultStabilityInterval,
		Completion:        CompletionStability,
		Recursive:         false,
		Batch:             defaultDiscoveryBatch,
		Matcher:           Matcher{TemporarySuffixes: append([]string(nil), defaultTemporarySuffixes...)},
	}

	if df != nil {
		if df.Enabled != nil {
			d.Enabled = *df.Enabled
		}
		if df.Recursive != nil {
			d.Recursive = *df.Recursive
		}
		d.Interval = l.fileDuration("discovery.interval", df.Interval, d.Interval, minDiscoveryInterval, maxDiscoveryInterval)
		d.StabilityInterval = l.fileDuration("discovery.stability_interval", df.StabilityInterval, d.StabilityInterval, minStabilityInterval, maxStabilityInterval)
		if df.Batch != 0 {
			if df.Batch < 1 || df.Batch > 10000 {
				l.fail("discovery.batch must be between 1 and 10000")
			} else {
				d.Batch = df.Batch
			}
		}
		if df.MaxFileBytes < 0 {
			l.fail("discovery.max_file_bytes must not be negative")
		} else {
			d.MaxFileBytes = df.MaxFileBytes
		}
		switch df.Completion {
		case "":
		case string(CompletionStability), string(CompletionRename):
			d.Completion = CompletionContract(df.Completion)
		default:
			l.fail("discovery.completion must be %q or %q", CompletionStability, CompletionRename)
		}
		if df.TemporarySuffixes != nil {
			d.Matcher.TemporarySuffixes = compileSuffixes(l, df.TemporarySuffixes)
		}
		ci := false
		if df.MatchCaseInsensitive != nil {
			ci = *df.MatchCaseInsensitive
		}
		d.Matcher.Include, d.Matcher.includeSrc = compilePatterns(l, "discovery.include", df.Include, ci)
		d.Matcher.Exclude, d.Matcher.excludeSrc = compilePatterns(l, "discovery.exclude", df.Exclude, ci)
	}

	// The environment may still override the enable flag and the intervals,
	// which is what lets one instance be turned off without editing the file.
	d.Enabled = l.boolVal("FN_DISCOVERY_ENABLED", d.Enabled)
	d.Interval = l.duration("FN_DISCOVERY_INTERVAL", d.Interval, minDiscoveryInterval, maxDiscoveryInterval)
	d.StabilityInterval = l.duration("FN_DISCOVERY_STABILITY_INTERVAL", d.StabilityInterval, minStabilityInterval, maxStabilityInterval)
	d.Batch = l.intVal("FN_DISCOVERY_BATCH", d.Batch, 1, 10000)
	return d
}

func compilePatterns(l *loader, where string, src []string, caseInsensitive bool) ([]*regexp.Regexp, []string) {
	if len(src) > maxSelectionPatterns {
		l.fail("%s: more than %d patterns", where, maxSelectionPatterns)
		return nil, nil
	}
	var out []*regexp.Regexp
	var kept []string
	for i, p := range src {
		if p == "" {
			l.fail("%s[%d]: empty pattern", where, i)
			continue
		}
		if len(p) > maxSelectionPatternLen {
			l.fail("%s[%d]: pattern longer than %d characters", where, i, maxSelectionPatternLen)
			continue
		}
		expr := p
		if caseInsensitive {
			expr = "(?i)" + expr
		}
		re, err := regexp.Compile(expr)
		if err != nil {
			l.fail("%s[%d]: %v", where, i, err)
			continue
		}
		out = append(out, re)
		kept = append(kept, p)
	}
	return out, kept
}

func compileSuffixes(l *loader, src []string) []string {
	if len(src) > maxTemporarySuffixes {
		l.fail("discovery.temporary_suffixes: more than %d entries", maxTemporarySuffixes)
		return nil
	}
	var out []string
	for i, s := range src {
		if s == "" || !strings.HasPrefix(s, ".") {
			l.fail("discovery.temporary_suffixes[%d]: must be non-empty and start with a dot", i)
			continue
		}
		out = append(out, strings.ToLower(s))
	}
	return out
}

// compilePolicy builds the naming policy and its identity.
func compilePolicy(l *loader, nf *NormalizationFile) Policy {
	base := naming.DefaultPolicy()

	if nf != nil {
		if nf.MaxNameBytes != 0 {
			if nf.MaxNameBytes < 16 || nf.MaxNameBytes > 4096 {
				l.fail("normalization.max_name_bytes must be between 16 and 4096")
			} else {
				base.MaxNameBytes = nf.MaxNameBytes
			}
		}
		if nf.MaxExtensionLength != 0 {
			if nf.MaxExtensionLength < 1 || nf.MaxExtensionLength > 64 {
				l.fail("normalization.max_extension_length must be between 1 and 64")
			} else {
				base.MaxExtensionLen = nf.MaxExtensionLength
			}
		}
		if nf.MaxCollisionSuffix != 0 {
			if nf.MaxCollisionSuffix < 1 || nf.MaxCollisionSuffix > 100000 {
				l.fail("normalization.max_collision_suffix must be between 1 and 100000")
			} else {
				base.MaxCollisionSuffix = nf.MaxCollisionSuffix
			}
		}
		if nf.RequireExtension != nil {
			base.RequireExtension = *nf.RequireExtension
		}
		rules, problems := naming.CompileRules(nf.Rules)
		for _, p := range problems {
			l.fail("%s", p)
		}
		base.Rules = rules
	}

	// A policy that cannot name its own probe corpus, or that does not
	// converge, is rejected before anything is accepted for processing.
	for _, p := range base.SelfCheck() {
		l.fail("%s", p)
	}

	return Policy{Policy: base, Identity: policyIdentity(base, nf)}
}

// policyIdentity fingerprints every setting that can change a produced name.
//
// The fingerprint covers the numeric bounds and the ordered rule list, so two
// deployments that differ only in, say, rule order produce different
// identities. It deliberately excludes everything that cannot change a name --
// concurrency, intervals, roots -- so an operational tweak does not strand
// queued work.
func policyIdentity(p naming.Policy, nf *NormalizationFile) string {
	h := sha256.New()
	fmt.Fprintf(h, "v=%s\n", naming.PolicyVersion)
	fmt.Fprintf(h, "max_name_bytes=%d\n", p.MaxNameBytes)
	fmt.Fprintf(h, "max_extension_length=%d\n", p.MaxExtensionLen)
	fmt.Fprintf(h, "max_collision_suffix=%d\n", p.MaxCollisionSuffix)
	fmt.Fprintf(h, "require_extension=%t\n", p.RequireExtension)
	if nf != nil {
		for i, r := range nf.Rules {
			fmt.Fprintf(h, "rule[%d]=%s|%s|%s|%t|%t\n",
				i, r.Name, r.Pattern, r.Replacement, r.All, r.CaseInsensitive)
		}
	}
	return naming.PolicyVersion + "+" + hex.EncodeToString(h.Sum(nil))[:12]
}

// fileDuration parses a duration string from the file layer.
func (l *loader) fileDuration(where, raw string, def, min, max time.Duration) time.Duration {
	if strings.TrimSpace(raw) == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		l.fail("%s: %q is not a duration", where, raw)
		return def
	}
	if d < min || d > max {
		l.fail("%s: %s is outside %s..%s", where, d, min, max)
		return def
	}
	return d
}

// applyStorageFile lets the file supply storage roots beneath the environment.
func applyStorageFile(sf *StorageFile, defs map[string]string) {
	if sf == nil {
		return
	}
	set := func(key, v string) {
		if strings.TrimSpace(v) != "" {
			defs[key] = v
		}
	}
	set("FN_STORAGE_INCOMING", sf.Incoming)
	set("FN_STORAGE_QUEUED", sf.Queued)
	set("FN_STORAGE_STAGING", sf.Staging)
	set("FN_STORAGE_CONSUME", sf.Consume)
	set("FN_STORAGE_FAILED", sf.Failed)
}

// validateStorageRoots rejects root sets that cannot be safe.
//
// Containment is enforced at use time against these roots, so the roots
// themselves must be absolute, distinct, and non-nested: a staging root inside
// the incoming root would make a staged copy look like a new submission, and a
// consume root inside staging would expose partial files to the consumer.
func validateStorageRoots(l *loader, s Storage) {
	roots := map[string]string{
		"incoming": s.Incoming,
		"queued":   s.Queued,
		"staging":  s.Staging,
		"consume":  s.Consume,
		"failed":   s.Failed,
	}
	names := make([]string, 0, len(roots))
	for n := range roots {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, n := range names {
		p := roots[n]
		if p == "" {
			l.fail("storage.%s is required", n)
			continue
		}
		if !filepath.IsAbs(p) {
			l.fail("storage.%s must be an absolute path", n)
		}
		if p != filepath.Clean(p) {
			l.fail("storage.%s must be a clean path", n)
		}
	}

	for i, a := range names {
		for _, b := range names[i+1:] {
			pa, pb := roots[a], roots[b]
			if pa == "" || pb == "" {
				continue
			}
			switch {
			case pa == pb:
				l.fail("storage.%s and storage.%s are the same path", a, b)
			case within(pb, pa):
				l.fail("storage.%s is inside storage.%s", b, a)
			case within(pa, pb):
				l.fail("storage.%s is inside storage.%s", a, b)
			}
		}
	}
}

// within reports whether child is inside parent.
func within(child, parent string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".."
}

// ValidateDocument checks a candidate configuration document.
//
// It is used by the gRPC validation RPC and it deliberately does not touch the
// environment, the filesystem or any running state: the document is parsed and
// compiled in isolation, and the policy identity it would produce is returned
// so an operator can see in advance whether applying it would strand queued
// work. Nothing is activated; activation is a restart.
func ValidateDocument(body []byte) (policyIdentity string, problems []string) {
	fc, err := parseDocument(body)
	if err != nil {
		return "", []string{err.Error()}
	}
	l := &loader{fileDefaults: map[string]string{}}
	policy := compilePolicy(l, fc.Normalization)
	// Discovery is compiled too, so a bad selection pattern is reported by
	// validation rather than at the next restart. The environment overrides it
	// consults are the validating process's own, which is the honest answer to
	// "would this document work here?".
	compileDiscovery(l, fc.Discovery)
	validateDocumentShape(l, fc)

	if len(l.problems) > 0 {
		sort.Strings(l.problems)
		return policy.Identity, dedupe(l.problems)
	}
	return policy.Identity, nil
}

// PolicyFromDocument compiles just the naming policy from a document, for a
// preview against a candidate configuration.
func PolicyFromDocument(body []byte) (Policy, []string) {
	fc, err := parseDocument(body)
	if err != nil {
		return Policy{}, []string{err.Error()}
	}
	l := &loader{fileDefaults: map[string]string{}}
	policy := compilePolicy(l, fc.Normalization)
	if len(l.problems) > 0 {
		sort.Strings(l.problems)
		return Policy{}, dedupe(l.problems)
	}
	return policy, nil
}

// parseDocument applies the same strict parse LoadFileConfig uses.
func parseDocument(body []byte) (FileConfig, error) {
	var fc FileConfig
	if len(body) > maxConfigBytes {
		return fc, fmt.Errorf("configuration document is larger than %d bytes", maxConfigBytes)
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fc); err != nil {
		return fc, fmt.Errorf("configuration document: %w", err)
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return fc, errors.New("configuration document: unexpected trailing content after the object")
	}
	if fc.Version != FileConfigVersion {
		return fc, fmt.Errorf("configuration document: version %d is not supported; this build accepts version %d",
			fc.Version, FileConfigVersion)
	}
	return fc, nil
}

// validateDocumentShape checks the parts that do not need the environment.
func validateDocumentShape(l *loader, fc FileConfig) {
	if fc.Storage != nil {
		for name, p := range map[string]string{
			"incoming": fc.Storage.Incoming, "queued": fc.Storage.Queued,
			"staging": fc.Storage.Staging, "consume": fc.Storage.Consume,
			"failed": fc.Storage.Failed,
		} {
			if p == "" {
				continue
			}
			if !filepath.IsAbs(p) {
				l.fail("storage.%s must be an absolute path", name)
			}
			if p != filepath.Clean(p) {
				l.fail("storage.%s must be a clean path", name)
			}
		}
	}
	if fc.Processing != nil {
		if c := fc.Processing.Concurrency; c != 0 && (c < 1 || c > 64) {
			l.fail("processing.concurrency must be between 1 and 64")
		}
		if pf := fc.Processing.Prefetch; pf != 0 && (pf < 1 || pf > 1000) {
			l.fail("processing.prefetch must be between 1 and 1000")
		}
		if a := fc.Processing.MaxDeliveryAttempts; a != 0 && (a < 1 || a > 100) {
			l.fail("processing.max_delivery_attempts must be between 1 and 100")
		}
		if fc.Processing.Concurrency > 0 && fc.Processing.Prefetch > 0 &&
			fc.Processing.Prefetch < fc.Processing.Concurrency {
			l.fail("processing.prefetch must be at least processing.concurrency")
		}
	}
}
