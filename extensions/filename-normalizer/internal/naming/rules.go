package naming

import (
	"fmt"
	"regexp"
	"strings"
)

// Rules are ordered transform rules applied to the stem before the character
// pipeline runs.
//
// # Selection is not transformation
//
// Two different kinds of pattern exist in this system and they must not be
// confused:
//
//   - discovery.include / discovery.exclude SELECT which files are eligible.
//     They never change a name.
//   - normalization.rules TRANSFORM a stem. They never decide eligibility.
//
// A file excluded by selection is never normalized; a rule that matches
// nothing leaves a selected file to the default pipeline.
//
// # Why rules run first
//
// Rules run on the raw stem, before NFC, lowercasing and character mapping.
// The pipeline therefore always has the last word, which is what keeps a rule
// from bypassing an invariant: whatever a rule emits is still lowercased, still
// stripped of unsupported punctuation, still collapsed, and still length
// capped. A rule cannot introduce a path separator, escape a root, change the
// extension, or alter document bytes, because it never sees a path, an
// extension, or the file.
//
// The remaining risk a rule genuinely carries is non-idempotence -- a rule that
// keeps appending text would produce a different name on every pass. That is
// checked rather than trusted: Policy.Normalize runs the whole pipeline twice
// and refuses to name a file whose two passes disagree, and configuration
// validation runs a fixed corpus through the compiled rules for the same
// reason.
type Rules []Rule

// Rule is one compiled transform rule.
type Rule struct {
	// Name identifies the rule in diagnostics. It is operator-supplied and
	// must be a safe identifier, because it appears in logs.
	Name string
	// Pattern is an RE2 regular expression matched against the stem.
	Pattern *regexp.Regexp
	// Replacement is the substitution text. It supports RE2 group expansion
	// ($1, ${name}) exactly as Go's Regexp.ReplaceAllString does.
	Replacement string
	// All replaces every match rather than only the first.
	All bool
}

// RuleSpec is the unvalidated form of a rule, as it appears in configuration.
type RuleSpec struct {
	Name            string `json:"name"`
	Pattern         string `json:"pattern"`
	Replacement     string `json:"replacement"`
	All             bool   `json:"all"`
	CaseInsensitive bool   `json:"case_insensitive"`
}

// maxPatternLen bounds a configured pattern. RE2 has no catastrophic
// backtracking, so the bound is about operator error rather than safety.
const maxPatternLen = 512

// CompileRules validates and compiles configured rules in order.
//
// Every problem is collected so an operator sees the whole list at once.
func CompileRules(specs []RuleSpec) (Rules, []string) {
	var problems []string
	var out Rules
	seen := map[string]bool{}

	for i, spec := range specs {
		where := fmt.Sprintf("normalization.rules[%d]", i)

		switch {
		case spec.Name == "":
			problems = append(problems, where+": name is required")
		case !safeRuleName(spec.Name):
			problems = append(problems, where+": name %q must be 1-40 chars of [a-z0-9_-]")
		case seen[spec.Name]:
			problems = append(problems, where+": duplicate rule name "+spec.Name)
		default:
			seen[spec.Name] = true
		}

		if spec.Pattern == "" {
			problems = append(problems, where+": pattern is required")
			continue
		}
		if len(spec.Pattern) > maxPatternLen {
			problems = append(problems, fmt.Sprintf("%s: pattern longer than %d characters", where, maxPatternLen))
			continue
		}

		pattern := spec.Pattern
		if spec.CaseInsensitive {
			pattern = "(?i)" + pattern
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			// The compiler's message can quote the operator's own pattern,
			// which is configuration rather than document text, so it is safe
			// to surface here.
			problems = append(problems, fmt.Sprintf("%s: %v", where, err))
			continue
		}
		// A pattern that matches the empty string would splice the replacement
		// between every rune. RE2 allows it; the policy does not.
		if re.MatchString("") {
			problems = append(problems, where+": pattern matches the empty string")
			continue
		}

		out = append(out, Rule{
			Name:        spec.Name,
			Pattern:     re,
			Replacement: spec.Replacement,
			All:         spec.All,
		})
	}
	return out, problems
}

func safeRuleName(s string) bool {
	if s == "" || len(s) > 40 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

// apply runs every rule in order and reports which ones changed the stem.
func (rs Rules) apply(stem string) (string, []string) {
	if len(rs) == 0 {
		return stem, nil
	}
	var hits []string
	for _, r := range rs {
		before := stem
		if r.All {
			stem = r.Pattern.ReplaceAllString(stem, r.Replacement)
		} else {
			stem = replaceFirst(r.Pattern, stem, r.Replacement)
		}
		if stem != before {
			hits = append(hits, r.Name)
		}
	}
	return stem, hits
}

// replaceFirst substitutes only the first match, with group expansion.
func replaceFirst(re *regexp.Regexp, s, repl string) string {
	loc := re.FindStringSubmatchIndex(s)
	if loc == nil {
		return s
	}
	var b []byte
	b = append(b, s[:loc[0]]...)
	b = re.ExpandString(b, repl, s, loc)
	b = append(b, s[loc[1]:]...)
	return string(b)
}

// Names lists the compiled rule names in order.
func (rs Rules) Names() []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Name)
	}
	return out
}

// idempotenceCorpus is the fixed set of synthetic stems configuration
// validation pushes through the compiled policy. It covers each character
// class the pipeline treats specially, so a rule that breaks convergence is
// rejected at startup instead of at the first matching document.
var idempotenceCorpus = []string{
	"Bank Statement - August (Final) 2026",
	"John's Invoice #123",
	"Medical  --  Statement",
	"  TAX___RETURN 2025!!",
	"Überweisung Straße",
	"請求書 2026",
	"report.final",
	"!!!",
	"a",
	strings.Repeat("long", 80),
	"trailing___",
	"---leading",
	"mixed_-_separators",
	"digits 2026 and 01",
}

// SelfCheck runs the corpus through the policy twice and reports any input
// whose name did not converge, plus any input the policy cannot name at all.
//
// It is a configuration-time gate, not a test: an operator who writes a rule
// that keeps rewriting its own output learns about it during validation.
func (p Policy) SelfCheck() []string {
	var problems []string
	const probeJobID = "00000000-0000-4000-8000-000000000000"
	for _, stem := range idempotenceCorpus {
		name := stem + ".pdf"
		first, err := p.normalizeOnce(name, probeJobID)
		if err != nil {
			problems = append(problems, fmt.Sprintf("normalization self-check: %q -> %v", stem, err))
			continue
		}
		second, err := p.normalizeOnce(first.Name, probeJobID)
		if err != nil {
			problems = append(problems, fmt.Sprintf("normalization self-check: %q reprocessed -> %v", stem, err))
			continue
		}
		if second.Name != first.Name {
			problems = append(problems, fmt.Sprintf(
				"normalization self-check: rules are not idempotent; a probe name changed again on the second pass (%d -> %d bytes)",
				len(first.Name), len(second.Name)))
		}
	}
	return problems
}
