package naming

import (
	"errors"
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

// maxIntermediateBytes bounds the stem WHILE rules are running.
//
// The final name is capped, but the cap is applied after every rule has run,
// and nothing bounded the intermediate. A handful of rules of the form
// ^(.+)$ -> ${1}${1} doubles the stem each time, so twenty such rules turn a
// three-character name into eight megabytes and thirty turn it into eight
// gigabytes. Because gRPC ValidateConfig and PreviewName run this work inside
// the application process, a tiny, perfectly valid-looking candidate
// configuration could exhaust the running watcher while merely being checked.
//
// The bound is generous relative to any legitimate rule -- a rule set needs
// room to expand a name before later rules trim it -- and tiny relative to the
// growth an unbounded expansion reaches within a few rules.
const maxIntermediateBytes = 1 << 16

// ErrExpansionTooLarge reports that rules grew the stem past the working bound.
var ErrExpansionTooLarge = errors.New("configured rules expanded the name beyond the working limit")

// projectedSize is an upper bound on what a rule would produce, computed
// WITHOUT producing it.
//
// The previous bound checked len(stem) after ReplaceAllString had already
// built the result, which does not bound anything that matters: a rule
// replacing ^.+$ with 30,000 characters, followed by one replacing every
// character with 30,000 more, asks the regexp engine for a 900 MB string
// before the 64 KB check is ever reached. The configuration doing it is about
// 60 KB. Refusing afterwards is refusing after the damage.
//
// The estimate is deliberately conservative. Each match contributes the
// literal replacement plus, for every group reference it contains, the largest
// a group could be -- the whole input. Over-estimating rejects a few exotic
// rule sets that would in fact have fitted; under-estimating would let the
// allocation happen, which is the thing being prevented.
func projectedSize(re *regexp.Regexp, stem, replacement string, all bool) int {
	matches := re.FindAllStringIndex(stem, -1)
	if matches == nil {
		return len(stem)
	}
	n := len(matches)
	if !all {
		n = 1
	}

	groupRefs := strings.Count(replacement, "$")
	perMatch := len(replacement) + groupRefs*len(stem)

	consumed := 0
	for i, m := range matches {
		if !all && i > 0 {
			break
		}
		consumed += m[1] - m[0]
	}
	return len(stem) - consumed + n*perMatch
}

// apply runs every rule in order and reports which ones changed the stem.
//
// Every rule is projected before it runs, and the projection is checked
// against the same bound as the result, so an expanding rule set is refused
// before it allocates rather than after.
func (rs Rules) apply(stem string) (string, []string, error) {
	if len(rs) == 0 {
		return stem, nil, nil
	}
	if len(stem) > maxIntermediateBytes {
		return "", nil, fmt.Errorf("%w: input is %d bytes, limit %d",
			ErrExpansionTooLarge, len(stem), maxIntermediateBytes)
	}

	var hits []string
	for _, r := range rs {
		if projected := projectedSize(r.Pattern, stem, r.Replacement, r.All); projected > maxIntermediateBytes {
			return "", hits, fmt.Errorf("%w: rule %q would produce up to %d bytes, limit %d",
				ErrExpansionTooLarge, r.Name, projected, maxIntermediateBytes)
		}

		before := stem
		if r.All {
			stem = r.Pattern.ReplaceAllString(stem, r.Replacement)
		} else {
			stem = replaceFirst(r.Pattern, stem, r.Replacement)
		}
		// The projection is an upper bound, so this should never fire. It is
		// kept because a bound that is only ever asserted is a bound nobody
		// notices has drifted.
		if len(stem) > maxIntermediateBytes {
			return "", hits, fmt.Errorf("%w: rule %q produced %d bytes, limit %d",
				ErrExpansionTooLarge, r.Name, len(stem), maxIntermediateBytes)
		}
		if stem != before {
			hits = append(hits, r.Name)
		}
	}
	return stem, hits, nil
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

// unicodePreservationProbes are names whose non-ASCII letters the owner's
// confirmed direction requires to survive.
//
// The contract is "preserve Unicode rather than transliterate to ASCII", and
// it is stated for these characters specifically. A configured rule can
// violate it trivially -- a case-insensitive ü-to-u replacement converges
// perfectly well and would have passed every other check -- so it is checked
// directly rather than left to the character pipeline, which only decides
// which classes survive, not what a rule replaced them with.
var unicodePreservationProbes = []struct {
	stem   string
	expect []rune
}{
	{"Überweisung Straße", []rune{'ü', 'ß'}},
	{"Ärztliche Prüfung", []rune{'ä', 'ü'}},
	{"Öffnung", []rune{'ö'}},
	{"請求書", []rune{'請', '求', '書'}},
	{"Ελληνικά", []rune{'λ'}},
	{"Документ", []rune{'д'}},
}

// checkUnicodePreservation reports rules that transliterate preserved letters.
//
// It asks a narrow, checkable question: after the rules run, is each probe
// character still present? A rule that deletes a whole segment legitimately
// removes it too, so the probe is only counted as violated when the character
// is gone AND the stem still has content -- which is what a transliterating
// substitution looks like, as opposed to a rule that drops text wholesale.
func (p Policy) checkUnicodePreservation() []string {
	if len(p.Rules) == 0 {
		return nil
	}
	var problems []string
	for _, probe := range unicodePreservationProbes {
		after, _, err := p.Rules.apply(probe.stem)
		if err != nil {
			continue // reported separately by SelfCheck
		}
		lowered := strings.ToLower(after)
		if strings.TrimSpace(lowered) == "" {
			continue // the rules removed everything; not a transliteration
		}
		for _, r := range probe.expect {
			if !strings.ContainsRune(lowered, r) && !strings.ContainsRune(strings.ToUpper(after), r) {
				problems = append(problems, fmt.Sprintf(
					"normalization.rules transliterate a preserved character: %q is no longer present "+
						"after the rules run, and the accepted contract preserves Unicode rather than "+
						"mapping it to ASCII", string(r)))
				break
			}
		}
	}
	return problems
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
			continue
		}

		// The collision suffixes and the shortening marker are appended AFTER
		// the convergence check above, so they need their own. A rule that
		// matches a suffixed name can make the published name normalize to
		// something different from itself, and the published name is the one
		// that has to satisfy the policy.
		for _, n := range []int{1, 2, 99} {
			candidate, cerr := p.Candidate(first, n)
			if cerr != nil {
				problems = append(problems, fmt.Sprintf(
					"normalization self-check: collision candidate %d -> %v", n, cerr))
				break
			}
			if verr := p.VerifyFinalName(candidate, probeJobID); verr != nil {
				problems = append(problems, fmt.Sprintf(
					"normalization self-check: collision candidate %d of a probe name is not a fixed "+
						"point of the policy, so a published name would not survive its own rules", n))
				break
			}
		}
	}
	problems = append(problems, p.checkUnicodePreservation()...)
	return problems
}
