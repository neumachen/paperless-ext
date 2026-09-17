package naming

import (
	"strings"
	"testing"
)

// Regression tests for the third review round's naming findings.
//
// Each one fails against the reviewed candidate e06ab85 and passes now; the
// comment says what the old behaviour was, so the test explains the defect
// rather than merely asserting the fix.

// FN-N008: the collision suffix is appended AFTER Normalize's own convergence
// check, so a rule that matches a suffixed name can make the published name
// normalize to something else.
//
// Before: "foo.pdf" normalized and converged; the allocator then produced the
// collision candidate "foo_01.pdf", which the same policy normalizes back to
// "foo.pdf". A published name that is not a fixed point of the policy that
// produced it means a retry recomputing the name disagrees with the
// reservation.
func TestFinalNameWithCollisionSuffixMustBeAFixedPoint(t *testing.T) {
	p := DefaultPolicy()
	rules, problems := CompileRules([]RuleSpec{
		{Name: "strip-01", Pattern: `^foo_01$`, Replacement: "foo"},
	})
	if len(problems) != 0 {
		t.Fatalf("the rule should compile: %v", problems)
	}
	p.Rules = rules

	// The base name still normalizes and converges, which is why the old
	// check passed.
	res, err := p.Normalize("foo.pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if res.Name != "foo.pdf" {
		t.Fatalf("base name = %q", res.Name)
	}

	// The suffixed candidate is where it breaks.
	candidate, err := p.Candidate(res, 1)
	if err != nil {
		t.Fatalf("Candidate: %v", err)
	}
	if candidate != "foo_01.pdf" {
		t.Fatalf("candidate = %q", candidate)
	}
	if err := p.VerifyFinalName(candidate, testJobID); err == nil {
		t.Errorf("VerifyFinalName accepted %q, which this policy renormalizes to something else", candidate)
	}

	// SelfCheck does NOT catch this one, and that limit is worth stating: it
	// probes a fixed corpus, and this rule is written to match a stem the
	// corpus does not contain. A configuration-time gate cannot anticipate
	// every name a producer will submit. The guarantee is therefore the
	// runtime check above, which runs against the actual candidate
	// immediately before the destination is reserved; SelfCheck is the early
	// warning for rule shapes that are visible from the corpus, tested next.
}

// TestSelfCheckCatchesSuffixRulesItCanSee is the configuration-time half: a
// rule general enough to affect the probe corpus is rejected before intake,
// so an operator learns about it at validation rather than at the first
// colliding document.
func TestSelfCheckCatchesSuffixRulesItCanSee(t *testing.T) {
	p := DefaultPolicy()
	rules, problems := CompileRules([]RuleSpec{
		// Strips any collision suffix, whatever the stem.
		{Name: "strip-suffix", Pattern: `_\d\d$`, Replacement: ""},
	})
	if len(problems) != 0 {
		t.Fatalf("compile: %v", problems)
	}
	p.Rules = rules

	problems = p.SelfCheck()
	if len(problems) == 0 {
		t.Fatalf("SelfCheck accepted a rule that strips every collision suffix")
	}
	found := false
	for _, s := range problems {
		if strings.Contains(s, "collision candidate") {
			found = true
		}
	}
	if !found {
		t.Errorf("the problem does not identify the collision candidate: %v", problems)
	}
}

// FN-N008: a configured rule must not transliterate a preserved character.
//
// Before: a case-insensitive ü-to-u rule compiled, converged, and passed every
// check, silently defeating the owner's explicitly confirmed requirement that
// ä, ö, ü and ß are preserved rather than mapped to ASCII.
func TestRulesMayNotTransliteratePreservedCharacters(t *testing.T) {
	p := DefaultPolicy()
	rules, problems := CompileRules([]RuleSpec{
		{Name: "umlaut", Pattern: "ü", Replacement: "u", All: true, CaseInsensitive: true},
	})
	if len(problems) != 0 {
		t.Fatalf("compile: %v", problems)
	}
	p.Rules = rules

	// It converges, which is exactly why convergence was not enough. Runtime
	// now refuses it per input, which is stronger than the corpus gate: a rule
	// written as ^für$ -> fur evades any finite probe set.
	_, err := p.Normalize("Überweisung.pdf", testJobID)
	if err == nil {
		t.Fatal("Normalize accepted a rule that maps a preserved character to ASCII")
	}
	if HoldCategory(err) != "policy_transliterates" {
		t.Errorf("category = %q, want policy_transliterates", HoldCategory(err))
	}

	problems = p.SelfCheck()
	if len(problems) == 0 {
		t.Fatalf("SelfCheck accepted a rule that maps a preserved character to ASCII")
	}
	found := false
	for _, s := range problems {
		if strings.Contains(s, "transliterate") {
			found = true
		}
	}
	if !found {
		t.Errorf("the problem does not explain the transliteration: %v", problems)
	}
}

// TestRulesThatRemoveTextWholesaleAreStillAllowed guards the other direction:
// the preservation check must not forbid a rule that legitimately deletes a
// segment, which would make the gate useless in practice.
func TestRulesThatRemoveTextWholesaleAreStillAllowed(t *testing.T) {
	p := DefaultPolicy()
	rules, problems := CompileRules([]RuleSpec{
		{Name: "drop-copy", Pattern: `\s*\(copy\)`, Replacement: "", All: true, CaseInsensitive: true},
	})
	if len(problems) != 0 {
		t.Fatalf("compile: %v", problems)
	}
	p.Rules = rules
	if problems := p.SelfCheck(); len(problems) != 0 {
		t.Errorf("a rule that only deletes a marker was rejected: %v", problems)
	}
}

// FN-N009: rules are applied before the length cap, and nothing bounded the
// intermediate.
//
// Before: twenty rules of the form ^(.+)$ -> ${1}${1} turned a three-character
// stem into eight megabytes, and thirty into eight gigabytes. Because gRPC
// ValidateConfig runs this inside the application process, a tiny candidate
// document could exhaust the running watcher while merely being validated.
func TestRuleExpansionIsBounded(t *testing.T) {
	var specs []RuleSpec
	for i := 0; i < 40; i++ {
		specs = append(specs, RuleSpec{
			Name:        "double-" + string(rune('a'+i%26)) + string(rune('a'+i/26)),
			Pattern:     `^(.+)$`,
			Replacement: "${1}${1}",
		})
	}
	rules, problems := CompileRules(specs)
	if len(problems) != 0 {
		t.Fatalf("the rules are individually valid: %v", problems)
	}
	p := DefaultPolicy()
	p.Rules = rules

	_, err := p.Normalize("abc.pdf", testJobID)
	if err == nil {
		t.Fatalf("40 doubling rules were applied without complaint")
	}
	if HoldCategory(err) != "policy_expansion_too_large" {
		t.Errorf("category = %q, want policy_expansion_too_large", HoldCategory(err))
	}

	// The bound must hold for the configuration gate too, which is the path an
	// untrusted candidate document actually takes.
	if problems := p.SelfCheck(); len(problems) == 0 {
		t.Errorf("SelfCheck accepted an unboundedly expanding rule set")
	}
}

// TestBoundedExpansionStillAllowsUsefulGrowth: the bound must not forbid a
// rule set that expands a name before a later rule trims it.
func TestBoundedExpansionStillAllowsUsefulGrowth(t *testing.T) {
	rules, problems := CompileRules([]RuleSpec{
		{Name: "expand", Pattern: `^(.+)$`, Replacement: "prefix-${1}-suffix"},
		{Name: "trim", Pattern: `^prefix-(.+)-suffix$`, Replacement: "${1}"},
	})
	if len(problems) != 0 {
		t.Fatalf("compile: %v", problems)
	}
	p := DefaultPolicy()
	p.Rules = rules
	got, err := p.Normalize("doc.pdf", testJobID)
	if err != nil {
		t.Fatalf("a bounded expand-then-trim rule set was rejected: %v", err)
	}
	if got.Name != "doc.pdf" {
		t.Errorf("got %q, want %q", got.Name, "doc.pdf")
	}
}

// FN-N008 (second round): the corpus gate could be evaded by writing a rule
// that only fires on a stem the corpus does not contain.
//
// Before: pattern ^für$ with replacement "fur" passed CompileRules, passed
// SelfCheck -- none of the six fixed probes is "für" -- converged, and turned
// für.pdf into fur.pdf, transliterating a character the owner explicitly
// required to be preserved.
func TestCorpusEvadingTransliterationIsRefusedAtRuntime(t *testing.T) {
	p := DefaultPolicy()
	rules, problems := CompileRules([]RuleSpec{
		{Name: "fuer", Pattern: "^für$", Replacement: "fur"},
	})
	if len(problems) != 0 {
		t.Fatalf("compile: %v", problems)
	}
	p.Rules = rules

	// The corpus gate does NOT catch this, and saying so is the point: a
	// configuration-time probe set cannot anticipate every input.
	if probes := p.SelfCheck(); len(probes) != 0 {
		t.Logf("SelfCheck happened to flag it: %v", probes)
	}

	_, err := p.Normalize("für.pdf", testJobID)
	if err == nil {
		t.Fatal("für.pdf was transliterated to fur.pdf")
	}
	if HoldCategory(err) != "policy_transliterates" {
		t.Errorf("category = %q, want policy_transliterates", HoldCategory(err))
	}

	// A name the rule does not touch must be unaffected.
	got, err := p.Normalize("Bericht.pdf", testJobID)
	if err != nil {
		t.Fatalf("an untouched name was refused: %v", err)
	}
	if got.Name != "bericht.pdf" {
		t.Errorf("got %q", got.Name)
	}
}

// TestDeletingRulesRemainAllowed: the per-input check must not forbid a rule
// that legitimately removes text containing a preserved character.
func TestDeletingRulesRemainAllowed(t *testing.T) {
	p := DefaultPolicy()
	rules, problems := CompileRules([]RuleSpec{
		{Name: "drop-suffix", Pattern: ` – Überweisung$`, Replacement: "", All: true},
	})
	if len(problems) != 0 {
		t.Fatalf("compile: %v", problems)
	}
	p.Rules = rules
	got, err := p.Normalize("Rechnung 2026 – Überweisung.pdf", testJobID)
	if err != nil {
		t.Fatalf("a rule that deletes a segment was refused as transliteration: %v", err)
	}
	if got.Name != "rechnung_2026.pdf" {
		t.Errorf("got %q, want %q", got.Name, "rechnung_2026.pdf")
	}
}
