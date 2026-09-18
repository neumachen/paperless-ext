package naming

import (
	"strings"
	"testing"

	"golang.org/x/text/unicode/norm"
)

// ruled builds the default policy with one extra rule, or fails the test.
func ruled(t *testing.T, name, pattern, replacement string) Policy {
	t.Helper()
	p := DefaultPolicy()
	rules, problems := CompileRules([]RuleSpec{
		{Name: name, Pattern: pattern, Replacement: replacement},
	})
	if len(problems) != 0 {
		t.Fatalf("compile: %v", problems)
	}
	p.Rules = rules
	return p
}

// The Unicode guard used to ask whether a letter still appeared SOMEWHERE in
// the result. An input containing that letter twice answers the question by
// itself: a rule can transliterate one occurrence while the other keeps the
// guard satisfied, and `für für.pdf` becomes `fur_für.pdf`.
//
// This is not one more example for a list. The guard counts occurrences now,
// so the property holds for inputs nobody thought of.
func TestARepeatedLetterCannotCoverATransliteration(t *testing.T) {
	p := ruled(t, "fuer_fuer", "^für für$", "fur für")

	_, err := p.Normalize("für für.pdf", testJobID)
	if err == nil {
		t.Fatal("a rule that transliterated one of two ü survived the guard")
	}
	if HoldCategory(err) != "policy_transliterates" {
		t.Fatalf("category = %q, want policy_transliterates", HoldCategory(err))
	}
}

// Three occurrences, one of them changed. The count has to drop for the guard
// to fire, and it does.
func TestOneOfSeveralOccurrencesIsEnoughToRefuse(t *testing.T) {
	p := ruled(t, "first_only", "^ü(.*)ü(.*)ü$", "u${1}ü${2}ü")

	if _, err := p.Normalize("üaübü.pdf", testJobID); err == nil {
		t.Fatal("changing one of three occurrences was accepted")
	}
}

// A rule that genuinely preserves its letters must still pass, or the guard is
// just a ban on rules.
func TestPreservingRulesAreStillAccepted(t *testing.T) {
	p := ruled(t, "dash_it", "^rechnung (.+)$", "rechnung-$1")

	res, err := p.Normalize("rechnung für märz.pdf", testJobID)
	if err != nil {
		t.Fatalf("a preserving rule was refused: %v", err)
	}
	for _, r := range []string{"ü", "ä"} {
		if !strings.Contains(res.Name, r) {
			t.Errorf("%q is missing from %q", r, res.Name)
		}
	}
}

// A decomposed input defeats a guard that counts LETTERS.
//
// "für" is f, u, COMBINING DIAERESIS, r. The combining mark is Mn, not a
// letter, so a rule anchored on that exact stem and replacing it with "fur"
// removed the diaeresis while the count of non-ASCII letters was zero on both
// sides. The document's name silently lost a character the contract preserves.
func TestADecomposedLetterCannotBeTransliteratedAway(t *testing.T) {
	p := ruled(t, "decomposed_fuer", "^für$", "fur")

	if _, err := p.Normalize("für.pdf", testJobID); err == nil {
		t.Fatal("a rule that removed a combining diaeresis survived the guard")
	} else if HoldCategory(err) != "policy_transliterates" {
		t.Fatalf("category = %q, want policy_transliterates", HoldCategory(err))
	}
}

// The composed and decomposed spellings of the same name are protected alike.
func TestTheComposedSpellingIsProtectedToo(t *testing.T) {
	p := ruled(t, "composed_fuer", "^für$", "fur")

	if _, err := p.Normalize("für.pdf", testJobID); err == nil {
		t.Fatal("a rule that transliterated a composed ü survived the guard")
	}
}

// A rule that only rearranges a decomposed name is still accepted: the guard
// must not become a ban on rules that touch non-ASCII text at all.
func TestADecomposedNameSurvivesAPreservingRule(t *testing.T) {
	p := ruled(t, "prefix_it", "^(für.*)$", "rechnung-$1")

	res, err := p.Normalize("für maerz.pdf", testJobID)
	if err != nil {
		t.Fatalf("a preserving rule was refused: %v", err)
	}
	if !strings.Contains(norm.NFC.String(res.Name), "ü") {
		t.Fatalf("the letter did not survive: %q", res.Name)
	}
}
