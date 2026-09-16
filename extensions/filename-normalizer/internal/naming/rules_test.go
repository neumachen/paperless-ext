package naming

import (
	"strings"
	"testing"
)

func compile(t *testing.T, specs ...RuleSpec) Rules {
	t.Helper()
	rules, problems := CompileRules(specs)
	if len(problems) != 0 {
		t.Fatalf("unexpected compile problems: %v", problems)
	}
	return rules
}

func TestRuleReplacementAndOrdering(t *testing.T) {
	p := DefaultPolicy()
	p.Rules = compile(t,
		RuleSpec{Name: "expand-inv", Pattern: `\bInv\b`, Replacement: "Invoice", All: true},
		RuleSpec{Name: "drop-copy", Pattern: `\s*\(copy\)`, Replacement: "", All: true, CaseInsensitive: true},
	)

	got, err := p.Normalize("Inv 2026 (Copy).pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Name != "invoice_2026.pdf" {
		t.Errorf("got %q, want %q", got.Name, "invoice_2026.pdf")
	}
	if len(got.RuleHits) != 2 {
		t.Errorf("expected both rules to fire, got %v", got.RuleHits)
	}
}

// TestRulesRunInOrder: a later rule sees the earlier rule's output.
func TestRulesRunInOrder(t *testing.T) {
	p := DefaultPolicy()
	p.Rules = compile(t,
		RuleSpec{Name: "a-to-b", Pattern: `a`, Replacement: "b", All: true},
		RuleSpec{Name: "b-to-c", Pattern: `b`, Replacement: "c", All: true},
	)
	got, err := p.Normalize("aaa.pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Name != "ccc.pdf" {
		t.Errorf("rules did not chain: got %q, want %q", got.Name, "ccc.pdf")
	}
}

func TestRuleGroupExpansion(t *testing.T) {
	p := DefaultPolicy()
	p.Rules = compile(t, RuleSpec{
		Name: "swap", Pattern: `^(\d{4})-(\w+)$`, Replacement: "$2 $1",
	})
	got, err := p.Normalize("2026-invoice.pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Name != "invoice_2026.pdf" {
		t.Errorf("got %q, want %q", got.Name, "invoice_2026.pdf")
	}
}

func TestReplaceFirstVersusAll(t *testing.T) {
	p := DefaultPolicy()
	// Anchored, because an unanchored first-only replacement is not
	// idempotent -- "xxx" would become "yxx" and then "yyx" -- and the policy
	// refuses such a rule. That refusal is covered by
	// TestNonIdempotentRuleIsRefused.
	p.Rules = compile(t, RuleSpec{Name: "once", Pattern: `^x`, Replacement: "y"})
	got, err := p.Normalize("xxx.pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Name != "yxx.pdf" {
		t.Errorf("first-only replacement got %q, want %q", got.Name, "yxx.pdf")
	}

	p.Rules = compile(t, RuleSpec{Name: "every", Pattern: `x`, Replacement: "y", All: true})
	got, _ = p.Normalize("xxx.pdf", testJobID)
	if got.Name != "yyy.pdf" {
		t.Errorf("replace-all got %q, want %q", got.Name, "yyy.pdf")
	}
}

// TestRulesCannotBypassThePipeline is the safety property: whatever a rule
// emits still goes through the character pipeline.
func TestRulesCannotBypassThePipeline(t *testing.T) {
	p := DefaultPolicy()
	p.Rules = compile(t, RuleSpec{
		// A rule that tries to inject traversal, separators and uppercase.
		Name: "hostile", Pattern: `doc`, Replacement: `../../etc/PASSWD`, All: true,
	})
	got, err := p.Normalize("doc.pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if strings.ContainsAny(got.Name, `/\`) {
		t.Fatalf("a rule injected a path separator: %q", got.Name)
	}
	if strings.Contains(got.Name, "..") {
		t.Fatalf("a rule injected traversal: %q", got.Name)
	}
	if got.Name != strings.ToLower(got.Name) {
		t.Fatalf("a rule bypassed lowercasing: %q", got.Name)
	}
	if got.Extension != "pdf" {
		t.Fatalf("a rule changed the extension: %q", got.Extension)
	}
	if got.Name != "etcpasswd.pdf" {
		t.Errorf("got %q, want %q", got.Name, "etcpasswd.pdf")
	}
}

// TestRulesCannotBreakTheLengthCap: the cap is applied after rules run.
func TestRulesCannotBreakTheLengthCap(t *testing.T) {
	p := DefaultPolicy()
	p.Rules = compile(t, RuleSpec{
		Name: "inflate", Pattern: `a`, Replacement: strings.Repeat("z", 500), All: true,
	})
	got, err := p.Normalize("a.pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if len(got.Name) > p.MaxNameBytes {
		t.Errorf("a rule pushed the name to %d bytes, over the %d cap", len(got.Name), p.MaxNameBytes)
	}
}

// TestNonIdempotentRuleIsRefused is the enforced guarantee: a rule that keeps
// rewriting its own output makes naming fail loudly rather than produce a name
// a retry would not reproduce.
func TestNonIdempotentRuleIsRefused(t *testing.T) {
	p := DefaultPolicy()
	p.Rules = compile(t, RuleSpec{
		Name: "grows", Pattern: `^(.+)$`, Replacement: "x$1",
	})
	if _, err := p.Normalize("doc.pdf", testJobID); err == nil {
		t.Fatalf("a non-idempotent rule was accepted")
	} else if HoldCategory(err) != "policy_not_idempotent" {
		t.Errorf("category = %q, want policy_not_idempotent", HoldCategory(err))
	}

	// And configuration validation catches it before any document is seen.
	if problems := p.SelfCheck(); len(problems) == 0 {
		t.Errorf("SelfCheck accepted a non-idempotent rule set")
	}
}

func TestCompileRulesRejectsBadSpecs(t *testing.T) {
	cases := []struct {
		name string
		spec RuleSpec
	}{
		{"no name", RuleSpec{Pattern: "a"}},
		{"bad name", RuleSpec{Name: "Has Spaces", Pattern: "a"}},
		{"no pattern", RuleSpec{Name: "x"}},
		{"bad pattern", RuleSpec{Name: "x", Pattern: "([unclosed"}},
		{"empty match", RuleSpec{Name: "x", Pattern: "a*"}},
		{"huge pattern", RuleSpec{Name: "x", Pattern: strings.Repeat("a", maxPatternLen+1)}},
	}
	for _, c := range cases {
		if _, problems := CompileRules([]RuleSpec{c.spec}); len(problems) == 0 {
			t.Errorf("%s: expected a compile problem", c.name)
		}
	}

	if _, problems := CompileRules([]RuleSpec{
		{Name: "dup", Pattern: "a"},
		{Name: "dup", Pattern: "b"},
	}); len(problems) == 0 {
		t.Errorf("duplicate rule names were accepted")
	}
}

func TestCompileRulesReportsEveryProblemAtOnce(t *testing.T) {
	_, problems := CompileRules([]RuleSpec{
		{Name: "", Pattern: ""},
		{Name: "ok", Pattern: "([unclosed"},
	})
	if len(problems) < 2 {
		t.Errorf("expected several problems, got %v", problems)
	}
}
