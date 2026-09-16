package naming

import (
	"errors"
	"strings"
	"testing"
)

const testJobID = "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"

// TestRequiredPRDExamples covers the four examples the PRD requires. These are
// settled requirements, not candidate behaviour: a change that breaks one of
// them breaks the contract.
func TestRequiredPRDExamples(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Bank Statement - August (Final) 2026.PDF", "bank_statement-august_final_2026.pdf"},
		{"John's Invoice #123.pdf", "johns_invoice_123.pdf"},
		{"Medical  --  Statement.pdf", "medical-statement.pdf"},
		{"  TAX___RETURN 2025!!.PDF", "tax_return_2025.pdf"},
	}
	p := DefaultPolicy()
	for _, c := range cases {
		got, err := p.Normalize(c.in, testJobID)
		if err != nil {
			t.Errorf("Normalize(%q) failed: %v", c.in, err)
			continue
		}
		if got.Name != c.want {
			t.Errorf("Normalize(%q) = %q, want %q", c.in, got.Name, c.want)
		}
	}
}

// TestUnicodeIsPreservedNotTransliterated covers the owner's confirmed
// direction: ä, ö, ü and ß survive.
func TestUnicodeIsPreservedNotTransliterated(t *testing.T) {
	p := DefaultPolicy()
	cases := []struct{ in, want string }{
		{"Überweisung Straße.PDF", "überweisung_straße.pdf"},
		{"Ärztliche Bescheinigung.pdf", "ärztliche_bescheinigung.pdf"},
		{"請求書 2026.PDF", "請求書_2026.pdf"},
		{"Ελληνικά Έγγραφο.pdf", "ελληνικά_έγγραφο.pdf"},
		{"Документ 7.pdf", "документ_7.pdf"},
	}
	for _, c := range cases {
		got, err := p.Normalize(c.in, testJobID)
		if err != nil {
			t.Errorf("Normalize(%q) failed: %v", c.in, err)
			continue
		}
		if got.Name != c.want {
			t.Errorf("Normalize(%q) = %q, want %q", c.in, got.Name, c.want)
		}
		if strings.ContainsAny(got.Name, "?") {
			t.Errorf("Normalize(%q) produced a replacement character: %q", c.in, got.Name)
		}
	}
}

// TestCanonicallyEquivalentSpellingsConverge is a consequence of choosing NFC,
// not an independent requirement. It is asserted here because the candidate
// policy selects NFC, so the behaviour is now real and must stay stable.
func TestCanonicallyEquivalentSpellingsConverge(t *testing.T) {
	p := DefaultPolicy()
	composed := "Über.pdf"    // U+00DC
	decomposed := "Über.pdf" // U+0055 U+0308
	a, err := p.Normalize(composed, testJobID)
	if err != nil {
		t.Fatalf("composed: %v", err)
	}
	b, err := p.Normalize(decomposed, testJobID)
	if err != nil {
		t.Fatalf("decomposed: %v", err)
	}
	if a.Name != b.Name {
		t.Errorf("NFC did not converge: %q vs %q", a.Name, b.Name)
	}
}

// TestIdempotence applies the policy to its own output across a wide corpus.
func TestIdempotence(t *testing.T) {
	p := DefaultPolicy()
	inputs := []string{
		"Bank Statement - August (Final) 2026.PDF",
		"John's Invoice #123.pdf",
		"Medical  --  Statement.pdf",
		"  TAX___RETURN 2025!!.PDF",
		"Überweisung Straße.PDF",
		"請求書 2026.PDF",
		"report.final.PDF",
		"a.pdf",
		"_-_-_.pdf",
		"trailing___.pdf",
		"---leading.pdf",
		strings.Repeat("verylongsegment", 40) + ".pdf",
		"mixed _ - _ separators.pdf",
		"tab\tand\nnewline.pdf",
	}
	for _, in := range inputs {
		first, err := p.Normalize(in, testJobID)
		if err != nil {
			t.Errorf("Normalize(%q) failed: %v", in, err)
			continue
		}
		second, err := p.Normalize(first.Name, testJobID)
		if err != nil {
			t.Errorf("re-normalizing %q failed: %v", first.Name, err)
			continue
		}
		if second.Name != first.Name {
			t.Errorf("not idempotent: %q -> %q -> %q", in, first.Name, second.Name)
		}
	}
}

// TestEmptyStemFallback covers the candidate fallback shape and, importantly,
// that the fallback itself is idempotent -- a UUID's dashes would otherwise be
// read as divider dashes on a second pass.
func TestEmptyStemFallback(t *testing.T) {
	p := DefaultPolicy()
	got, err := p.Normalize("!!!.PDF", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if !got.UsedFallback {
		t.Errorf("expected the fallback to be used for %q", "!!!.PDF")
	}
	want := "document_3f2b1c4d5e6f4a7b8c9d0e1f2a3b4c5d.pdf"
	if got.Name != want {
		t.Errorf("fallback = %q, want %q", got.Name, want)
	}
	again, err := p.Normalize(got.Name, testJobID)
	if err != nil {
		t.Fatalf("re-normalize: %v", err)
	}
	if again.Name != got.Name {
		t.Errorf("fallback is not idempotent: %q -> %q", got.Name, again.Name)
	}
}

// TestFallbackIsStablePerJobAndDistinctAcrossJobs: a retry must reuse the same
// name, and two different jobs must not collide on the fallback.
func TestFallbackIsStablePerJobAndDistinctAcrossJobs(t *testing.T) {
	p := DefaultPolicy()
	other := "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d"

	first, _ := p.Normalize("###.pdf", testJobID)
	retry, _ := p.Normalize("###.pdf", testJobID)
	if first.Name != retry.Name {
		t.Errorf("fallback changed between attempts: %q vs %q", first.Name, retry.Name)
	}
	distinct, _ := p.Normalize("###.pdf", other)
	if distinct.Name == first.Name {
		t.Errorf("two jobs shared a fallback name: %q", distinct.Name)
	}
}

func TestExtensionHandling(t *testing.T) {
	p := DefaultPolicy()

	// The extension is lowercased and never inferred.
	got, err := p.Normalize("Scan.JPEG", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Extension != "jpeg" {
		t.Errorf("extension = %q, want %q", got.Extension, "jpeg")
	}

	// CANDIDATE: no compound-extension handling.
	got, err = p.Normalize("archive.tar.gz", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Name != "archivetar.gz" {
		t.Errorf("compound extension = %q, want %q", got.Name, "archivetar.gz")
	}

	// Held, not repaired.
	for _, in := range []string{"noextension", "trailingdot.", "weird.p df", "toolong." + strings.Repeat("x", 17)} {
		if _, err := p.Normalize(in, testJobID); err == nil {
			t.Errorf("Normalize(%q) should have been held", in)
		}
	}
	if _, err := p.Normalize("noextension", testJobID); !errors.Is(err, ErrMissingExtension) {
		t.Errorf("want ErrMissingExtension, got %v", err)
	}
	if _, err := p.Normalize("weird.p df", testJobID); !errors.Is(err, ErrInvalidExtension) {
		t.Errorf("want ErrInvalidExtension, got %v", err)
	}
}

func TestLengthCapAndShortening(t *testing.T) {
	p := DefaultPolicy()
	long := strings.Repeat("segment", 60) + ".pdf" // well over the cap
	got, err := p.Normalize(long, testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if !got.Shortened {
		t.Errorf("expected shortening for a %d byte name", len(long))
	}
	if len(got.Name) > p.MaxNameBytes {
		t.Errorf("shortened name is %d bytes, over the %d cap", len(got.Name), p.MaxNameBytes)
	}
	if !strings.HasSuffix(got.Name, ".pdf") {
		t.Errorf("shortening dropped the extension: %q", got.Name)
	}
	again, err := p.Normalize(got.Name, testJobID)
	if err != nil {
		t.Fatalf("re-normalize: %v", err)
	}
	if again.Name != got.Name {
		t.Errorf("shortened name is not idempotent: %q -> %q", got.Name, again.Name)
	}

	// Two long names sharing a long prefix must not shorten to the same name.
	a, _ := p.Normalize(strings.Repeat("x", 300)+"alpha.pdf", testJobID)
	b, _ := p.Normalize(strings.Repeat("x", 300)+"beta.pdf", testJobID)
	if a.Name == b.Name {
		t.Errorf("two distinct long names shortened to the same name: %q", a.Name)
	}
}

// TestShorteningIsMultibyteSafe: truncation must not split a rune.
func TestShorteningIsMultibyteSafe(t *testing.T) {
	p := DefaultPolicy()
	got, err := p.Normalize(strings.Repeat("請求書", 100)+".pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if len(got.Name) > p.MaxNameBytes {
		t.Errorf("name is %d bytes, over the cap", len(got.Name))
	}
	if !utf8Valid(got.Name) {
		t.Errorf("truncation split a rune: %q", got.Name)
	}
}

func utf8Valid(s string) bool {
	for _, r := range s {
		if r == '�' {
			return false
		}
	}
	return true
}

func TestCollisionCandidates(t *testing.T) {
	p := DefaultPolicy()
	res, err := p.Normalize("Statement.pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	want := []string{"statement.pdf", "statement_01.pdf", "statement_02.pdf"}
	for i, w := range want {
		got, err := p.Candidate(res, i)
		if err != nil {
			t.Fatalf("Candidate(%d): %v", i, err)
		}
		if got != w {
			t.Errorf("Candidate(%d) = %q, want %q", i, got, w)
		}
	}
	// Every candidate must itself be a fixed point of the policy, or a retry
	// that recomputed the name would disagree with the reservation.
	for i := 0; i <= 120; i++ {
		c, err := p.Candidate(res, i)
		if err != nil {
			t.Fatalf("Candidate(%d): %v", i, err)
		}
		again, err := p.Normalize(c, testJobID)
		if err != nil {
			t.Fatalf("re-normalize %q: %v", c, err)
		}
		if again.Name != c {
			t.Errorf("candidate %q is not a fixed point: %q", c, again.Name)
		}
	}
	if _, err := p.Candidate(res, p.MaxCollisionSuffix+1); err == nil {
		t.Errorf("expected the collision sequence to be bounded")
	}
}

// TestCollisionSuffixRespectsTheCap: a suffix must never push a name over the
// byte cap; the stem gives way instead.
func TestCollisionSuffixRespectsTheCap(t *testing.T) {
	p := DefaultPolicy()
	res, err := p.Normalize(strings.Repeat("y", 400)+".pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	for _, n := range []int{0, 1, 99, 100, 9999} {
		c, err := p.Candidate(res, n)
		if err != nil {
			t.Fatalf("Candidate(%d): %v", n, err)
		}
		if len(c) > p.MaxNameBytes {
			t.Errorf("candidate %d is %d bytes, over the %d cap", n, len(c), p.MaxNameBytes)
		}
	}
}

func TestOrphanCombiningMarksAreRemoved(t *testing.T) {
	p := DefaultPolicy()
	// A combining acute with no base letter must not survive as a leading mark.
	got, err := p.Normalize("́abc.pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Name != "abc.pdf" {
		t.Errorf("orphan mark survived: %q", got.Name)
	}
	// A mark that does combine with a letter is kept, via NFC composition.
	got, err = p.Normalize("café.pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Name != "café.pdf" {
		t.Errorf("combining mark was not preserved: %q", got.Name)
	}
}

func TestNonDecimalNumeralsAreRemoved(t *testing.T) {
	p := DefaultPolicy()
	// Circled digit and a fraction are symbols, not decimal digits.
	got, err := p.Normalize("report ① ½.pdf", testJobID)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Name != "report.pdf" {
		t.Errorf("non-decimal numerals survived: %q", got.Name)
	}
}

func TestDashPunctuationMapsToASCII(t *testing.T) {
	p := DefaultPolicy()
	for _, in := range []string{
		"a – b.pdf", // en dash
		"a — b.pdf", // em dash
		"a − b.pdf", // minus sign
		"a - b.pdf", // ASCII
	} {
		got, err := p.Normalize(in, testJobID)
		if err != nil {
			t.Fatalf("Normalize(%q): %v", in, err)
		}
		if got.Name != "a-b.pdf" {
			t.Errorf("Normalize(%q) = %q, want %q", in, got.Name, "a-b.pdf")
		}
	}
}

func TestPathSeparatorsCannotSurvive(t *testing.T) {
	p := DefaultPolicy()
	// Even if a caller passed a name containing separators or traversal, the
	// character pipeline removes them: "/" and "." are unsupported punctuation.
	for _, in := range []string{"../escape.pdf", "a/b.pdf", "..\\b.pdf"} {
		got, err := p.Normalize(in, testJobID)
		if err != nil {
			continue // held is also an acceptable outcome
		}
		if strings.ContainsAny(got.Name, `/\`) {
			t.Errorf("Normalize(%q) kept a path separator: %q", in, got.Name)
		}
		if strings.Contains(got.Name, "..") {
			t.Errorf("Normalize(%q) kept a traversal sequence: %q", in, got.Name)
		}
	}
}

func TestReservationKeyFoldsCaseAndForm(t *testing.T) {
	if ReservationKey("Statement.PDF") != ReservationKey("statement.pdf") {
		t.Errorf("reservation key is case-sensitive")
	}
	if ReservationKey("Über.pdf") != ReservationKey("Über.pdf") {
		t.Errorf("reservation key is not NFC-folded")
	}
}

func TestSelfCheckPassesForTheDefaultPolicy(t *testing.T) {
	if problems := DefaultPolicy().SelfCheck(); len(problems) != 0 {
		t.Errorf("default policy failed its own self-check: %v", problems)
	}
}
