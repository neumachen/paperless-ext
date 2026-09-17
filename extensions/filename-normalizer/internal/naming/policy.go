// Package naming implements the filename normalization policy.
//
// # Authority
//
// The rules marked SETTLED below come from the PRD and the role agreement and
// are requirements. Everything marked CANDIDATE was left unresolved by the
// owner and is chosen here for local synthetic testing, under the authority
// granted in the fifth handoff. A candidate choice is a real, documented,
// deterministic behaviour of this build -- it is not an accepted production
// policy, and PolicyVersion below says so in its own name.
//
// # Pipeline
//
// The extension is split off first and normalized independently of the stem,
// so no stem rule can change the file type.
//
//  0. configured transform rules run on the stem (see rules.go)
//  1. NFC normalization                           CANDIDATE (form)
//  2. Unicode-aware lowercasing                   SETTLED
//  3. character mapping, rune by rune:            SETTLED (classes)
//     letters, decimal digits, and combining
//     marks after a kept letter  -> kept      CANDIDATE (exact set)
//     Unicode whitespace           -> "_"       SETTLED
//     "_"                          -> "_"       SETTLED
//     dash punctuation, U+2212     -> "-"       CANDIDATE (mapping)
//     anything else                -> removed   SETTLED
//  4. collapse runs of "_" and runs of "-"        SETTLED
//  5. drop "_" adjacent to "-"                    SETTLED
//  6. collapse "-" runs again, then strip leading
//     and trailing "_" and "-"                    SETTLED
//  7. empty stem -> document_<job id>             CANDIDATE (shape)
//  8. length cap and shortening                   CANDIDATE (cap and format)
//
// Steps 4-6 are ordered so that a dash always wins over the underscores around
// it: "medical  --  statement" reaches "medical__--__statement" at step 3,
// "medical_-_statement" at step 4 and "medical-statement" at step 5.
//
// # Idempotence
//
// Applying the policy to its own output must be a no-op, and this is checked
// rather than asserted: Normalize runs the pipeline a second time over its own
// result and reports ErrNotIdempotent if the two disagree. The shortening and
// collision suffixes are therefore built only from characters step 3 keeps --
// a "~" marker, for instance, would be stripped on the second pass and would
// have made the policy non-idempotent.
package naming

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// PolicyVersion identifies the behaviour of this build.
//
// The name carries its own status: this is a candidate for local synthetic
// testing, not a production-accepted naming policy. A build reports this
// string in telemetry and stamps it on every job, so a job accepted under one
// policy can always be told apart from a job accepted under another.
const PolicyVersion = "v1-candidate-2026-09-16"

// Defaults for the CANDIDATE numeric choices. All three are configurable.
const (
	// DefaultMaxNameBytes caps the final filename, including its extension and
	// any collision suffix, at 200 UTF-8 bytes.
	DefaultMaxNameBytes = 200
	// DefaultMaxExtensionLen bounds a valid extension at 16 characters.
	DefaultMaxExtensionLen = 16
	// DefaultMaxCollisionSuffix bounds the collision sequence.
	DefaultMaxCollisionSuffix = 9999
	// shortenDigestLen is the number of hex characters appended when a stem is
	// truncated. Hex keeps the marker inside the character set step 3 keeps.
	shortenDigestLen = 8
)

// Errors returned by Normalize. Each maps to a closed-set hold category, so a
// document that cannot be named safely becomes a visible hold rather than a
// guess.
var (
	// ErrMissingExtension reports a name with no extension at all.
	ErrMissingExtension = errors.New("missing extension")
	// ErrInvalidExtension reports an extension outside the accepted shape. The
	// file type is never guessed.
	ErrInvalidExtension = errors.New("invalid extension")
	// ErrNotIdempotent reports that the pipeline did not converge in one pass.
	// It can only be produced by configured transform rules.
	ErrNotIdempotent = errors.New("policy is not idempotent for this input")
	// ErrNameTooLong reports that no truncation could fit the cap, which means
	// the extension alone exceeds it.
	ErrNameTooLong = errors.New("name cannot be shortened to fit")
)

// Policy is the compiled, immutable naming policy.
type Policy struct {
	// MaxNameBytes caps the final filename in UTF-8 bytes.
	MaxNameBytes int
	// MaxExtensionLen bounds a valid extension.
	MaxExtensionLen int
	// MaxCollisionSuffix bounds the collision sequence.
	MaxCollisionSuffix int
	// RequireExtension holds files with no extension instead of publishing
	// them without one. CANDIDATE: true by default, because Paperless routes
	// on file type and guessing one would change the document's meaning.
	RequireExtension bool
	// Rules are the configured transform rules, applied to the stem before the
	// character pipeline. Empty by default.
	Rules Rules
}

// DefaultPolicy returns the candidate policy with no configured rules.
func DefaultPolicy() Policy {
	return Policy{
		MaxNameBytes:       DefaultMaxNameBytes,
		MaxExtensionLen:    DefaultMaxExtensionLen,
		MaxCollisionSuffix: DefaultMaxCollisionSuffix,
		RequireExtension:   true,
	}
}

// Result is the outcome of normalizing one submission name.
type Result struct {
	// Name is the normalized filename: stem plus "." plus extension.
	Name string
	// Stem and Extension are its parts, after normalization.
	Stem, Extension string
	// UsedFallback reports that the stem normalized away entirely and the
	// job-id fallback was used.
	UsedFallback bool
	// Shortened reports that the stem was truncated to fit MaxNameBytes.
	Shortened bool
	// RuleHits names the configured rules that changed the stem, in order.
	// It is diagnostic only and never contains document text.
	RuleHits []string
}

// Normalize applies the policy to one original filename.
//
// jobID supplies the empty-stem fallback. It must be a stable per-job value so
// that a retry of the same job produces the same name; the caller passes the
// ledger job id, which never changes across attempts.
func (p Policy) Normalize(original, jobID string) (Result, error) {
	res, err := p.normalizeOnce(original, jobID)
	if err != nil {
		return Result{}, err
	}

	// Convergence check. The pipeline is idempotent by construction, but a
	// configured rule can break that, and a non-idempotent policy would make a
	// reserved name disagree with a later recomputation of it.
	second, err := p.normalizeOnce(res.Name, jobID)
	if err != nil {
		return Result{}, fmt.Errorf("%w: second pass failed: %v", ErrNotIdempotent, err)
	}
	if second.Name != res.Name {
		return Result{}, fmt.Errorf("%w: converged to two different names", ErrNotIdempotent)
	}
	return res, nil
}

func (p Policy) normalizeOnce(original, jobID string) (Result, error) {
	stem, ext, err := p.splitExtension(original)
	if err != nil {
		return Result{}, err
	}

	var res Result
	var rerr error
	stem, res.RuleHits, rerr = p.Rules.apply(stem)
	if rerr != nil {
		return Result{}, rerr
	}
	stem = normalizeStem(stem)

	if stem == "" {
		stem = fallbackStem(jobID)
		res.UsedFallback = true
	}

	stem, shortened, err := p.fit(stem, ext, "")
	if err != nil {
		return Result{}, err
	}
	res.Shortened = shortened
	res.Stem = stem
	res.Extension = ext
	res.Name = join(stem, ext)
	return res, nil
}

// splitExtension separates the stem from the extension at the final ASCII dot.
//
// CANDIDATE: compound extensions get no special treatment. "archive.tar.gz"
// splits into stem "archive.tar" and extension "gz"; the earlier dot is then
// unsupported punctuation and is removed, giving "archivetar.gz". Treating
// ".tar.gz" as one extension would require a list of known compounds, which is
// a product decision rather than an implementation detail.
func (p Policy) splitExtension(original string) (stem, ext string, err error) {
	// A leading dot is not an extension separator: ".bashrc" is a hidden file
	// with no extension, not an extensionless name with extension "bashrc".
	// Discovery skips hidden files, so this only guards direct callers.
	idx := strings.LastIndexByte(original, '.')
	if idx <= 0 {
		if p.RequireExtension {
			return "", "", ErrMissingExtension
		}
		return original, "", nil
	}
	stem, ext = original[:idx], original[idx+1:]

	if ext == "" {
		if p.RequireExtension {
			return "", "", fmt.Errorf("%w: empty", ErrInvalidExtension)
		}
		return original, "", nil
	}
	// CANDIDATE: an accepted extension is ASCII letters and digits only, 1 to
	// MaxExtensionLen characters. Anything else is held rather than repaired,
	// because repairing it would be guessing a file type.
	if len(ext) > p.MaxExtensionLen {
		return "", "", fmt.Errorf("%w: longer than %d characters", ErrInvalidExtension, p.MaxExtensionLen)
	}
	for _, r := range ext {
		isDigit := r >= '0' && r <= '9'
		isAlpha := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		if !isDigit && !isAlpha {
			return "", "", fmt.Errorf("%w: non-alphanumeric", ErrInvalidExtension)
		}
	}
	return stem, strings.ToLower(ext), nil
}

// normalizeStem runs steps 1 to 6 of the pipeline.
func normalizeStem(s string) string {
	s = norm.NFC.String(s)
	s = strings.ToLower(s)

	var b strings.Builder
	b.Grow(len(s))
	lastKeptLetter := false
	for _, r := range s {
		switch {
		case unicode.IsLetter(r):
			b.WriteRune(r)
			lastKeptLetter = true
		case unicode.IsDigit(r) && unicode.In(r, unicode.Nd):
			// Decimal digits only. Other numeric runes (fractions, Roman
			// numerals, circled digits) are symbols and are removed.
			b.WriteRune(r)
			lastKeptLetter = false
		case unicode.In(r, unicode.Mn, unicode.Mc, unicode.Me):
			// A combining mark is kept only when it actually combines with a
			// letter this pass kept. An orphan mark is removed, so a name
			// cannot begin with a floating diacritic.
			if lastKeptLetter {
				b.WriteRune(r)
			}
		case unicode.IsSpace(r):
			b.WriteByte('_')
			lastKeptLetter = false
		case r == '_':
			b.WriteByte('_')
			lastKeptLetter = false
		case r == '-' || r == '−' || unicode.In(r, unicode.Pd):
			b.WriteByte('-')
			lastKeptLetter = false
		default:
			lastKeptLetter = false
		}
	}

	out := collapse(b.String())
	out = dropUnderscoresAroundDashes(out)
	out = collapse(out)
	return strings.Trim(out, "_-")
}

// collapse reduces runs of "_" and runs of "-" to one character each.
func collapse(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	var prev rune
	for _, r := range s {
		if (r == '_' || r == '-') && r == prev {
			continue
		}
		b.WriteRune(r)
		prev = r
	}
	return b.String()
}

// dropUnderscoresAroundDashes removes every "_" that touches a "-", so a
// divider dash keeps its dividing role instead of accumulating separators.
func dropUnderscoresAroundDashes(s string) string {
	runes := []rune(s)
	keep := make([]bool, len(runes))
	for i, r := range runes {
		keep[i] = true
		if r != '_' {
			continue
		}
		if i > 0 && runes[i-1] == '-' {
			keep[i] = false
		}
		if i+1 < len(runes) && runes[i+1] == '-' {
			keep[i] = false
		}
	}
	var b strings.Builder
	for i, r := range runes {
		if keep[i] {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// fallbackStem builds the empty-stem fallback.
//
// CANDIDATE: "document_" plus the job id with its dashes removed. The dashes
// are dropped so the fallback survives its own normalization unchanged -- a
// UUID's dashes would otherwise be read as divider dashes on a second pass.
func fallbackStem(jobID string) string {
	return "document_" + strings.ReplaceAll(strings.ToLower(jobID), "-", "")
}

// fit truncates the stem so that stem + suffix + "." + ext fits MaxNameBytes.
//
// CANDIDATE: truncation is at a rune boundary, and the truncated stem gets a
// deterministic "_<8 hex>" marker derived from the full pre-truncation stem.
// Two different long names that share a prefix therefore keep different
// shortened names, and the marker uses only characters the pipeline keeps, so
// the shortened name normalizes to itself.
func (p Policy) fit(stem, ext, suffix string) (string, bool, error) {
	budget := p.MaxNameBytes - len(suffix)
	if ext != "" {
		budget -= len(ext) + 1 // the separating dot
	}
	if budget <= 0 {
		return "", false, fmt.Errorf("%w: extension and suffix alone exceed %d bytes", ErrNameTooLong, p.MaxNameBytes)
	}
	if len(stem) <= budget {
		return stem, false, nil
	}

	sum := sha256.Sum256([]byte(stem))
	marker := "_" + hex.EncodeToString(sum[:])[:shortenDigestLen]
	head := budget - len(marker)
	if head <= 0 {
		return "", false, fmt.Errorf("%w: no room for the shortening marker in %d bytes", ErrNameTooLong, p.MaxNameBytes)
	}

	truncated := truncateRunes(stem, head)
	// Truncation can expose a trailing separator; strip it so the shortened
	// stem still satisfies step 6.
	truncated = strings.TrimRight(truncated, "_-")
	return truncated + marker, true, nil
}

// truncateRunes returns the longest prefix of s that fits n bytes without
// splitting a rune.
func truncateRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := 0
	for i := range s {
		if i > n {
			break
		}
		cut = i
	}
	return s[:cut]
}

func join(stem, ext string) string {
	if ext == "" {
		return stem
	}
	return stem + "." + ext
}

// Candidate returns the nth destination name for a normalized result.
//
// n == 0 is the normalized name itself; n >= 1 appends the collision suffix
// "_01", "_02", ... . CANDIDATE: the sequence is zero-padded to two digits and
// grows beyond that naturally ("_100"), and the cap is re-applied per
// candidate so a suffix can never push a name past MaxNameBytes.
func (p Policy) Candidate(res Result, n int) (string, error) {
	if n < 0 || n > p.MaxCollisionSuffix {
		return "", fmt.Errorf("collision sequence %d outside 0..%d", n, p.MaxCollisionSuffix)
	}
	if n == 0 {
		return res.Name, nil
	}
	suffix := fmt.Sprintf("_%02d", n)
	stem, _, err := p.fit(res.Stem, res.Extension, suffix)
	if err != nil {
		return "", err
	}
	return join(stem+suffix, res.Extension), nil
}

// ReservationKey returns the comparison key for a destination name.
//
// Destination uniqueness must hold on filesystems that compare names
// case-insensitively and on those that do not, so the key is the NFC form
// case-folded. The pipeline already lowercases and NFC-normalizes, so for
// policy output this is usually the identity; it matters for pre-existing
// files discovered in the destination, which this build did not name.
func ReservationKey(name string) string {
	return strings.ToLower(norm.NFC.String(name))
}

// HoldCategory maps a normalization error to its closed-set category.
func HoldCategory(err error) string {
	switch {
	case errors.Is(err, ErrMissingExtension):
		return "missing_extension"
	case errors.Is(err, ErrInvalidExtension):
		return "invalid_extension"
	case errors.Is(err, ErrNotIdempotent):
		return "policy_not_idempotent"
	case errors.Is(err, ErrNameTooLong):
		return "name_too_long"
	case errors.Is(err, ErrExpansionTooLarge):
		return "policy_expansion_too_large"
	default:
		return "normalization_failed"
	}
}

// VerifyFinalName checks that a name the allocator is about to publish is
// itself a fixed point of the policy.
//
// Normalize's own convergence check runs on the normalized stem, BEFORE the
// collision suffix and any shortening are appended. Those are applied
// afterwards, so a configured rule can make the final name normalize to
// something else even though the policy passed its own idempotence gate. A
// rule matching "^foo_01$" and replacing it with "foo", for instance, lets
// foo.pdf normalize cleanly and then produces the collision candidate
// foo_01.pdf, which normalizes back to foo.pdf -- a published name that is not
// what the policy would produce for itself.
//
// The published name is what matters, so it is the published name that is
// checked, immediately before the destination is reserved.
func (p Policy) VerifyFinalName(name, jobID string) error {
	again, err := p.normalizeOnce(name, jobID)
	if err != nil {
		return fmt.Errorf("%w: the final name is not acceptable to the policy: %v", ErrNotIdempotent, err)
	}
	if again.Name != name {
		return fmt.Errorf("%w: the final name is not a fixed point", ErrNotIdempotent)
	}
	return nil
}
