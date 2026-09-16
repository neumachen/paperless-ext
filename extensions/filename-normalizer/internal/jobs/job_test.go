package jobs

import (
	"strings"
	"testing"
	"time"
)

func TestParseStateAcceptsOnlyTheClosedSet(t *testing.T) {
	for _, s := range States() {
		got, err := ParseState(string(s))
		if err != nil || got != s {
			t.Errorf("ParseState(%q) = %q,%v", s, got, err)
		}
	}
	for _, bad := range []string{"", "done", "PENDING_DISPATCH", "held ", "../../etc"} {
		if _, err := ParseState(bad); err == nil {
			t.Errorf("ParseState(%q) was accepted", bad)
		}
	}
}

func TestStatesAreSafeMetricLabels(t *testing.T) {
	// Job states are used directly as Prometheus label values.
	for _, s := range States() {
		if !SafeIdentifier(string(s)) {
			t.Errorf("state %q is not a safe label value", s)
		}
	}
	for _, c := range Categories() {
		if !SafeIdentifier(string(c)) {
			t.Errorf("category %q is not a safe label value", c)
		}
	}
}

func TestSafeIdentifierRejectsDocumentDerivedText(t *testing.T) {
	for _, ok := range []string{"held", "normalization_unimplemented", "sha-256", "v1.0", "a"} {
		if !SafeIdentifier(ok) {
			t.Errorf("SafeIdentifier(%q) = false", ok)
		}
	}
	for _, bad := range []string{
		"", "Bank Statement.pdf", "/srv/fn/incoming", "überweisung",
		"請求書", "a b", "name;drop", "-leading", strings.Repeat("x", 65),
	} {
		if SafeIdentifier(bad) {
			t.Errorf("SafeIdentifier(%q) = true; document-derived text must be rejected", bad)
		}
	}
}

func TestDecodeMessageAcceptsTheCurrentContract(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	body, err := Message{
		ContractVersion: ContractVersion,
		JobID:           "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		Attempt:         2,
		EnqueuedAt:      now,
	}.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := DecodeMessage(body)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if got.JobID != "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee" || got.Attempt != 2 {
		t.Errorf("round trip lost data: %+v", got)
	}
	if !got.EnqueuedAt.Equal(now) {
		t.Errorf("timestamp round trip: %v != %v", got.EnqueuedAt, now)
	}

	// The payload must carry an opaque reference and nothing else.
	for _, leak := range []string{"source", "name", "path", "fingerprint", "content", "size"} {
		if strings.Contains(strings.ToLower(string(body)), leak) {
			t.Errorf("the broker payload contains %q: %s", leak, body)
		}
	}
}

func TestDecodeMessageRejectsUnsafePayloads(t *testing.T) {
	cases := map[string]string{
		"malformed JSON":      `{"contract_version":1,`,
		"future version":      `{"contract_version":2,"job_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","attempt":1}`,
		"missing version":     `{"job_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","attempt":1}`,
		"non-UUID job":        `{"contract_version":1,"job_id":"../../etc/passwd","attempt":1}`,
		"uppercase UUID":      `{"contract_version":1,"job_id":"AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE","attempt":1}`,
		"zero attempt":        `{"contract_version":1,"job_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","attempt":0}`,
		"negative attempt":    `{"contract_version":1,"job_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","attempt":-3}`,
		"empty body":          ``,
		"job id with newline": `{"contract_version":1,"job_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeee\n","attempt":1}`,
	}
	for name, body := range cases {
		if _, err := DecodeMessage([]byte(body)); err == nil {
			t.Errorf("%s: payload was accepted", name)
		}
	}
}

func TestIsJobIDIsStrict(t *testing.T) {
	// Job IDs are the one job-specific value allowed into logs, so a malformed
	// reference must never pass this check and reach an output stream.
	for _, ok := range []string{
		"00000000-0000-0000-0000-000000000000",
		"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
	} {
		if !IsJobID(ok) {
			t.Errorf("IsJobID(%q) = false", ok)
		}
	}
	for _, bad := range []string{
		"", "not-a-uuid",
		"AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE",
		"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeeee",
		"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeee",
		"aaaaaaaa_bbbb_4ccc_8ddd_eeeeeeeeeeee",
		"gaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		"../../../etc/passwd; DROP TABLE jobs",
	} {
		if IsJobID(bad) {
			t.Errorf("IsJobID(%q) = true", bad)
		}
	}
}
