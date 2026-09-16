package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ContractVersion is the version of the broker payload this build speaks.
//
// The payload carries an opaque job reference only. Document bytes, original
// names, paths and fingerprints are never placed on the broker.
const ContractVersion = 1

// Message is the versioned broker payload.
type Message struct {
	ContractVersion int       `json:"contract_version"`
	JobID           string    `json:"job_id"`
	Attempt         int       `json:"attempt"`
	EnqueuedAt      time.Time `json:"enqueued_at"`
}

// ErrUnsupportedContract reports a payload this build must not interpret.
var ErrUnsupportedContract = errors.New("unsupported message contract")

// Encode renders the payload for publication.
func (m Message) Encode() ([]byte, error) {
	if m.ContractVersion == 0 {
		m.ContractVersion = ContractVersion
	}
	return json.Marshal(m)
}

// DecodeMessage parses a delivery body and rejects anything this build cannot
// safely interpret. An unknown future version is an explicit error rather than
// a best-effort read, so a newer producer can never be silently misprocessed.
func DecodeMessage(body []byte) (Message, error) {
	var m Message
	if err := json.Unmarshal(body, &m); err != nil {
		return Message{}, fmt.Errorf("%w: malformed payload", ErrUnsupportedContract)
	}
	if m.ContractVersion != ContractVersion {
		return Message{}, fmt.Errorf("%w: version %d", ErrUnsupportedContract, m.ContractVersion)
	}
	if !IsJobID(m.JobID) {
		return Message{}, fmt.Errorf("%w: malformed job reference", ErrUnsupportedContract)
	}
	if m.Attempt < 1 {
		return Message{}, fmt.Errorf("%w: non-positive attempt", ErrUnsupportedContract)
	}
	return m, nil
}

// IsJobID reports whether s is a canonical lowercase UUID. Job IDs are the one
// job-specific value allowed in logs, so the format is checked strictly: a
// malformed reference must never be echoed into a log line or a metric.
func IsJobID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, r := range s {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			isDigit := r >= '0' && r <= '9'
			isHex := r >= 'a' && r <= 'f'
			if !isDigit && !isHex {
				return false
			}
		}
	}
	return true
}
