package config

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

// Effective is the non-secret view of what a process is actually running.
//
// It is the single source for the `check-config` output and the gRPC
// effective-configuration RPC, so the two can never drift apart. It holds no
// credentials, and it cannot: the fields are enumerated here by hand, and
// FileConfig has nowhere to carry a secret in the first place.
//
// Selection patterns and transform rules ARE included. They are
// operator-authored configuration, not document-derived data, and an operator
// who cannot see the effective rule list cannot tell why a name came out the
// way it did.
type Effective struct {
	Application string `json:"application"`
	Instance    string `json:"instance"`
	ConfigFile  string `json:"config_file,omitempty"`

	Policy EffectivePolicy `json:"policy"`

	Storage map[string]string `json:"storage"`
	// StorageDevices reports the filesystem device backing each root. It is
	// how "are staging and consume genuinely on different filesystems?" is
	// answered from the kernel rather than inferred from two paths -- and the
	// runtime images have no shell, so the application is the only thing that
	// can answer it from inside the container.
	StorageDevices map[string]uint64 `json:"storage_devices,omitempty"`
	// StorageResolved reports the directory each root actually resolves to,
	// so a symlinked root is visible as what it is.
	StorageResolved map[string]string `json:"storage_resolved,omitempty"`

	Discovery *EffectiveDiscovery `json:"discovery,omitempty"`

	// Archive is reported by the watcher, the only process that moves an
	// original, whether it is enabled or not: "is anything going to move files
	// out of the drop folder?" has to be answerable either way.
	Archive *EffectiveArchive `json:"archive,omitempty"`

	Processing *EffectiveProcessing `json:"processing,omitempty"`

	Dependencies EffectiveDependencies `json:"dependencies"`
}

// EffectivePolicy is the naming policy in force.
type EffectivePolicy struct {
	// Identity is what gets stamped on a job: version plus a fingerprint of
	// every name-affecting setting.
	Identity string `json:"identity"`
	// Version is the policy code this build implements.
	Version string `json:"version"`
	// Status says plainly what the identity does not: this policy is a
	// documented candidate for local synthetic testing, not an
	// owner-accepted production policy.
	Status             string   `json:"status"`
	MaxNameBytes       int      `json:"max_name_bytes"`
	MaxExtensionLength int      `json:"max_extension_length"`
	MaxCollisionSuffix int      `json:"max_collision_suffix"`
	RequireExtension   bool     `json:"require_extension"`
	Rules              []string `json:"rules"`
}

// EffectiveDiscovery is the discovery configuration in force.
type EffectiveDiscovery struct {
	Enabled           bool     `json:"enabled"`
	IntervalSeconds   float64  `json:"interval_seconds"`
	StabilitySeconds  float64  `json:"stability_interval_seconds"`
	Completion        string   `json:"completion"`
	Recursive         bool     `json:"recursive"`
	Batch             int      `json:"batch"`
	MaxFileBytes      int64    `json:"max_file_bytes"`
	Include           []string `json:"include"`
	Exclude           []string `json:"exclude"`
	TemporarySuffixes []string `json:"temporary_suffixes"`
}

// EffectiveArchive is the source-archival configuration in force.
type EffectiveArchive struct {
	Enabled bool `json:"enabled"`
	// Action is "move" or "remove".
	Action string `json:"action"`
	// Directory is relative to the incoming root, and used by a move.
	Directory       string  `json:"directory"`
	IntervalSeconds float64 `json:"interval_seconds"`
	Batch           int     `json:"batch"`
}

// EffectiveProcessing is the renamer's operational configuration.
type EffectiveProcessing struct {
	Concurrency         int  `json:"concurrency"`
	Prefetch            int  `json:"prefetch"`
	MaxDeliveryAttempts int  `json:"max_delivery_attempts"`
	DryRun              bool `json:"dry_run"`
	// PublishTakeoverMS is how old a publication claim must be before another
	// attempt may take it over. Reported because an operator diagnosing a
	// stranded or a double-attempted publication needs to know it, and it is
	// no longer implied by the shutdown timeout.
	PublishTakeoverMS int64 `json:"publish_takeover_ms"`
}

// EffectiveDependencies names the endpoints without their credentials.
type EffectiveDependencies struct {
	DatabaseHost   string `json:"database_host"`
	DatabasePort   int    `json:"database_port"`
	DatabaseName   string `json:"database_name"`
	ReplicaHost    string `json:"replica_host,omitempty"`
	BrokerHost     string `json:"broker_host"`
	BrokerPort     int    `json:"broker_port"`
	BrokerVHost    string `json:"broker_vhost"`
	BrokerQueue    string `json:"broker_queue"`
	BrokerExchange string `json:"broker_exchange"`
	MaxAttempts    int    `json:"broker_delivery_limit"`
}

// PolicyStatusCandidate is the honest status string for this build.
const PolicyStatusCandidate = "candidate-for-local-synthetic-testing; not an owner-accepted production policy"

func (c Common) effectiveBase() Effective {
	return Effective{
		Application: string(c.Application),
		Instance:    c.Instance,
		ConfigFile:  c.ConfigFile,
		Policy: EffectivePolicy{
			Identity:           c.Policy.Identity,
			Version:            policyCodeVersion(c.Policy.Identity),
			Status:             PolicyStatusCandidate,
			MaxNameBytes:       c.Policy.MaxNameBytes,
			MaxExtensionLength: c.Policy.MaxExtensionLen,
			MaxCollisionSuffix: c.Policy.MaxCollisionSuffix,
			RequireExtension:   c.Policy.RequireExtension,
			Rules:              c.Policy.Rules.Names(),
		},
		Storage: map[string]string{
			"incoming": c.Storage.Incoming,
			"queued":   c.Storage.Queued,
			"staging":  c.Storage.Staging,
			"consume":  c.Storage.Consume,
			"failed":   c.Storage.Failed,
		},
		StorageDevices:  c.Roots.Devices(),
		StorageResolved: c.Roots.Resolved(),
		Dependencies: EffectiveDependencies{
			DatabaseHost:   c.Database.PrimaryHost,
			DatabasePort:   c.Database.PrimaryPort,
			DatabaseName:   c.Database.Name,
			ReplicaHost:    c.Database.ReplicaHost,
			BrokerHost:     c.Broker.Host,
			BrokerPort:     c.Broker.Port,
			BrokerVHost:    c.Broker.VHost,
			BrokerQueue:    c.Broker.Queue,
			BrokerExchange: c.Broker.Exchange,
			MaxAttempts:    c.Broker.DeliveryLimit,
		},
	}
}

// policyCodeVersion splits the build-level version out of an identity.
func policyCodeVersion(identity string) string {
	for i := len(identity) - 1; i >= 0; i-- {
		if identity[i] == '+' {
			return identity[:i]
		}
	}
	return identity
}

// Effective renders the watcher's non-secret configuration.
func (c WatcherConfig) Effective() Effective {
	e := c.Common.effectiveBase()
	include, exclude := c.Discovery.Matcher.Patterns()
	e.Discovery = &EffectiveDiscovery{
		Enabled:           c.Discovery.Enabled,
		IntervalSeconds:   c.Discovery.Interval.Seconds(),
		StabilitySeconds:  c.Discovery.StabilityInterval.Seconds(),
		Completion:        string(c.Discovery.Completion),
		Recursive:         c.Discovery.Recursive,
		Batch:             c.Discovery.Batch,
		MaxFileBytes:      c.Discovery.MaxFileBytes,
		Include:           nonNil(include),
		Exclude:           nonNil(exclude),
		TemporarySuffixes: nonNil(append([]string(nil), c.Discovery.Matcher.TemporarySuffixes...)),
	}
	sort.Strings(e.Discovery.TemporarySuffixes)
	e.Archive = &EffectiveArchive{
		Enabled:         c.Archive.Enabled,
		Action:          c.Archive.Action,
		Directory:       c.Archive.Directory,
		IntervalSeconds: c.Archive.Interval.Seconds(),
		Batch:           c.Archive.Batch,
	}
	return e
}

// Effective renders the renamer's non-secret configuration.
func (c RenamerConfig) Effective() Effective {
	e := c.Common.effectiveBase()
	e.Processing = &EffectiveProcessing{
		Concurrency:         c.Concurrency,
		Prefetch:            c.Prefetch,
		MaxDeliveryAttempts: c.MaxDeliveryAttempts,
		PublishTakeoverMS:   c.PublishTakeoverAfter.Milliseconds(),
		DryRun:              c.DryRun,
	}
	return e
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// WriteJSON renders an effective configuration for an operator.
func (e Effective) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(e); err != nil {
		return fmt.Errorf("render effective configuration: %w", err)
	}
	return nil
}
