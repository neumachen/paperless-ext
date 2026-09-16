// Package buildinfo exposes identifying information that is stamped into the
// binaries at build time. Nothing here is derived at runtime from the
// environment, so the reported values always describe the artifact itself.
package buildinfo

import "runtime"

// Values overridden with -ldflags at image build time.
var (
	// Version is the artifact version of the filename-normalizer extension.
	Version = "0.0.0-dev"
	// Revision is the git revision the artifact was built from.
	Revision = "unknown"
	// BuildDate is the RFC3339 timestamp of the build.
	BuildDate = "unknown"
	// SourceDigest is a content hash of the source tree the artifact was
	// compiled from, computed inside the build from the copied build context.
	//
	// It exists so that "which candidate is running?" has a real answer. A
	// matching Go version and a shared git revision label do not exclude a
	// stale image: an uncommitted edit leaves the revision unchanged, and a
	// build that silently did not run leaves an older image in place. Every
	// artifact produced by one build carries the same digest, so the
	// integration suite can require that the applications it tests were
	// compiled from the same source as the suite binary itself.
	SourceDigest = "unknown"
)

// PolicyVersion is intentionally absent from this package.
//
// It used to live here as the constant "unimplemented", because no naming
// policy existed. A policy exists now, so there are two different questions
// and they deserve two different answers:
//
//   - Which policy CODE does this build contain? naming.PolicyVersion.
//   - Which policy is this PROCESS running, including its configured rules
//     and bounds? config.Policy.Identity, which is naming.PolicyVersion plus
//     a fingerprint of every name-affecting setting.
//
// Only the second one belongs on a job row, because two processes built from
// the same source can still be configured to produce different names. Keeping
// a build-level constant here would have made it too easy to stamp the wrong
// one.
//
// The implemented policy is a documented CANDIDATE for local synthetic
// testing. Implementation is not owner acceptance; see
// docs/filename-normalizer-requirements.md.

// GoVersion reports the toolchain the binary was compiled with.
func GoVersion() string { return runtime.Version() }
