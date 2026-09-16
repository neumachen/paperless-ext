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

// PolicyVersion identifies the naming policy the binaries implement.
//
// The normalization policy itself is not implemented in this increment, so the
// value is deliberately "unimplemented" rather than "v1". It must only move to
// a real policy identifier once the transformation is implemented and the
// owner has accepted the visible naming behaviour.
const PolicyVersion = "unimplemented"

// GoVersion reports the toolchain the binary was compiled with.
func GoVersion() string { return runtime.Version() }
