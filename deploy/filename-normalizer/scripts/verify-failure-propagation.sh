#!/bin/sh
# FN-F003 — retain reviewable evidence that the public build and verification
# entry points propagate a containerized failure.
#
# This is run deliberately and last, so its artefacts survive in the reported
# evidence set. The first correction pass ran equivalent probes *before* the
# final clean run, and `make clean-evidence` removes the whole directory, so
# the status document ended up naming files that were not there.
#
# Six things are established, across both public targets:
#
#   1. run-logged.sh propagates a non-zero status from the containerized
#      command (not tee's status, which is what the defect was).
#   2. run-logged.sh propagates success.
#   3. `make verify` exits non-zero when the containerized verification really
#      fails, using a real formatting violation in a throwaway file.
#   4. `make verify` exits zero again once that file is removed.
#   5. `make build` exits non-zero when the containerized BUILD really fails,
#      using a real compile error. This needs its own fixture: `build` does not
#      depend on `verify`, and the build stage does not run gofmt, so a
#      formatting violation is not evidence that a build failure propagates.
#   6. `make build` exits zero again once that file is removed, so the images
#      being reported are rebuilt from the candidate rather than left stale.
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
EXT_DIR="$(cd "$DEPLOY_DIR/../../extensions/filename-normalizer" && pwd)"
EVIDENCE_DIR="$EXT_DIR/.evidence"
PROBE_FILE="$EXT_DIR/internal/buildinfo/zz_propagation_probe.go"
BUILD_PROBE_FILE="$EXT_DIR/internal/buildinfo/zz_build_probe.go"

mkdir -p "$EVIDENCE_DIR"
cd "$DEPLOY_DIR"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
FAILURES=0
note_result() {
    printf '    %s: exit=%s (expected %s)\n' "$1" "$2" "$3"
    if [ "$2" != "$3" ]; then
        printf '    MISMATCH\n' >&2
        FAILURES=$((FAILURES + 1))
    fi
}

# Always remove the probe file, including on interrupt: leaving it behind would
# break every later build.
cleanup() { rm -f "$PROBE_FILE" "$BUILD_PROBE_FILE"; }
trap cleanup EXIT INT TERM

log "1/6: run-logged.sh must propagate a non-zero containerized status"
./scripts/run-logged.sh "$EVIDENCE_DIR/f003-propagation-probe.log" \
    docker run --rm alpine:latest sh -c 'echo "simulated tool output"; exit 17' \
    >/dev/null 2>&1
PROBE_STATUS=$?
note_result "run-logged.sh with a command exiting 17" "$PROBE_STATUS" 17

log "2/6: run-logged.sh must propagate success"
./scripts/run-logged.sh "$EVIDENCE_DIR/f003-propagation-ok.log" \
    docker run --rm alpine:latest sh -c 'echo ok' \
    >/dev/null 2>&1
OK_STATUS=$?
note_result "run-logged.sh with a succeeding command" "$OK_STATUS" 0

log "3/6: make verify must fail on a real formatting violation"
cat > "$PROBE_FILE" <<'PROBE'
package buildinfo
    // deliberately misformatted, to prove that `make verify` fails
func   propagationProbe( ) string {return "probe" }
PROBE
make verify > "$EVIDENCE_DIR/f003-make-verify-fails.log" 2>&1
FAIL_STATUS=$?
if [ "$FAIL_STATUS" -eq 0 ]; then
    printf '    make verify with a formatting violation: exit=0 (expected non-zero)\n' >&2
    printf '    MISMATCH\n' >&2
    FAILURES=$((FAILURES + 1))
else
    printf '    make verify with a formatting violation: exit=%s (expected non-zero)\n' "$FAIL_STATUS"
fi
if grep -q 'zz_propagation_probe' "$EVIDENCE_DIR/f003-make-verify-fails.log"; then
    printf '    the log names the offending file, so the failure is the intended one\n'
else
    printf '    the log does not name the offending file; the failure may be unrelated\n' >&2
    FAILURES=$((FAILURES + 1))
fi

log "4/6: make verify must pass again once the violation is removed"
rm -f "$PROBE_FILE"
make verify > "$EVIDENCE_DIR/f003-make-verify-restored.log" 2>&1
RESTORED_STATUS=$?
note_result "make verify on the restored candidate" "$RESTORED_STATUS" 0

log "5/6: make build must fail on a real compile error"
cat > "$BUILD_PROBE_FILE" <<'PROBE'
package buildinfo

// Deliberately uncompilable, to prove that `make build` propagates a real
// containerized build failure. A formatting violation would not: the build
// stage does not run gofmt, and `build` does not depend on `verify`.
func propagationBuildProbe() string { return 42 }
PROBE
make build > "$EVIDENCE_DIR/f003-make-build-fails.log" 2>&1
BUILD_FAIL_STATUS=$?
if [ "$BUILD_FAIL_STATUS" -eq 0 ]; then
    printf '    make build with a compile error: exit=0 (expected non-zero)\n' >&2
    printf '    MISMATCH\n' >&2
    FAILURES=$((FAILURES + 1))
else
    printf '    make build with a compile error: exit=%s (expected non-zero)\n' "$BUILD_FAIL_STATUS"
fi
if grep -q 'zz_build_probe' "$EVIDENCE_DIR/f003-make-build-fails.log"; then
    printf '    the log names the offending file, so the failure is the intended one\n'
    BUILD_FAIL_IDENTIFIED=yes
else
    printf '    the log does not name the offending file; the failure may be unrelated\n' >&2
    FAILURES=$((FAILURES + 1))
    BUILD_FAIL_IDENTIFIED=no
fi
BUILD_FAIL_REASON="$(grep -m1 -E 'cannot use|zz_build_probe.go:[0-9]+' \
    "$EVIDENCE_DIR/f003-make-build-fails.log" 2>/dev/null | sed 's/^[[:space:]]*//' || true)"
[ -n "$BUILD_FAIL_REASON" ] || BUILD_FAIL_REASON="(compiler message not captured)"
printf '    compiler said: %s\n' "$BUILD_FAIL_REASON"

log "6/6: make build must succeed again once the compile error is removed"
rm -f "$BUILD_PROBE_FILE"
make build > "$EVIDENCE_DIR/f003-make-build-restored.log" 2>&1
BUILD_RESTORED_STATUS=$?
note_result "make build on the restored candidate" "$BUILD_RESTORED_STATUS" 0

{
    echo "FN-F003 — failure propagation through the public entry points"
    echo
    echo "run-logged.sh, command exiting 17          exit=$PROBE_STATUS   expected 17"
    echo "run-logged.sh, succeeding command          exit=$OK_STATUS   expected 0"
    echo "make verify, real formatting violation     exit=$FAIL_STATUS   expected non-zero"
    echo "make verify, violation removed             exit=$RESTORED_STATUS   expected 0"
    echo "make build,  real compile error            exit=$BUILD_FAIL_STATUS   expected non-zero"
    echo "make build,  compile error removed         exit=$BUILD_RESTORED_STATUS   expected 0"
    echo
    echo "Both public targets are covered, each with its own fixture:"
    echo
    echo "  make verify — a formatting violation at"
    echo "    internal/buildinfo/zz_propagation_probe.go"
    echo "  make build  — a compile error at"
    echo "    internal/buildinfo/zz_build_probe.go"
    echo "    offending file named in the build log: $BUILD_FAIL_IDENTIFIED"
    echo "    compiler message: $BUILD_FAIL_REASON"
    echo
    echo "The build fixture is deliberately separate. \`build\` does not depend on"
    echo "\`verify\`, and the build stage does not run gofmt, so a formatting violation"
    echo "says nothing about whether a build failure propagates."
    echo
    echo "Both fixtures were removed immediately afterwards, and steps 4 and 6 re-run"
    echo "the real targets on the restored tree, so the reported candidate is verified"
    echo "and rebuilt rather than assumed intact."
    echo "make reports its own status for a failed recipe, so the negative cases assert"
    echo "non-zero rather than a specific code."
    echo
    echo "mismatches: $FAILURES"
} > "$EVIDENCE_DIR/f003-failure-propagation.txt"
cat "$EVIDENCE_DIR/f003-failure-propagation.txt"

for _leftover in "$PROBE_FILE" "$BUILD_PROBE_FILE"; do
    if [ -f "$_leftover" ]; then
        echo "error: the probe file $_leftover was not removed" >&2
        exit 1
    fi
done
if [ "$FAILURES" -ne 0 ]; then
    echo "FAILED: $FAILURES expectation(s) not met" >&2
    exit 1
fi
log "PASSED: failures propagate through run-logged.sh, make verify and make build"
