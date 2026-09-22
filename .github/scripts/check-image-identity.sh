#!/bin/sh
# Ask each built image what it is, and refuse it if the answer is not the
# release being made.
#
# The applications carry a `version` subcommand that prints the values stamped
# into them at build time. Running it is the only way to establish that the
# image about to be published was compiled from the intended commit: the tag on
# an image is a label somebody wrote, and a build that silently did not run
# leaves an older image under the same label.
#
# Inputs (environment):
#   WATCHER_IMAGE      required. Local reference of the built watcher image.
#   RENAMER_IMAGE      required. Local reference of the built renamer image.
#   EXPECTED_VERSION   required. The version the release is being cut as.
#   EXPECTED_REVISION  required. The full commit SHA being released.
set -eu

die() { echo "error: $*" >&2; exit 1; }

[ -n "${WATCHER_IMAGE:-}" ]     || die "WATCHER_IMAGE is required."
[ -n "${RENAMER_IMAGE:-}" ]     || die "RENAMER_IMAGE is required."
[ -n "${EXPECTED_VERSION:-}" ]  || die "EXPECTED_VERSION is required."
[ -n "${EXPECTED_REVISION:-}" ] || die "EXPECTED_REVISION is required."

REPORT=""
add() { REPORT="$REPORT$1
"; }

# The source digest is a content hash of the build context, computed inside the
# build. Both images come from one context, so the two must report the same
# value; if they do not, one of them was served from a stale layer.
WATCHER_SOURCE=""
RENAMER_SOURCE=""

check() {
    _c_image="$1"; _c_binary="$2"; _c_which="$3"

    _c_platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$_c_image")" \
        || die "$_c_image was not loaded into the daemon."
    [ "$_c_platform" = "linux/amd64" ] \
        || die "$_c_image is $_c_platform; this release publishes linux/amd64."

    # The local image id. It identifies a blob in THIS daemon and is not a
    # registry digest: nothing outside this runner can pull by it.
    _c_id="$(docker image inspect --format '{{.Id}}' "$_c_image")"

    # The image's entrypoint is the application itself, so this runs the real
    # binary that would run in production.
    _c_out="$(docker run --rm --platform linux/amd64 "$_c_image" version)" \
        || die "$_c_binary did not run inside $_c_image."

    case "$_c_out" in
        "$_c_binary $EXPECTED_VERSION revision=$EXPECTED_REVISION source="*) ;;
        *)
            echo "error: $_c_image reports an identity this release did not ask for." >&2
            echo "  expected prefix: $_c_binary $EXPECTED_VERSION revision=$EXPECTED_REVISION source=..." >&2
            echo "  reported:        $_c_out" >&2
            exit 1
            ;;
    esac

    _c_source="$(printf '%s' "$_c_out" | sed -n 's/.* source=\([^ ]*\) .*/\1/p')"
    [ -n "$_c_source" ] && [ "$_c_source" != "unknown" ] \
        || die "$_c_image reports no source digest; the build did not stamp one."

    if [ "$_c_which" = watcher ]; then
        WATCHER_SOURCE="$_c_source"
    else
        RENAMER_SOURCE="$_c_source"
    fi

    add "$_c_which"
    add "  image        $_c_image"
    add "  platform     $_c_platform"
    add "  local id     $_c_id   (this daemon only; not a registry digest)"
    add "  version says $_c_out"
}

check "$WATCHER_IMAGE" fn-watcher watcher
check "$RENAMER_IMAGE" fn-renamer renamer

[ "$WATCHER_SOURCE" = "$RENAMER_SOURCE" ] || die \
    "the two images report different source digests ($WATCHER_SOURCE and $RENAMER_SOURCE); they were not built from one tree."
add "both images report source digest $WATCHER_SOURCE"
add "both images report version $EXPECTED_VERSION at revision $EXPECTED_REVISION"

printf '%s' "$REPORT"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
    delimiter="report-$(date +%s)-$$"
    {
        echo "report<<$delimiter"
        printf '%s' "$REPORT"
        echo "$delimiter"
    } >> "$GITHUB_OUTPUT"
fi
