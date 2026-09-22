#!/bin/sh
# Give one run -- a GitHub Actions run, a re-run attempt, or a local rehearsal
# -- its own Compose project, image namespace, image tag, host ports and
# credentials.
#
# Why this exists rather than a bare `make env`:
#
#   1. `make env` copies .env.example verbatim, and .env.example names the
#      developer project `fnfoundation`. Two runs on one machine would then
#      share containers, volumes and image tags, and a teardown scoped to that
#      name would remove a developer's running stack.
#
#   2. The Makefile and scripts/lib.sh resolve the project name DIFFERENTLY.
#      lib.sh prefers a shell COMPOSE_PROJECT_NAME and falls back to .env, the
#      way Compose itself resolves it; the Makefile reads .env only. A run that
#      exported one name and generated another would have the Makefile tagging
#      images for one project while the orchestration scripts stopped, started
#      and removed resources in another. This script writes the name into .env
#      AND exports the same value, then asserts the two agree before anything
#      is built.
#
# Credentials are generated fresh for every run by `make env`, inside a
# container, and are never printed here.
#
# Inputs (environment):
#   FN_CI_ID        required. Unique per run AND per attempt.
#   FN_CI_VERSION   required. The image tag this run's images carry.
#   FN_CI_PORT_BASE optional, default 18080. The applications' three published
#                   host ports are this, +1 and +2. A local rehearsal must pass
#                   a base that does not collide with a running stack.
set -eu

die() { echo "error: $*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/deploy/filename-normalizer"

[ -n "${FN_CI_ID:-}" ]      || die "FN_CI_ID is required."
[ -n "${FN_CI_VERSION:-}" ] || die "FN_CI_VERSION is required."
PORT_BASE="${FN_CI_PORT_BASE:-18080}"
case "$PORT_BASE" in
    ''|*[!0-9]*) die "FN_CI_PORT_BASE must be a number, got '$PORT_BASE'." ;;
esac

# Compose project names must start with a letter or digit and hold only
# lowercase letters, digits, hyphens and underscores. Image repository names
# have the same lowercase restriction, so one sanitiser serves both.
sanitize_name() {
    printf '%s' "$1" | LC_ALL=C tr '[:upper:]' '[:lower:]' \
        | sed 's/[^a-z0-9]\{1,\}/-/g; s/^-*//; s/-*$//'
}

# Image tags additionally allow dots and underscores and may not begin with a
# separator.
sanitize_tag() {
    printf '%s' "$1" | LC_ALL=C tr '[:upper:]' '[:lower:]' \
        | sed 's/[^a-z0-9._-]\{1,\}/-/g; s/^[._-]*//; s/[._-]*$//'
}

RUN_SLUG="$(sanitize_name "$FN_CI_ID")"
[ -n "$RUN_SLUG" ] || die "FN_CI_ID '$FN_CI_ID' sanitises to an empty name."
PROJECT="fnci-$RUN_SLUG"
IMAGE_PREFIX="fnci-$RUN_SLUG"
VERSION="$(sanitize_tag "$FN_CI_VERSION")"
[ -n "$VERSION" ] || die "FN_CI_VERSION '$FN_CI_VERSION' sanitises to an empty tag."

# fnfoundation is the developer stack's project: a run that adopted it would
# build over that stack's images and a scoped teardown would delete its
# containers and volumes. With the fnci- prefix above this cannot trigger, and
# it is not what protects that stack -- the .env ownership check below is. It
# guards a future change to the derivation, not this one.
[ "$PROJECT" != "fnfoundation" ] || die "refusing to run as the developer project 'fnfoundation'."

cd "$DEPLOY_DIR"

# A .env belonging to something else is never overwritten. An identical one is
# accepted so a step can be re-run inside the same job.
if [ -f .env ]; then
    existing="$(sed -n 's/^COMPOSE_PROJECT_NAME=//p' .env | head -1)"
    [ "$existing" = "$PROJECT" ] || die \
        "$DEPLOY_DIR/.env already exists for project '$existing', not '$PROJECT'. Refusing to overwrite it."
    echo "reusing the existing .env for project $PROJECT"
else
    # Generates the three passwords in a container and writes the secret files.
    make env >/dev/null
fi

set_key() {
    _sk_key="$1"; _sk_value="$2"
    grep -q "^$_sk_key=" .env \
        || die "$DEPLOY_DIR/.env has no $_sk_key line to set; .env.example has changed."
    sed -i.bak "s|^$_sk_key=.*|$_sk_key=$_sk_value|" .env
    rm -f .env.bak
}

set_key COMPOSE_PROJECT_NAME "$PROJECT"
set_key FN_IMAGE_PREFIX      "$IMAGE_PREFIX"
set_key FN_VERSION           "$VERSION"
set_key FN_WATCHER_PORT      "$PORT_BASE"
set_key FN_RENAMER1_PORT     "$((PORT_BASE + 1))"
set_key FN_RENAMER2_PORT     "$((PORT_BASE + 2))"
chmod 0600 .env

# The secret files are rewritten from the patched .env, so the files the
# containers mount and the values in .env cannot drift apart.
make secrets >/dev/null

# --- assertions -----------------------------------------------------------
# Each of these is a way a run has to be able to fail EARLY rather than half
# way through a ninety-minute suite.

env_project="$(sed -n 's/^COMPOSE_PROJECT_NAME=//p' .env | head -1)"
[ "$env_project" = "$PROJECT" ] \
    || die "the generated .env names project '$env_project', not '$PROJECT'."

# lib.sh prefers a shell COMPOSE_PROJECT_NAME over the .env value. If one is
# already exported it must be the same name, or the scripts and the Makefile
# would operate on two different projects.
if [ -n "${COMPOSE_PROJECT_NAME:-}" ] && [ "$COMPOSE_PROJECT_NAME" != "$PROJECT" ]; then
    die "COMPOSE_PROJECT_NAME is exported as '$COMPOSE_PROJECT_NAME' but .env names '$PROJECT'."
fi

for key in FN_DB_PASSWORD FN_REPLICATION_PASSWORD FN_AMQP_PASSWORD; do
    value="$(sed -n "s/^$key=//p" .env | head -1)"
    [ -n "$value" ] || die "$key is empty in the generated .env."
done
for f in secrets/fn_db_password secrets/fn_amqp_password; do
    [ -s "$f" ] || die "$DEPLOY_DIR/$f is missing or empty."
done

# Proves the file interpolates with these values and that the secret files the
# `secrets:` section names are present. A broken .env fails here, not during a
# build.
COMPOSE_PROJECT_NAME="$PROJECT" docker compose --profile test --profile tools config --quiet \
    || die "docker compose could not resolve the stack with the generated .env."

# --- publish the identity -------------------------------------------------
# Exported so lib.sh, the Makefile and docker compose all see the same name.
if [ -n "${GITHUB_ENV:-}" ]; then
    {
        echo "COMPOSE_PROJECT_NAME=$PROJECT"
        echo "FN_IMAGE_PREFIX=$IMAGE_PREFIX"
        echo "FN_VERSION=$VERSION"
    } >> "$GITHUB_ENV"
fi

cat <<EOF
isolated stack identity
  COMPOSE_PROJECT_NAME = $PROJECT
  FN_IMAGE_PREFIX      = $IMAGE_PREFIX
  FN_VERSION           = $VERSION
  published ports      = $PORT_BASE, $((PORT_BASE + 1)), $((PORT_BASE + 2))
  credentials          = generated for this run; not printed
EOF
