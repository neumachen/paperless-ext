#!/bin/sh
# Remove exactly what this run created, and nothing else.
#
# The project name comes from the generated .env, which is the same value the
# Makefile tags images with and the same value scripts/lib.sh resolves. Every
# removal below is scoped by that name or by this run's image references.
#
# There is no `docker system prune`, no `docker volume prune` and no
# `docker image prune` here, deliberately: those commands are scoped to the
# DAEMON, not to this run, and on a machine that also carries a developer
# stack they would remove that stack's volumes and images.
#
# errexit is off on purpose. Every step is attempted and every failure is
# reported; the script then exits non-zero so a leak is visible rather than
# swallowed.
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/deploy/filename-normalizer"

if [ ! -f "$DEPLOY_DIR/.env" ]; then
    echo "no $DEPLOY_DIR/.env: nothing was created, so nothing is removed."
    exit 0
fi

read_key() { sed -n "s/^$1=//p" "$DEPLOY_DIR/.env" | head -1; }

PROJECT="$(read_key COMPOSE_PROJECT_NAME)"
PREFIX="$(read_key FN_IMAGE_PREFIX)"
VERSION="$(read_key FN_VERSION)"

if [ -z "$PROJECT" ]; then
    echo "error: .env names no COMPOSE_PROJECT_NAME; refusing to guess what to remove." >&2
    exit 1
fi
if [ "$PROJECT" = "fnfoundation" ]; then
    echo "error: .env names the developer project 'fnfoundation'; refusing to tear it down." >&2
    exit 1
fi
if [ -n "${COMPOSE_PROJECT_NAME:-}" ] && [ "$COMPOSE_PROJECT_NAME" != "$PROJECT" ]; then
    echo "error: COMPOSE_PROJECT_NAME is '$COMPOSE_PROJECT_NAME' but .env names '$PROJECT';" >&2
    echo "       the two disagree, so a scoped teardown cannot be trusted." >&2
    exit 1
fi

failures=0
echo "tearing down project $PROJECT"

# Every profile, so a fault-profile or tools-profile container left behind by
# an interrupted phase is removed too. --volumes is scoped to this project's
# named volumes by Compose itself.
( cd "$DEPLOY_DIR" \
  && COMPOSE_PROJECT_NAME="$PROJECT" docker compose \
       --profile test --profile tools --profile fault --profile consumer \
       down --remove-orphans --volumes --timeout 30 ) \
  || { echo "error: compose down failed for project $PROJECT." >&2; failures=$((failures + 1)); }

# This run's images, by exact reference. Nothing is matched by pattern.
if [ -n "$PREFIX" ] && [ -n "$VERSION" ]; then
    for image in "$PREFIX/watcher:$VERSION" "$PREFIX/renamer:$VERSION" \
                 "$PREFIX/fnctl:$VERSION" "$PREFIX/integration:$VERSION"; do
        if docker image inspect "$image" >/dev/null 2>&1; then
            docker image rm "$image" >/dev/null \
                || { echo "error: could not remove image $image." >&2; failures=$((failures + 1)); }
        fi
    done
fi
# `make verify` tags its stage <project>/verify:local.
if docker image inspect "$PROJECT/verify:local" >/dev/null 2>&1; then
    docker image rm "$PROJECT/verify:local" >/dev/null \
        || { echo "error: could not remove image $PROJECT/verify:local." >&2; failures=$((failures + 1)); }
fi

# --- prove the scope ------------------------------------------------------
# Compose labels everything it creates with its project. A non-zero count here
# means something of this run's survived; it says nothing about, and touches
# nothing of, any other project.
left_containers="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" | wc -l | tr -d ' ')"
left_volumes="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT" | wc -l | tr -d ' ')"
left_networks="$(docker network ls -q --filter "label=com.docker.compose.project=$PROJECT" | wc -l | tr -d ' ')"
echo "remaining for $PROJECT: containers=$left_containers volumes=$left_volumes networks=$left_networks"
if [ "$left_containers" != "0" ] || [ "$left_volumes" != "0" ] || [ "$left_networks" != "0" ]; then
    echo "error: resources labelled for project $PROJECT survived the teardown." >&2
    failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
    echo "teardown reported $failures failure(s); resources may have leaked." >&2
    exit 1
fi
echo "teardown complete; only project $PROJECT was touched."
