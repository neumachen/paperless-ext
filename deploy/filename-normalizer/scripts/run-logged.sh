#!/bin/sh
# Run a command, tee its output to a log file, and exit with the command's
# own status.
#
# This exists because `cmd | tee log` reports tee's status, not cmd's. Under
# /bin/sh there is no pipefail to rely on, so a failed containerized build or
# verification would exit 0 and a documented `make build && make test` chain
# would carry on and test whatever images happened to already exist.
#
# errexit is deliberately NOT enabled: it would abort the subshell the moment
# the command failed, before the status could be recorded, which is the same
# class of mistake this script exists to fix.
set -u

if [ "$#" -lt 2 ]; then
    echo "usage: run-logged.sh <logfile> <command> [args...]" >&2
    exit 2
fi

log="$1"
shift
mkdir -p "$(dirname "$log")" || exit 2
status_file="$log.status"
rm -f "$status_file"

# The status is captured inside the pipeline, using `if` so the command's
# failure is tested rather than fatal, and read back afterwards.
{
    if "$@" 2>&1; then
        echo 0 > "$status_file"
    else
        echo "$?" > "$status_file"
    fi
} | tee "$log"

status="$(cat "$status_file" 2>/dev/null || echo 127)"
rm -f "$status_file"

case "$status" in
    ''|*[!0-9]*) status=127 ;;
esac

if [ "$status" -ne 0 ]; then
    printf '\n\033[1;31m==> FAILED (exit %s): %s\033[0m\n' "$status" "$1" >&2
    printf '    full output: %s\n' "$log" >&2
fi
exit "$status"
