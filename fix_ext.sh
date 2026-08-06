#!/usr/bin/env bash
#
# Unwedge the VSCode Remote-SSH extension host.
#
# Symptom: every remote extension is silently dead at once — Claude, Python,
# git, Copilot. Typically surfaces as "command 'claude-vscode.*' not found".
#
# Why "Developer: Reload Window" cannot fix it: the server sets
# VSCODE_RECONNECTION_GRACE_TIME=10800000ms (3 hours). When a flaky link drops
# the renderer<->host connection, the host survives in an empty state, and for
# the next 3 hours every reload REATTACHES to that same empty host instead of
# spawning a fresh one. The host must be killed server-side to break the cycle.

set -uo pipefail

readonly EXTHOST_PATTERN='bootstrap-fork.*type=extensionHost'
readonly RESPAWN_TIMEOUT=150
readonly HEALTHY_EXT_COUNT=3


main() {
    local mode="${1:-run}"

    local server_root; server_root=$(find_server_root)
    local logs_dir="$server_root/data/logs"

    report_current_state "$logs_dir"

    case "$mode" in
        --check)   exit 0 ;;
        --dry-run) show_kill_targets; exit 0 ;;
        run)       ;;
        *)         die "usage: $(basename "$0") [--check|--dry-run]" ;;
    esac

    if extensions_are_active "$logs_dir"; then
        echo "Extensions are already active — nothing to fix."
        echo "(If a command is still missing, it is NOT this wedge.)"
        exit 0
    fi

    show_kill_targets
    kill_extension_hosts
    wait_for_activation "$logs_dir"
}


# The live server tree is NOT under $HOME on this box (it is /var/tmp/$USER),
# so derive it from a running process rather than guessing.
find_server_root() {
    local root
    root=$(pgrep -u "$USER" -af 'vscode-server' \
           | grep -oE '/[^ ]*/\.vscode-server' \
           | head -1)

    [[ -n "$root" ]] || root="/var/tmp/$USER/.vscode-server"
    [[ -d "$root" ]] || die "no .vscode-server tree found (looked at $root)"

    echo "$root"
}


# A healthy extension host writes one log subdirectory per activated extension.
# A wedged host has only remoteexthost.log, so the count is zero.
count_active_extensions() {
    local logs_dir=$1
    find "$logs_dir" -maxdepth 3 -mindepth 2 -type d -newermt '-10 minutes' \
         2>/dev/null | grep -c 'exthost'
}


extensions_are_active() {
    [[ $(count_active_extensions "$1") -ge $HEALTHY_EXT_COUNT ]]
}


report_current_state() {
    local logs_dir=$1

    echo "server tree : $(dirname "$logs_dir" | xargs dirname)"
    echo "exthosts    : $(pgrep -u "$USER" -c -f "$EXTHOST_PATTERN" || echo 0) running"
    echo "active exts : $(count_active_extensions "$logs_dir") (>=$HEALTHY_EXT_COUNT means healthy)"
    echo
}


show_kill_targets() {
    echo "would kill:"
    local pid build
    for pid in $(pgrep -u "$USER" -f "$EXTHOST_PATTERN"); do
        [[ $pid == $$ ]] && continue
        build=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null \
                | grep -oE 'Stable-[a-f0-9]{8}' | head -1)
        printf '  pid=%-8s build=%-18s age=%s\n' \
               "$pid" "${build:-?}" "$(ps -p "$pid" -o etime= | tr -d ' ')"
    done
    echo
}


# -u "$USER" matters: this is a shared box and an unscoped pattern also matches
# other users' extension hosts.
kill_extension_hosts() {
    pkill -u "$USER" -f "$EXTHOST_PATTERN" \
        && echo "SIGTERM sent." \
        || die "no extension host matched — pattern may be stale"
}


# The critical step. Respawn takes ~25s and activation ~4s, but a window reload
# inside this period kills the host mid-load and restores the wedge.
wait_for_activation() {
    local logs_dir=$1

    echo
    echo ">>> DO NOT reload the window until this finishes. <<<"
    echo

    local waited=0
    while (( waited < RESPAWN_TIMEOUT )); do
        sleep 5; (( waited += 5 ))

        local hosts; hosts=$(pgrep -u "$USER" -c -f "$EXTHOST_PATTERN")
        local exts;  exts=$(count_active_extensions "$logs_dir")
        printf '  t=%-4s exthosts=%s active_exts=%s\n' "${waited}s" "$hosts" "$exts"

        if (( exts >= HEALTHY_EXT_COUNT )); then
            echo
            echo "Recovered. Extensions are activating again."
            show_claude_status "$logs_dir"
            return 0
        fi
    done

    echo
    echo "Still not activating after ${RESPAWN_TIMEOUT}s."
    echo "The window may be holding a dead connection — close it and reconnect."
    return 1
}


show_claude_status() {
    local log
    log=$(find "$1" -ipath '*Anthropic.claude-code/Claude VSCode.log' \
          -newermt '-10 minutes' 2>/dev/null | head -1)

    [[ -n "$log" ]] && { echo; tail -3 "$log"; }
}


die() {
    echo "error: $*" >&2
    exit 1
}


main "$@"
