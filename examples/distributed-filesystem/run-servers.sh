#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] [N]

Start N distfs servers.

Arguments:
  N                 Number of servers to start (default: 5)

Options:
  -h, --help        Show this help message
  --keep-state      Don't remove the existing .distfs directory
EOF
}

n=5
keep_state=false
# The script isn't at the root of the repo, but
# directories such as .distfs are at the root of the repo,
# so we need to detect it
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"

while (($# > 0)); do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --keep-state)
            keep_state=true
            shift
            ;;
        -*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
        *)
            if [[ "$n" != 5 ]]; then
                echo "Too many arguments" >&2
                usage >&2
                exit 1
            fi
            n=$1
            shift
            ;;
    esac
done

if ! "$keep_state"; then
    rm -rf "$repo_root/.distfs"
fi

# We know that there's going to be at least one server
# whose port is 4000. That's the port
# we use for the client CLI by default
start=4000
ports=($(seq "$start" $((start + n - 1))))

pids=()

cleanup() {
    echo "Stopping servers..."
    kill "${pids[@]}" 2> /dev/null || true
    wait
}
trap cleanup INT TERM EXIT

cabal build distfs-server

for port in "${ports[@]}"; do
    # We want to pass all of the ports to each executable, EXCEPT
    # for its own port of course!
    args=()
    for p in "${ports[@]}"; do
        [[ "$p" != "$port" ]] && args+=(--peer "$p")
    done

    mkdir -p "$repo_root/.distfs/$port"

    cabal run distfs-server -- --port="$port" "${args[@]}" > "$repo_root/.distfs/$port/events.log" &
    pids+=("$!")
done

wait