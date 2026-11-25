#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] [N]

Start N dstm servers.

Arguments:
  N                 Number of servers to start (default: 5)

Options:
  -h, --help        Show this help message
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
    rm -rf "$repo_root/.dstm"
fi

start=4000
ports=($(seq "$start" $((start + n - 1))))

pids=()

cleanup() {
    echo "Stopping servers..."
    kill "${pids[@]}" 2> /dev/null || true
    wait
}
trap cleanup INT TERM EXIT

cabal build dstm-bank

leader=true

for port in "${ports[@]}"; do
    # We want to pass all of the ports to each executable, EXCEPT
    # for its own port of course!
    args=()
    for p in "${ports[@]}"; do
        [[ "$p" != "$port" ]] && args+=(--peer "$p")
    done

    # We want a single leader; the executable that creates
    # the DVars
    if "$leader"; then
        args+=(--leader)
        leader=false
    fi

    mkdir -p "$repo_root/.dstm/$port"

    cabal run dstm-bank -- --port="$port" "${args[@]}" > "$repo_root/.dstm/$port/output.log" &
    pids+=("$!")
done

wait