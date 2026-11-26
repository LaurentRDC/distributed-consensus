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

    cabal run dstm-bank -- --port="$port" "${args[@]}" &
    pids+=("$!")
done

wait