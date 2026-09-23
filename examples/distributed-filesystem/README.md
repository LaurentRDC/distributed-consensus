# `distfs` -- a distributed in-memory filesystem build on `raft-consensus`

This directory contains an example implementation of a distributed, in-memory filesystem that uses `raft-consensus`.

For demonstration purposes, the networking is implemented using the Cloud Haskell implementation [`distributed-process`](https://github.com/haskell-distributed/distributed-process) via the `distributed-process-raft-consensus` package. The persistence layer is implemented using the `wal` package.

This implementation contains two executables. `distfs-server` implements the server-side logic, while `distfs-client` implements a small command-line interface to the cluster.

## Running the example

In order to execute the example, check out the `examples/distributed-filesystem/run-servers.sh` script. Its help text reads:

```bash
$ ./examples/distributed-filesystem/run-servers.sh -h
Usage: ./examples/distributed-filesystem/run-servers.sh [OPTIONS] [N]

Start N distfs servers.

Arguments:
  N                 Number of servers to start (default: 5)

Options:
  -h, --help        Show this help message
  --keep-state      Don't remove the existing .distfs directory
```

This will launch N servers, and keep their persistent state under `.distfs`.

Once the cluster is running, you can interact with the cluster using the `distfs-client` executable. For example:

```bash
$ cabal run distfs-client -- touch hello.txt
```

Alternatively, using `just`, you can start the servers with `just distfs-servers` and run client commands with `just distfs-client`.