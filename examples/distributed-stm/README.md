# `dstm` -- a distributed software transactional memory implementation on `raft-consensus`

This directory contains an example implementation of a distributed software transactional memory backend. Instead of the `STM` monad and `TVar`, we implement `DSTM` and `DVar`.

For demonstration purposes, the networking is implemented using the Cloud Haskell implementation [`distributed-process`](https://github.com/haskell-distributed/distributed-process) via the `raft-consensus-distributed-process` package. The persistence layer turned off.

This demo contains a single executable, `dstm-bank`, which shows multiple nodes transferring money between bank accounts, while balances are always conserved.

## Running the example

In order to execute the example, use the `just dstm-bank` recipe:

```console
$ just dstm-bank n=3
Wed Nov 25 14:19:15 UTC 2026 pid://127.0.0.1:4001:0:8: Account sum: 30 (7 + 11 + 12)
Wed Nov 25 14:19:15 UTC 2026 pid://127.0.0.1:4000:0:8: Account sum: 30 (4 + 12 + 14)
Wed Nov 25 14:19:16 UTC 2026 pid://127.0.0.1:4002:0:8: Account sum: 30 (1 + 13 + 16)
Wed Nov 25 14:19:16 UTC 2026 pid://127.0.0.1:4000:0:8: Account sum: 30 (3 + 10 + 17)
Wed Nov 25 14:19:16 UTC 2026 pid://127.0.0.1:4001:0:8: Account sum: 30 (5 + 7 + 18)
...
```

The account sum should always be constant!
