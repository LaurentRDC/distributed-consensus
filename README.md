# distributed-consensus

:warning: This repository is under heavy development. It has been made public so I can get more Github Actions credits.

Haskell implementation of the distributed consensus protocols, with built-in support for deterministic simulation testing.

This repository contains a few packages:

* `raft-consensus`: a Haskell implementation of the Raft consensus protocol, generic over networking and persistence;
* `wal`: a Haskell implementation of write-ahead logs;
* `raft-consensus-wal`: an adapter package to use `raft-consensus` with a persistence layer powered by the write-ahead log of `wal`;
* `raft-consensus-distributed-process`: an adapter package to use `raft-consensus` in the context of Cloud Haskell's `distributed-process` library;
* `linearizable`: linearizability-checker used in the `raft-consensus` test suite.
