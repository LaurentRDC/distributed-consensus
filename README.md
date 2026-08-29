# distributed-consensus

:warning: This repository is under heavy development. It has been made public so I can get more Github Actions credits.

Haskell implementation of the distributed consensus protocols, with built-in support for deterministic simulation testing.

Currently, three packages are being worked on:

* `raft-consensus`: a Haskell implementation of the Raft consensus protocol, generic over networking and persistence.
* `wal`: a Haskell implementation of write-ahead logs.
* `distributed-process-raft-consensus`: an adapter package to use `raft-consensus` in the context of Cloud Haskell's `distributed-process` library.
