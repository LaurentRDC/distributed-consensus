# hs-raft

Haskell implementation of the Raft consensus protocol, with built-in support for deterministic simulation testing.

This library lets you bring your own pure state machine, persistence layer, and communication layer to implement a fault-tolerant, strongly-consistent, distributed state machine.

Features of the Raft algorithm include:

* Leader election;
* Log replication;
* Log compaction (i.e. snapshotting);

_Upcoming_ Raft features include:

* Membership changes;
* Read-only queries optimizations

The goal is to have feature-parity with industry standard implementations such as [etcd's Raft implementation](https://github.com/etcd-io/raft).

## Development

This library can be built using `cabal`:

```sh
$ cabal build
```

and tests can be run with:

```sh
$ cabal test
```

### Deterministic simulation testing

During the course of development, you may see a test failure for the deterministic simulation test suite:

```sh
$ cabal test
Running 1 test suites...
Test suite hs-raft-test: RUNNING...
hs-raft
  Raft
    Property tests
      Cluster properties: FAIL (0.05s)
        <snip>
        Use --quickcheck-replay="(SMGen 6009993106432336185 7272677387067049729,0)" to reproduce.
        <snip>
```

This is great news! You can replay the specific failing scenario using `--quickcheck-replay` in combination with `test-options`:

```sh
$ cabal test --test-options='--quickcheck-replay="(SMGen 6009993106432336185 7272677387067049729,0)"'
```
