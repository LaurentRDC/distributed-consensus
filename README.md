# raft-consensus

Haskell implementation of the Raft consensus protocol, with built-in support for deterministic simulation testing.

This library lets you bring your own pure state machine, persistence layer, and communication layer to implement a fault-tolerant, strongly-consistent, distributed state machine.

Features of the Raft algorithm include:

* Leader election;
* Log replication;
* Log compaction (i.e. snapshotting);
* Membership changes;
* Client request pipelining;

_Upcoming_ Raft features include:

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

Cluster properties are tested under two modes: with, and without, schedule exploration.

In both cases, the cluster's initial conditions are generated at random by QuickCheck: timeouts, cluster configuration, fault schedules, etc.

Without schedule exploration, a simulated trace is run from these initial conditions. This is relatively fast, and allows to explore the behavior of the system under many initial conditions. You can tweak the number of tests run in this mode using the `quichcheck-tests` option. For example:

```sh
cabal test --test-options='--quickcheck-tests=1000'
```

With schedule exploration, the system is started in randomly-generated conditions. However, whenever the simulation reaches a branching-point, timing-wise, both branches are explored separately as tests. This is much slower, but allows to detect races in the execution of a cluster. You can tweak the number of tests in this mode using the `num-racy-tests` option. For example:

```sh
cabal test --test-options='--num-racy-tests=5'
```

### Replaying a failure


During the course of development, you may see a test failure for the deterministic simulation test suite:

```sh
$ cabal test
Running 1 test suites...
Test suite raft-consensus-test: RUNNING...
raft-consensus
  Raft
    Property tests
      Cluster properties: FAIL
        <snip>
        Use --quickcheck-replay="(SMGen 6009993106432336185 7272677387067049729,0)" to reproduce.
        <snip>
```

This is great news! You can replay the specific failing scenario using `--quickcheck-replay` in combination with `test-options`:

```sh
$ cabal test --test-options='--quickcheck-replay="(SMGen 6009993106432336185 7272677387067049729,0)"'
```
