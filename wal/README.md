# wal

Implementation of write-ahead logs in Haskell.

Write-ahead logs are append-only durable logs, which are often used for persistence of data in databases or other
fault-tolerant distributed systems.

The implementation in this package features multi-threaded writers, batching, and simultaneous readers, for maximum
throughput.
