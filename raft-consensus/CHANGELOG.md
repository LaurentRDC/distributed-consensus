# Revision history for raft-consensus

## 0.1.1.0 -- Unreleased

* Improved that latency of service read-only requests. In version 0.1.0.0, a read-only request
  could never be served faster than a heartbeat period. Starting with version 0.1.1.0, read-only
  requests trigger a heartbeat when appropriate, cutting latency by up to 40%.

## 0.1.0.0 -- 2026-11-26

* First version. Released on an unsuspecting world.
