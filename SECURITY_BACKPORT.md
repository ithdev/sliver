# C2 envelope allocation backport

This branch starts from Sliver v1.5.44 (`9122878c`) and backports bounded
length-prefixed framing for CVE-2026-32941 / GHSA-97vp-pwqj-46qc.

The framing contract is:

- mTLS, WireGuard, and pivot frames are limited to 64 MiB.
- The server permits at most 128 MiB of C2 envelope data in flight across all
  mTLS and WireGuard connections.
- Both readers and writers reject frames outside the contract before writing or
  allocating the body.
- The server holds each reservation through response routing or handler
  execution, then releases it explicitly.

The implant-side checks are based on the later upstream fixes in Sliver pull
requests #2292 and #2305. This backport uses a lower limit because it targets
older Linux hosts where the upstream 512 MiB allocation bound may itself exhaust
memory. It also adds the server-wide budget missing from those changes.

Sliver v1.5.44 sends some file transfers in a single protobuf envelope. Uploads,
downloads, or command output that produce an encoded frame larger than 64 MiB
will therefore fail with a frame-length error and must be split or transferred
through another approved channel.
