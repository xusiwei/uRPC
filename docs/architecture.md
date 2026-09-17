# urpc architecture

Cross-platform, gRPC-compatible unary RPC over HTTP/2 (feature 001). The
project constitution (`specs/` governance) and the feature specification
`specs/001-unary-rpc/` are the source of truth; this file is the code-level
orientation.

## Layering (constitution Principle II)

```
urpc/api    C++17 public API: Server/ServerContext, Channel/typed unary calls
urpc/cabi   stable C surface for future language bindings (Python/Lua, phase 2)
urpc/core   kernel: event loop, HTTP/2 sessions, framing, routing, server,
            channel, logging, platform (libuv/nghttp2/upb live only here)
```

- `core` never depends on the layers above; `cabi` wraps `core`; `api`
  wraps both. Dependencies link PRIVATE except where public headers require
  them (libuv for `loop.h`, upb for the typed API).
- Typed API glue (`urpc/unary.h`, `URPC_UNARY_METHOD`) bridges upb-generated
  C message types; string fields are **non-owning views** — source bytes
  must live in the message arena (see tests for the canonical pattern).

## Runtime model (research.md #3)

- Each `Server`/`Channel` owns a `LoopRunner` (dedicated libuv loop thread).
  External threads submit via `Post()`; `uv_async` wakes the loop.
- Client channels are lazy: the first call dials; requests multiplex over
  one HTTP/2 connection (stream-per-call, FR-005).
- Sync calls fast-fail on a urpc event-loop thread instead of deadlocking
  (FR-002). `Shutdown()` drains in-flight calls; idle connections close
  immediately, the grace window protects in-flight work only (FR-012).

## Wire behavior

`H2Session` wraps nghttp2 (bytes in/out, no sockets). `Server::Conn` and
`Channel::Impl` bind sessions to libuv TCP streams and implement gRPC
unary semantics per `specs/001-unary-rpc/contracts/wire-protocol.md`
(5-byte prefix framing, `grpc-timeout`, trailers, status mapping).

## Testing & tooling

- Unit/integration: `urpc/*/test` (GoogleTest) incl. in-memory HTTP/2
  session tests and loopback e2e api tests.
- Process-level e2e: `urpc/e2e/echo/echo_e2e_runner.cpp` (uv_spawn).
- Interop: `urpc/e2e/interop/python` (official gRPC peers, both directions).
- Benchmarks: `urpc/api/bench/bench_unary.cpp`; baselines + >10% gate in
  `tools/baselines`, `tools/compare_baseline.py`.
- Third-party deps: vendored under `third_party/` by the pinned, idempotent
  `tools/fetch_third_party.cmake` (spec 002); configuration never uses the
  network.
