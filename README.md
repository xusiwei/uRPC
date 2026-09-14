# urpc

upb based RPC — a cross-platform, gRPC-compatible RPC framework built on
libuv + nghttp2 + upb (protobuf), in C++17 with CMake.

## Build

Third-party C++ dependencies are vendored under `third_party/` by a pinned,
idempotent fetch command (run once from the repository root; needs network
only for this step):

```sh
cmake -P tools/fetch_third_party.cmake
cmake --preset release
cmake --build --preset release
ctest --preset verify
```

Dependency versions/sources/checksums live in
[`third_party/versions.cmake`](third_party/versions.cmake) — the single
reviewed source of truth. Configuration never touches the network; if a
dependency is missing, the build tells you to run the fetch command above.

Python dependencies (gRPC interop tooling) are separate — see
`interop/python/requirements.txt`.

## Layout

- `urpc/core` — runtime kernel (event loop, HTTP/2 sessions, upb codec)
- `urpc/cabi` — stable C-ABI boundary for future language bindings
- `urpc/api` — C++17 public API
- `interop/` — gRPC Python interop peers
- `examples/` — runnable end-to-end examples
- `specs/` — Spec Kit feature specifications
