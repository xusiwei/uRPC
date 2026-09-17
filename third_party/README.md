# Vendored C++ dependencies

This directory holds the vendored source of every C++ third-party dependency
of urpc (work product — git-ignored except `versions.cmake` and this README).

Populate it with the fetch command (run from the repository root):

```sh
cmake -P tools/fetch_third_party.cmake
```

The command is idempotent: already-vendored dependencies at the pinned
versions are skipped. Versions, source URLs, SHA256 checksums and licenses
live in [`versions.cmake`](versions.cmake) — the single reviewed source of
truth. Upgrading a dependency means editing that one file and re-running the
fetch command.

Current set: libuv, nghttp2, protobuf (upb runtime + host protoc source),
abseil (build dependency of protoc), googletest, benchmark.

Python dependencies (interop tooling) are deliberately NOT vendored here;
see `urpc/e2e/interop/python/requirements.txt`.

Each vendored directory contains a `.urpc-version` marker (version + SHA256)
written after a verified fetch; the build refuses to configure when a marker
is missing or stale, and tells you to run the fetch command above.
