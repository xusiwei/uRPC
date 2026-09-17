#!/usr/bin/env python3
"""Interop driver (spec 001 US5 / 004 US5): runs the interop quadrants.

  A)     official client (grpcio) -> urpc echo server
  B)     urpc echo client -> official server (peer_server.py)
  A-str) official streaming client -> urpc streaming server
  B-str) urpc streaming client -> official streaming server

Exit code 0 = all attempted quadrants passed. Skips (exit 77) when
grpcio is missing so environments without Python deps don't fail the
build.
"""

import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))


def find_bin(name):
    for d in ("build/release", "build/debug", "build/ci-repro"):
        base = os.path.join(ROOT, d, "urpc", "e2e")
        for sub in ("echo", "streaming"):
            p = os.path.join(base, sub, name)
            if os.path.exists(p):
                return os.path.abspath(p)
    return None


def wait_port_ok(addr, timeout=10.0):
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection(addr.split(":"), timeout=0.5)
            s.close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def gen_streaming_stubs():
    """Generates streaming_pb2*.py next to this script via grpcio-tools.

    Prefers the canonical example proto so the Python peers always speak
    exactly the wire contract of the C++ example service. Returns True
    when the modules are importable afterwards.
    """
    proto = os.path.join(ROOT, "urpc", "e2e", "streaming", "streaming.proto")
    if not os.path.exists(proto):
        proto = os.path.join(HERE, "streaming.proto")
    if not os.path.exists(proto):
        return False
    try:
        from grpc_tools import protoc
        rc = protoc.main([
            "protoc",
            "-I" + os.path.dirname(proto),
            "--python_out=" + HERE,
            "--grpc_python_out=" + HERE,
            proto,
        ])
        if rc != 0:
            return False
    except ImportError:
        pass  # no grpcio-tools: fall back to pre-generated stubs below
    try:
        import streaming_pb2  # noqa: F401
        import streaming_pb2_grpc  # noqa: F401
        return True
    except ImportError:
        return False


def main():
    try:
        import grpc  # noqa: F401
    except ImportError:
        print("interop: SKIP (grpcio not installed)")
        return 77

    server_bin = find_bin("urpc_echo_server")
    client_bin = find_bin("urpc_echo_client")
    stream_server_bin = find_bin("urpc_streaming_server")
    stream_client_bin = find_bin("urpc_streaming_client")
    if not server_bin or not client_bin:
        print("interop: SKIP (example binaries not built)")
        return 77

    failures = 0

    # ---- A) official python client -> urpc echo server -------------------
    port_a = "51081"
    proc_s = subprocess.Popen([server_bin, "127.0.0.1:" + port_a])
    try:
        if not wait_port_ok("127.0.0.1:" + port_a):
            print("interop: urpc server did not come up")
            failures += 1
        else:
            rc = subprocess.call(
                [sys.executable, os.path.join(HERE, "peer_client.py"),
                 "127.0.0.1:" + port_a])
            if rc != 0:
                print("interop: quadrant A (official->urpc) FAILED")
                failures += 1
            else:
                print("interop: quadrant A (official->urpc) ok")
    finally:
        proc_s.terminate()
        proc_s.wait()

    # ---- B) urpc echo client -> official python server --------------------
    port_b = "51082"
    proc_p = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "peer_server.py"), port_b])
    try:
        if not wait_port_ok("127.0.0.1:" + port_b):
            print("interop: python server did not come up")
            failures += 1
        else:
            rc = subprocess.call([client_bin, "127.0.0.1:" + port_b])
            if rc != 0:
                print("interop: quadrant B success path FAILED")
                failures += 1
            else:
                print("interop: quadrant B success path ok")
            rc = subprocess.call(
                [client_bin, "127.0.0.1:" + port_b, "--expect-error"])
            if rc != 0:
                print("interop: quadrant B error path FAILED")
                failures += 1
            else:
                print("interop: quadrant B error path ok")
    finally:
        proc_p.terminate()
        proc_p.wait()

    # ---- streaming quadrants (spec 004 US5) --------------------------------
    have_stream_stubs = gen_streaming_stubs()
    have_stream_bins = stream_server_bin and stream_client_bin
    if not have_stream_stubs:
        print("interop: streaming SKIP (grpcio-tools/stubs unavailable)")
    elif not have_stream_bins:
        print("interop: streaming SKIP (streaming example binaries missing)")

    if have_stream_stubs and have_stream_bins:
        # A-str) official streaming client -> urpc streaming server
        port_c = "51083"
        proc_c = subprocess.Popen([stream_server_bin, "127.0.0.1:" + port_c])
        try:
            if not wait_port_ok("127.0.0.1:" + port_c):
                print("interop: urpc streaming server did not come up")
                failures += 1
            else:
                rc = subprocess.call(
                    [sys.executable,
                     os.path.join(HERE, "peer_stream_client.py"),
                     "127.0.0.1:" + port_c])
                if rc != 0:
                    print("interop: streaming A (official->urpc) FAILED")
                    failures += 1
                else:
                    print("interop: streaming A (official->urpc) ok")
        finally:
            proc_c.terminate()
            proc_c.wait()

        # B-str) urpc streaming client -> official streaming server
        port_d = "51084"
        proc_d = subprocess.Popen(
            [sys.executable,
             os.path.join(HERE, "peer_stream_server.py"), port_d])
        try:
            if not wait_port_ok("127.0.0.1:" + port_d):
                print("interop: official streaming server did not come up")
                failures += 1
            else:
                rc = subprocess.call([stream_client_bin, "127.0.0.1:" + port_d])
                if rc != 0:
                    print("interop: streaming B (urpc->official) FAILED")
                    failures += 1
                else:
                    print("interop: streaming B (urpc->official) ok")
        finally:
            proc_d.terminate()
            proc_d.wait()

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
