#!/usr/bin/env python3
"""Official gRPC Python streaming client (spec 004 US5, quadrant A).

Drives the three streaming forms against the urpc streaming example
server:
  Download(20 bytes, 4-byte chunks) -> 5 pattern chunks in order
  Download(0)                       -> normal empty completion
  Download(status_every=2)          -> INTERNAL mid-stream (error path)
  Upload(10 x 3-byte chunks)        -> receipt: 30 bytes / 10 chunks
  Upload(empty)                     -> receipt: 0 bytes / 0 chunks
  Chat(5)                           -> every message echoed with matching seq
"""

import sys

import grpc

import streaming_pb2
import streaming_pb2_grpc


def fill_pattern(idx, size):
    """Chunk payload convention (streaming.proto): every byte of chunk
    idx is (idx & 0xFF)."""
    return bytes([idx & 0xFF]) * size


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:51082"
    channel = grpc.insecure_channel(target)
    stub = streaming_pb2_grpc.StreamServiceStub(channel)

    # --- server streaming: Download 20 bytes in 4-byte chunks -----------
    got = b"".join(c.data for c in stub.Download(
        streaming_pb2.DownloadRequest(file_size=20, chunk_size=4),
        timeout=5))
    expect = b"".join(fill_pattern(i, 4) for i in range(5))
    if got != expect:
        sys.stderr.write("peer_stream_client: download mismatch: %r\n" % got)
        return 1
    sys.stdout.write("peer_stream_client: download ok\n")

    # --- server streaming: zero-byte file is a normal completion --------
    got = list(stub.Download(
        streaming_pb2.DownloadRequest(file_size=0), timeout=5))
    if got != []:
        sys.stderr.write(
            "peer_stream_client: empty download mismatch: %r\n" % got)
        return 1
    sys.stdout.write("peer_stream_client: download-empty ok\n")

    # --- server streaming: status_every aborts with INTERNAL ------------
    aborted = False
    try:
        list(stub.Download(streaming_pb2.DownloadRequest(
            file_size=8, chunk_size=2, status_every=2), timeout=5))
    except grpc.RpcError as e:
        aborted = e.code() == grpc.StatusCode.INTERNAL
    if not aborted:
        sys.stderr.write("peer_stream_client: download abort mismatch\n")
        return 1
    sys.stdout.write("peer_stream_client: download-abort ok\n")

    # --- client streaming: Upload 10 x 3-byte chunks ---------------------
    def gen():
        for i in range(10):
            yield streaming_pb2.Chunk(data=fill_pattern(i, 3))

    receipt = stub.Upload(gen(), timeout=5)
    if receipt.bytes_received != 30 or receipt.chunk_count != 10:
        sys.stderr.write("peer_stream_client: upload mismatch: %r\n" % receipt)
        return 1
    sys.stdout.write("peer_stream_client: upload ok\n")

    # --- client streaming: empty upload -> empty receipt -----------------
    receipt = stub.Upload(iter([]), timeout=5)
    if receipt.bytes_received != 0 or receipt.chunk_count != 0:
        sys.stderr.write(
            "peer_stream_client: empty upload mismatch: %r\n" % receipt)
        return 1
    sys.stdout.write("peer_stream_client: upload-empty ok\n")

    # --- bidi: 5 rounds, echoes must match in order ---
    def chat_gen():
        for i in range(5):
            yield streaming_pb2.ChatMsg(text="m%d" % i, seq=i)

    got = [(m.seq, m.text) for m in stub.Chat(chat_gen(), timeout=5)]
    if got != [(i, "m%d" % i) for i in range(5)]:
        sys.stderr.write("peer_stream_client: chat mismatch: %r\n" % got)
        return 1
    sys.stdout.write("peer_stream_client: chat ok\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
