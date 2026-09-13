#!/usr/bin/env python3
"""Official gRPC Python streaming client (spec 004 US5, quadrant A).

Drives the three streaming forms against the urpc streaming example
server:
  Range(5)  -> values 0..4 in order
  Sum(1..10) -> total 55, count 10
  Chat(5)   -> every message echoed with matching seq
"""

import sys

import grpc

import streaming_pb2
import streaming_pb2_grpc


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:51082"
    channel = grpc.insecure_channel(target)
    stub = streaming_pb2_grpc.StreamServiceStub(channel)

    # --- server streaming: Range(5) -> 0..4 in order ---
    got = [v.value for v in stub.Range(
        streaming_pb2.RangeRequest(count=5), timeout=5)]
    if got != [0, 1, 2, 3, 4]:
        sys.stderr.write("peer_stream_client: range mismatch: %r\n" % got)
        return 1
    sys.stdout.write("peer_stream_client: range ok\n")

    # --- server streaming: zero responses is a normal completion ---
    got = [v.value for v in stub.Range(
        streaming_pb2.RangeRequest(count=0), timeout=5)]
    if got != []:
        sys.stderr.write("peer_stream_client: empty range mismatch: %r\n" % got)
        return 1
    sys.stdout.write("peer_stream_client: range-empty ok\n")

    # --- client streaming: Sum(1..10) -> total 55, count 10 ---
    def gen():
        for i in range(1, 11):
            yield streaming_pb2.AddRequest(value=i)

    total = stub.Sum(gen(), timeout=5)
    if total.total != 55 or total.count != 10:
        sys.stderr.write("peer_stream_client: sum mismatch: %r\n" % total)
        return 1
    sys.stdout.write("peer_stream_client: sum ok\n")

    # --- client streaming: empty stream -> total 0, count 0 ---
    total = stub.Sum(iter([]), timeout=5)
    if total.total != 0 or total.count != 0:
        sys.stderr.write("peer_stream_client: empty sum mismatch: %r\n" % total)
        return 1
    sys.stdout.write("peer_stream_client: sum-empty ok\n")

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
