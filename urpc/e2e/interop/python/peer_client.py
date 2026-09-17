#!/usr/bin/env python3
"""Official gRPC Python peer CLIENT (spec 001 US5 / T036).

Calls a urpc server to prove wire compatibility in the official→urpc
direction: success path, unknown-method error path and deadline path.
"""

import sys

import grpc

sys.path.insert(0, __file__)

import echo_pb2
import echo_pb2_grpc


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:51081"
    channel = grpc.insecure_channel(target)
    stub = echo_pb2_grpc.EchoServiceStub(channel)

    # 1) success: message semantics must round-trip
    resp = stub.Echo(echo_pb2.EchoRequest(text="hello from python"), timeout=5)
    if resp.text != "hello from python":
        sys.stderr.write("peer_client: unexpected reply: %r\\n" % resp.text)
        return 1
    sys.stdout.write("peer_client: echo ok\\n")

    # 2) unknown method → UNIMPLEMENTED
    try:
        missing = channel.unary_unary(
            "/example.EchoService/NoSuchMethod",
            request_serializer=lambda m: m.SerializeToString(),
            response_deserializer=echo_pb2.EchoResponse.FromString)
        missing(echo_pb2.EchoRequest(text="x"), timeout=5)
        sys.stderr.write("peer_client: expected UNIMPLEMENTED\\n")
        return 1
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.UNIMPLEMENTED:
            sys.stderr.write("peer_client: wrong code: %s\\n" % e.code())
            return 1
    sys.stdout.write("peer_client: unimplemented ok\\n")

    # 3) deadline → DEADLINE_EXCEEDED (urpc SlowEcho delays ~200ms)
    try:
        stub.SlowEcho(echo_pb2.EchoRequest(text="slow"), timeout=0.05)
        sys.stderr.write("peer_client: expected DEADLINE_EXCEEDED\\n")
        return 1
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.DEADLINE_EXCEEDED:
            sys.stderr.write("peer_client: wrong code: %s\\n" % e.code())
            return 1
    sys.stdout.write("peer_client: deadline ok\\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
