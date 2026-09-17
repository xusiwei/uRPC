#!/usr/bin/env python3
"""Official gRPC Python peer SERVER (spec 002? no — spec 001 US5 / T036).

Implements example.EchoService.Echo with plain grpcio; urpc clients call it
to prove wire compatibility in the urpc→official direction.
"""

import sys
import time
from concurrent import futures

import grpc

# generated modules live next to this script (built by the CMake interop step)
sys.path.insert(0, __file__)

import echo_pb2
import echo_pb2_grpc


class EchoService(echo_pb2_grpc.EchoServiceServicer):
    def Echo(self, request, context):
        return echo_pb2.EchoResponse(text=request.text)

    def SlowEcho(self, request, context):
        time.sleep(0.2)
        return echo_pb2.EchoResponse(text=request.text)


def main():
    port = sys.argv[1] if len(sys.argv) > 1 else "51080"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    echo_pb2_grpc.add_EchoServiceServicer_to_server(EchoService(), server)
    server.add_insecure_port("127.0.0.1:" + port)
    server.start()
    sys.stderr.write("peer_server: listening on %s\\n" % port)
    sys.stderr.flush()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
