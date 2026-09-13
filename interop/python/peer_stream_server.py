#!/usr/bin/env python3
"""Official gRPC Python peer streaming SERVER (spec 004 US5, quadrant B).

Implements example.StreamService {Range, Sum, Chat} with plain grpcio;
urpc clients (urpc_streaming_client) call it to prove wire compatibility
for all three streaming forms in the urpc->official direction.
"""

import sys
from concurrent import futures

import grpc

import streaming_pb2
import streaming_pb2_grpc


class StreamService(streaming_pb2_grpc.StreamServiceServicer):
    def Range(self, request, context):
        for i in range(request.count):
            yield streaming_pb2.RangeValue(value=i)

    def Sum(self, request_iterator, context):
        total = 0
        count = 0
        for req in request_iterator:
            total += req.value
            count += 1
        return streaming_pb2.TotalResponse(total=total, count=count)

    def Chat(self, request_iterator, context):
        for msg in request_iterator:
            yield streaming_pb2.ChatMsg(text=msg.text, seq=msg.seq)


def main():
    port = sys.argv[1] if len(sys.argv) > 1 else "51083"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    streaming_pb2_grpc.add_StreamServiceServicer_to_server(StreamService(),
                                                           server)
    server.add_insecure_port("127.0.0.1:" + port)
    server.start()
    sys.stderr.write("peer_stream_server: listening on %s\n" % port)
    sys.stderr.flush()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
