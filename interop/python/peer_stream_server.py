#!/usr/bin/env python3
"""Official gRPC Python peer streaming SERVER (spec 004 US5, quadrant B).

Implements example.StreamService {Download, Upload, Chat} with plain
grpcio; urpc clients (urpc_streaming_client) call it to prove wire
compatibility for all three streaming forms in the urpc->official
direction.
"""

import sys
from concurrent import futures

import grpc

import streaming_pb2
import streaming_pb2_grpc

MAX_FILE_SIZE = 64 * 1024 * 1024
DEFAULT_CHUNK_SIZE = 64 * 1024
MAX_CHUNK_SIZE = 1024 * 1024


def fill_pattern(idx, size):
    """Chunk payload convention (streaming.proto): every byte of chunk
    idx is (idx & 0xFF)."""
    return bytes([idx & 0xFF]) * size


class StreamService(streaming_pb2_grpc.StreamServiceServicer):
    def Download(self, request, context):
        file_size = min(request.file_size, MAX_FILE_SIZE)
        chunk_size = request.chunk_size or DEFAULT_CHUNK_SIZE
        chunk_size = min(chunk_size, MAX_CHUNK_SIZE)
        sent = 0
        idx = 0
        while sent < file_size:
            take = min(chunk_size, file_size - sent)
            yield streaming_pb2.Chunk(data=fill_pattern(idx, take))
            sent += take
            idx += 1
            if (request.status_every > 0
                    and idx % request.status_every == 0
                    and sent < file_size):
                context.abort(grpc.StatusCode.INTERNAL,
                              "download aborted (status_every)")

    def Upload(self, request_iterator, context):
        bytes_received = 0
        count = 0
        for chunk in request_iterator:
            bytes_received += len(chunk.data)
            count += 1
        return streaming_pb2.UploadResponse(bytes_received=bytes_received,
                                            chunk_count=count)

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
