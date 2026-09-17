// Streaming benchmarks (spec 004, US7 / T030): server-streaming
// throughput and bidi round-trip latency, wired into the Google
// Benchmark suite alongside bench_unary.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

#include <benchmark/benchmark.h>
#include <upb/mem/arena.h>

#include "streaming.service.h"
#include "streaming.upb.h"
#include "urpc/client.h"
#include "urpc/server.h"
#include "urpc/stream.h"

namespace {

using urpc::Channel;
using urpc::Result;
using urpc::Server;
using urpc::ServerContext;
using urpc::ServerReaderWriter;
using urpc::ServerWriter;
using urpc::Status;
using urpc::gen::example::ChatMethod;
using urpc::gen::example::DownloadMethod;

constexpr char kAddr[] = "127.0.0.1:22090";
constexpr int kChunksPerStream = 1000;
constexpr int kChunkBytes = 8;

class StreamBenchEnv {
 public:
  StreamBenchEnv() {
    Server::Options opts;
    opts.listen_address = kAddr;
    server_ = Server::BuildAndStart(opts, nullptr);
    channel_ = Channel::Connect(kAddr);
  }
  ~StreamBenchEnv() { server_->Shutdown(); }

  std::shared_ptr<Server> server_;
  std::shared_ptr<Channel> channel_{
      Channel::Connect(kAddr)};
};

StreamBenchEnv& Env() {
  static StreamBenchEnv env;
  return env;
}

void RegisterHandlers(Server& server) {
  static bool registered = false;
  if (registered) return;
  registered = true;
  // Flow-correct producer: chunks are chained on their delivery
  // callbacks so any file size stays within the bounded send queue.
  server.RegisterServerStreamingFor<DownloadMethod>(
      "example.StreamService", "Download",
      [](ServerContext&, const example_DownloadRequest* req,
         ServerWriter<example_Chunk>& writer) {
        const uint64_t file_size = example_DownloadRequest_file_size(req);
        auto arm =
            std::make_shared<std::function<void(uint64_t, uint32_t)>>();
        *arm = [arm, writer, file_size,
                chunk_bytes = kChunkBytes](uint64_t sent,
                                           uint32_t idx) mutable {
          if (sent >= file_size) return;
          const uint64_t take =
              std::min<uint64_t>(chunk_bytes, file_size - sent);
          upb_Arena* a = upb_Arena_New();
          auto* c = example_Chunk_new(a);
          auto payload =
              std::make_unique<char[]>(static_cast<size_t>(take));
          std::memset(payload.get(), static_cast<int>(idx & 0xFF),
                      static_cast<size_t>(take));
          example_Chunk_set_data(
              c, upb_StringView_FromDataAndSize(
                     payload.get(), static_cast<size_t>(take)));
          writer.Write(c, [arm, writer, next = sent + take,
                           next_idx = idx + 1](Status st) mutable {
            if (!st.ok()) return;  // dropped / stream ended
            (*arm)(next, next_idx);
          });
          upb_Arena_Free(a);
        };
        (*arm)(0, 0);
      });
  server.RegisterBidiFor<ChatMethod>(
      "example.StreamService", "Chat",
      [](ServerContext&,
         ServerReaderWriter<example_ChatMsg, example_ChatMsg>& stream) {
        auto arm = std::make_shared<
            std::function<void(Status, bool, const example_ChatMsg*)>>();
        *arm = [arm, stream](Status st, bool eos,
                             const example_ChatMsg* msg) mutable {
          if (!st.ok()) return;
          if (eos) {
            stream.Finish(Status::Ok());
            return;
          }
          upb_Arena* a = upb_Arena_New();
          auto* out = example_ChatMsg_new(a);
          example_ChatMsg_set_text(out, example_ChatMsg_text(msg));
          example_ChatMsg_set_seq(out, example_ChatMsg_seq(msg));
          stream.Write(out);
          upb_Arena_Free(a);
          stream.ReadMessage(*arm);
        };
        stream.ReadMessage(*arm);
      });
}

// Server-streaming throughput: one stream, kChunksPerStream 8-byte chunks.
void BM_ServerStreamingThroughput(benchmark::State& state) {
  RegisterHandlers(*Env().server_);
  const uint64_t file_size =
      static_cast<uint64_t>(kChunksPerStream) * kChunkBytes;
  for (auto _ : state) {
    upb_Arena* a = upb_Arena_New();
    auto* req = example_DownloadRequest_new(a);
    example_DownloadRequest_set_file_size(req, file_size);
    example_DownloadRequest_set_chunk_size(req, kChunkBytes);
    auto reader = urpc::OpenClientReader<DownloadMethod>(
        Env().channel_, "example.StreamService", "Download", req, 30000);
    upb_Arena_Free(a);
    int got = 0;
    for (;;) {
      Result<example_Chunk> r = reader->Read();
      if (r.value() == nullptr) break;
      ++got;
    }
    auto fin = reader->Finish();
    if (!fin.ok() || got != kChunksPerStream) {
      state.SkipWithError("stream incomplete");
      return;
    }
    state.SetItemsProcessed(state.iterations() * kChunksPerStream + got);
  }
}
BENCHMARK(BM_ServerStreamingThroughput);

// Bidi round-trip: each iteration = one write + one echo read.
void BM_BidiRoundTrip(benchmark::State& state) {
  RegisterHandlers(*Env().server_);
  auto stream = urpc::OpenClientReaderWriter<ChatMethod>(
      Env().channel_, "example.StreamService", "Chat", 30000);
  for (auto _ : state) {
    upb_Arena* a = upb_Arena_New();
    auto* m = example_ChatMsg_new(a);
    example_ChatMsg_set_seq(m, 1);
    if (!stream->Write(m)) {
      state.SkipWithError("write failed");
      upb_Arena_Free(a);
      return;
    }
    upb_Arena_Free(a);
    Result<example_ChatMsg> r = stream->Read();
    if (!r.status().ok() || r.value() == nullptr) {
      state.SkipWithError("read failed");
      return;
    }
  }
  stream->WritesDone();
  stream->Finish();
}
BENCHMARK(BM_BidiRoundTrip);

}  // namespace
