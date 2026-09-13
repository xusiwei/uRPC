// Streaming benchmarks (spec 004, US7 / T030): server-streaming
// throughput and bidi round-trip latency, wired into the Google
// Benchmark suite alongside bench_unary.

#include <atomic>
#include <chrono>
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
using urpc::ServerReader;
using urpc::ServerReaderWriter;
using urpc::ServerWriter;
using urpc::Status;
using urpc::gen::example::ChatMethod;
using urpc::gen::example::RangeMethod;

constexpr char kAddr[] = "127.0.0.1:52090";
constexpr int kValuesPerStream = 1000;

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
  server.RegisterServerStreamingFor<RangeMethod>(
      "example.StreamService", "Range",
      [](ServerContext&, const example_RangeRequest* req,
         ServerWriter<example_RangeValue>& writer) {
        const uint32_t n = example_RangeRequest_count(req);
        for (uint32_t i = 0; i < n; ++i) {
          upb_Arena* a = upb_Arena_New();
          auto* v = example_RangeValue_new(a);
          example_RangeValue_set_value(v, i);
          writer.Write(v);
          upb_Arena_Free(a);
        }
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

// Server-streaming throughput: one stream, kValuesPerStream 8-byte values.
void BM_ServerStreamingThroughput(benchmark::State& state) {
  RegisterHandlers(*Env().server_);
  for (auto _ : state) {
    upb_Arena* a = upb_Arena_New();
    auto* req = example_RangeRequest_new(a);
    example_RangeRequest_set_count(req, kValuesPerStream);
    auto reader = urpc::OpenClientReader<RangeMethod>(
        Env().channel_, "example.StreamService", "Range", req, 30000);
    upb_Arena_Free(a);
    int got = 0;
    for (;;) {
      Result<example_RangeValue> r = reader->Read();
      if (r.value() == nullptr) break;
      ++got;
    }
    auto fin = reader->Finish();
    if (!fin.ok() || got != kValuesPerStream) {
      state.SkipWithError("stream incomplete");
      return;
    }
    state.SetItemsProcessed(state.iterations() * kValuesPerStream + got);
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
