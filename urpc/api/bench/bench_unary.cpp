// Unary round-trip / throughput benchmarks (US7 / T042).
// In-process server + client over real TCP loopback, typed api layer.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <benchmark/benchmark.h>
#include <upb/mem/arena.h>

#include "echo.upb.h"
#include "urpc/client.h"
#include "urpc/server.h"
#include "urpc/unary.h"

namespace {

using urpc::Channel;
using urpc::Result;
using urpc::Server;
using urpc::ServerContext;
using urpc::Status;
using urpc::UnaryDone;

URPC_UNARY_METHOD(EchoMethod, example, example__, EchoRequest, EchoResponse)

constexpr uint16_t kBenchPort = 51900;
constexpr size_t kPayloadBytes = 1024;  // 1 KiB, per research.md #1

std::shared_ptr<Server> g_server;
std::shared_ptr<Channel> g_channel;
std::string g_payload;

void SetupOnce() {
  if (g_server != nullptr) return;
  g_payload.assign(kPayloadBytes, 'x');

  Server::Options opts;
  opts.listen_address = "127.0.0.1:" + std::to_string(kBenchPort);
  opts.shutdown_grace_ms = 500;
  g_server = Server::BuildAndStart(opts, nullptr);
  if (g_server == nullptr) {
    std::fprintf(stderr, "bench: server failed to start\\n");
    std::exit(2);
  }
  g_server->RegisterUnaryFor<EchoMethod>(
      "example.EchoService", "Echo",
      [](ServerContext&, const example_EchoRequest* req,
         UnaryDone<example_EchoResponse> done) {
        upb_Arena* a = upb_Arena_New();
        auto* resp = example_EchoResponse_new(a);
        example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
        done(Status::Ok(), resp);
        upb_Arena_Free(a);
      });
  g_channel = Channel::Connect("127.0.0.1:" + std::to_string(kBenchPort));
}

example_EchoRequest* MakeRequest(upb_Arena* a) {
  auto* req = example_EchoRequest_new(a);
  char* p = static_cast<char*>(upb_Arena_Malloc(a, g_payload.size()));
  memcpy(p, g_payload.data(), g_payload.size());
  example_EchoRequest_set_text(
      req, upb_StringView_FromDataAndSize(p, g_payload.size()));
  return req;
}

void SerialRTT(benchmark::State& state) {
  SetupOnce();
  for (auto _ : state) {
    upb_Arena* a = upb_Arena_New();
    auto r = g_channel->Call<EchoMethod>("example.EchoService", "Echo",
                                         MakeRequest(a), 5000);
    upb_Arena_Free(a);
    if (!r.ok()) {
      state.SkipWithError(r.status().ToString().c_str());
      return;
    }
    benchmark::DoNotOptimize(r.value());
  }
}
BENCHMARK(SerialRTT);

void FourInFlightThroughput(benchmark::State& state) {
  SetupOnce();
  std::atomic<int> completed{0};
  std::atomic<int> failed{0};
  std::mutex mu;
  std::condition_variable cv;
  const int batch = 4;
  for (auto _ : state) {
    completed.store(0);
    failed.store(0);
    for (int i = 0; i < batch; i++) {
      upb_Arena* a = upb_Arena_New();
      g_channel->CallAsync<EchoMethod>(
          "example.EchoService", "Echo", MakeRequest(a), 5000,
          [&](Result<example_EchoResponse> r) {
            // NOTE: benchmark state is NOT thread-safe — defer error
            // reporting to the benchmark thread below.
            if (!r.ok()) failed.fetch_add(1);
            completed.fetch_add(1);
            cv.notify_one();
          });
      upb_Arena_Free(a);
    }
    std::unique_lock<std::mutex> lock(mu);
    cv.wait_for(lock, std::chrono::seconds(2),
                [&] { return completed.load() == batch; });
    if (completed.load() != batch) {
      state.SkipWithError("batch stalled (concurrency bug?)");
      return;
    }
    if (failed.load() > 0) {
      state.SkipWithError("async call failures");
      return;
    }
    state.SetItemsProcessed(batch);
  }
}
BENCHMARK(FourInFlightThroughput);

}  // namespace

BENCHMARK_MAIN();
