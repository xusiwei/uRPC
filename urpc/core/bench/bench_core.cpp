// Core-layer benchmarks: untyped bytes straight through the kernel
// (core::Server + core::Channel, no upb encode/decode on the hot path).
// Complements the typed urpc/api/bench suite by isolating the transport
// + dispatch cost from the message-layer overhead.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <benchmark/benchmark.h>

#include "urpc/core/channel.h"
#include "urpc/core/codec.h"
#include "urpc/core/loop.h"
#include "urpc/core/router.h"
#include "urpc/core/server.h"

namespace {

using namespace urpc::core;

constexpr char kAddr[] = "127.0.0.1:23090";
constexpr int kMessagesPerStream = 1000;
constexpr size_t kPayloadBytes = 1024;

std::string Framed(const std::string& payload) {
  std::string out;
  EncodeFrame(payload, &out);
  return out;
}

// Client-side sink: collects arriving messages (order-preserving) and
// exposes the terminal status via a future; WaitMessage() supports
// ping-pong style synchronization from the benchmark thread.
struct Sink : std::enable_shared_from_this<Sink> {
  std::mutex mu;
  std::condition_variable cv;
  std::deque<std::string> messages;
  std::promise<Status> done;
  std::future<Status> future = done.get_future();

  Channel::StreamEvents events() {
    auto self = shared_from_this();
    Channel::StreamEvents ev;
    ev.on_message = [self](Status st, std::string msg) {
      if (!st.ok()) return;
      {
        std::lock_guard<std::mutex> lock(self->mu);
        self->messages.push_back(std::move(msg));
      }
      self->cv.notify_all();
    };
    ev.on_complete = [self](Status st) { self->done.set_value(st); };
    return ev;
  }

  bool WaitMessage(std::string* out, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu);
    if (!cv.wait_for(lock, timeout, [&] { return !messages.empty(); })) {
      return false;
    }
    *out = std::move(messages.front());
    messages.pop_front();
    return true;
  }

  size_t Count() {
    std::lock_guard<std::mutex> lock(mu);
    return messages.size();
  }
};

// Shared in-memory server + loopback channel, constructed once.
class CoreBenchEnv {
 public:
  CoreBenchEnv() {
    if (!router_
                 .RegisterUnary("/bench.Core/Echo",
                                [](ServerCallCtx& ctx,
                                   const std::string& request) {
                                  ctx.Respond(Status::Ok(), request);
                                })
                 .ok()) {
      return;
    }
    // server streaming: request = decimal message count; each response
    // is kPayloadBytes, chained on delivery callbacks (flow-correct).
    (void)router_.RegisterStream(
        "/bench.Core/Download", MethodForm::kServerStreaming,
        [](StreamCallCtx& call) {
          call.ReadMessage([&call](Status st, bool eos, std::string msg) {
            if (!st.ok() || eos) return;
            const int n = std::atoi(msg.c_str());
            auto arm =
                std::make_shared<std::function<void(Status, int)>>();
            *arm = [&call, n, arm](Status ok, int i) mutable {
              if (!ok.ok()) return;
              if (i >= n) {
                call.Finish(Status::Ok());
                return;
              }
              call.WriteMessage(std::string(kPayloadBytes, 'x'),
                                [arm, i](Status sent) mutable {
                                  (*arm)(sent, i + 1);
                                });
            };
            (*arm)(Status::Ok(), 0);
          });
        });
    // bidi: echo every message with a prefix
    (void)router_.RegisterStream(
        "/bench.Core/Chat", MethodForm::kBidi,
        [](StreamCallCtx& call) {
          auto arm = std::make_shared<
              std::function<void(Status, bool, std::string)>>();
          *arm = [&call, arm](Status st, bool eos, std::string msg) mutable {
            if (!st.ok()) return;
            if (eos) {
              call.Finish(Status::Ok());
              return;
            }
            call.WriteMessage("echo:" + msg, [](Status) {});
            call.ReadMessage(*arm);
          };
          call.ReadMessage(*arm);
        });

    if (!server_loop_.Start() || !client_loop_.Start()) return;
    server_.emplace(&server_loop_, &router_,
                    Server::Options{kAddr, 1u << 20, 5000});
    if (!server_->Start().ok()) {
      server_.reset();
      return;
    }
    client_.emplace(&client_loop_, Channel::Options{kAddr, 1u << 20});
    ok_ = true;
  }

  ~CoreBenchEnv() {
    if (server_) {
      server_->Shutdown();
      server_.reset();
    }
    client_loop_.Stop();
    client_.reset();
    server_loop_.Stop();
  }

  bool ok() const { return ok_; }
  Channel& client() { return *client_; }

 private:
  Router router_;
  LoopRunner server_loop_;
  LoopRunner client_loop_;
  std::optional<Server> server_;
  std::optional<Channel> client_;
  bool ok_ = false;
};

CoreBenchEnv& Env() {
  static CoreBenchEnv env;
  return env;
}

// Unary echo round-trip through the full kernel: loop -> HTTP/2 -> dispatch.
void BM_CoreUnaryEchoRoundTrip(benchmark::State& state) {
  CoreBenchEnv& env = Env();
  if (!env.ok()) {
    state.SkipWithError("env init failed");
    return;
  }
  const std::string request(64, 'a');
  for (auto _ : state) {
    std::promise<Status> done;
    auto fut = done.get_future();
    env.client().Call("/bench.Core/Echo", Framed(request), 5000,
                      [&done](Status st, std::string) {
                        done.set_value(st);
                      });
    Status st = fut.get();
    if (!st.ok()) {
      std::fprintf(stderr, "unary bench: call failed: %s\n",
                   st.message().c_str());
      state.SkipWithError("call failed: " + st.message());
      return;
    }
    state.SetItemsProcessed(state.iterations() + 1);
  }
}
BENCHMARK(BM_CoreUnaryEchoRoundTrip)->UseRealTime();

// Server-streaming throughput: one stream, kMessagesPerStream responses
// of kPayloadBytes each, produced flow-correctly (delivery-chained).
void BM_CoreServerStreamingThroughput(benchmark::State& state) {
  CoreBenchEnv& env = Env();
  if (!env.ok()) {
    state.SkipWithError("env init failed");
    return;
  }
  for (auto _ : state) {
    auto sink = std::make_shared<Sink>();
    const uint64_t id = env.client().OpenStream(
        "/bench.Core/Download", sink->events(), 30000);
    if (id == 0) {
      std::fprintf(stderr, "streaming bench: open stream failed\n");
      state.SkipWithError("open stream failed");
      return;
    }
    env.client().StreamSend(id, Framed(std::to_string(kMessagesPerStream)),
                            [](Status) {}, true);
    if (sink->future.wait_for(std::chrono::seconds(30)) !=
        std::future_status::ready) {
      std::fprintf(stderr, "streaming bench: stream incomplete\n");
      state.SkipWithError("stream incomplete");
      return;
    }
    if (!sink->future.get().ok()) {
      std::fprintf(stderr, "streaming bench: stream failed\n");
      state.SkipWithError("stream failed");
      return;
    }
    const size_t got = sink->Count();
    if (got != static_cast<size_t>(kMessagesPerStream)) {
      std::fprintf(stderr, "streaming bench: got %zu messages\n", got);
      state.SkipWithError("message count mismatch");
      return;
    }
    state.SetItemsProcessed(state.iterations() * kMessagesPerStream +
                            static_cast<int64_t>(got));
    state.SetBytesProcessed(state.iterations() * kMessagesPerStream *
                                static_cast<int64_t>(kPayloadBytes) +
                            static_cast<int64_t>(got * kPayloadBytes));
  }
}
BENCHMARK(BM_CoreServerStreamingThroughput)->UseRealTime();

// Bidi round-trip latency: one benchmark iteration = one write + one echo.
void BM_CoreBidiRoundTrip(benchmark::State& state) {
  CoreBenchEnv& env = Env();
  if (!env.ok()) {
    state.SkipWithError("env init failed");
    return;
  }
  auto sink = std::make_shared<Sink>();
  const uint64_t id =
      env.client().OpenStream("/bench.Core/Chat", sink->events(), 30000);
  if (id == 0) {
    std::fprintf(stderr, "bidi bench: open stream failed\n");
    state.SkipWithError("open stream failed");
    return;
  }
  for (auto _ : state) {
    env.client().StreamSend(id, Framed("ping"), [](Status) {}, false);
    std::string got;
    if (!sink->WaitMessage(&got, std::chrono::seconds(5)) ||
        got != "echo:ping") {
      std::fprintf(stderr, "bidi bench: round trip failed\n");
      state.SkipWithError("round trip failed");
      break;
    }
    state.SetItemsProcessed(state.iterations() + 1);
  }
  env.client().StreamCloseSend(id);
  (void)sink->future.wait_for(std::chrono::seconds(5));
}
BENCHMARK(BM_CoreBidiRoundTrip)->UseRealTime();

}  // namespace
