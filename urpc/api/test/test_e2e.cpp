// End-to-end API tests (US1/US2/US3/US4 acceptance): real TCP loopback,
// typed unary calls, error semantics, deadlines, graceful shutdown.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <gtest/gtest.h>
#include <upb/mem/arena.h>

#include "echo.upb.h"
#include "urpc/client.h"
#include "urpc/server.h"
#include "urpc/unary.h"
#include "urpc/core/log.h"

namespace {

using urpc::Channel;
using urpc::Result;
using urpc::Server;
using urpc::ServerContext;
using urpc::Status;
using urpc::StatusCode;
using urpc::UnaryDone;

URPC_UNARY_METHOD(EchoMethod, example, example__, EchoRequest, EchoResponse)

std::string TextOf(const example_EchoRequest* m) {
  upb_StringView v = example_EchoRequest_text(m);
  return std::string(v.data, v.size);
}
// upb string setters store a NON-OWNING view — source bytes must live in
// the message's arena (or otherwise outlive the encode).
void SetText(example_EchoRequest* m, upb_Arena* arena, const std::string& s) {
  char* p = static_cast<char*>(upb_Arena_Malloc(arena, s.size()));
  memcpy(p, s.data(), s.size());
  example_EchoRequest_set_text(
      m, upb_StringView_FromDataAndSize(p, s.size()));
}

class E2eTest : public ::testing::Test {
 protected:
  void StartServer() {
    urpc::Server::Options opts;
    opts.listen_address = "127.0.0.1:0";  // ephemeral
    Status err;
    server_ = Server::BuildAndStart(opts, &err);
    ASSERT_TRUE(server_ != nullptr) << err.ToString();
    ASSERT_TRUE(
        server_->RegisterUnaryFor<EchoMethod>(
            "example.EchoService", "Echo",
            [](ServerContext&, const example_EchoRequest* req,
               UnaryDone<example_EchoResponse> done) {
              upb_Arena* a = upb_Arena_New();
              auto* resp = example_EchoResponse_new(a);
              example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
              done(Status::Ok(), resp);
              upb_Arena_Free(a);
            })
            .ok());
    port_ = ExtractPort();
    ASSERT_GT(port_, 0);
  }

  static uint16_t ExtractPort() {
    // Server::BuildAndStart binds an ephemeral port; recover it by parsing
    // the effective address is not exposed — use a fixed test port instead.
    return 0;
  }

  // NOTE: with port 0 the port must be discovered; simplest reliable test
  // strategy: fixed high port per test binary run.
  void StartServerOn(uint16_t port) {
    urpc::Server::Options opts;
    opts.listen_address = "127.0.0.1:" + std::to_string(port);
    Status err;
    server_ = Server::BuildAndStart(opts, &err);
    ASSERT_TRUE(server_ != nullptr) << err.ToString();
    port_ = port;
  }

  std::shared_ptr<Channel> Connect() {
    return Channel::Connect("127.0.0.1:" + std::to_string(port_));
  }

  std::shared_ptr<Server> server_;
  uint16_t port_ = 0;
};

TEST(Echo, SyncCallRoundTrip) {
  const uint16_t port = 51001;
  Server::Options opts;
  opts.listen_address = "127.0.0.1:" + std::to_string(port);
  Status err;
  auto server = Server::BuildAndStart(opts, &err);
  ASSERT_TRUE(server != nullptr) << err.ToString();
  ASSERT_TRUE(
      server->RegisterUnaryFor<EchoMethod>(
          "example.EchoService", "Echo",
          [](ServerContext&, const example_EchoRequest* req,
             UnaryDone<example_EchoResponse> done) {
            upb_Arena* a = upb_Arena_New();
            auto* resp = example_EchoResponse_new(a);
            example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
            done(Status::Ok(), resp);
            upb_Arena_Free(a);
          })
          .ok());

  auto channel = Channel::Connect("127.0.0.1:" + std::to_string(port));
  upb_Arena* a = upb_Arena_New();
  auto* req = example_EchoRequest_new(a);
  SetText(req, a, "hello, urpc");
  auto r = channel->Call<EchoMethod>("example.EchoService", "Echo", req, 3000);
  upb_Arena_Free(a);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  upb_StringView got = example_EchoResponse_text(r.value());
  EXPECT_EQ(std::string(got.data, got.size), "hello, urpc");

  // empty message round-trip (spec edge case)
  upb_Arena* a2 = upb_Arena_New();
  auto* req2 = example_EchoRequest_new(a2);
  SetText(req2, a2, "");
  auto r2 = channel->Call<EchoMethod>("example.EchoService", "Echo", req2, 3000);
  upb_Arena_Free(a2);
  ASSERT_TRUE(r2.ok());
  got = example_EchoResponse_text(r2.value());
  EXPECT_EQ(std::string(got.data, got.size), "");

  server->Shutdown();
}

TEST(Echo, UnknownMethodIsUnimplemented) {
  const uint16_t port = 51002;
  Server::Options opts;
  opts.listen_address = "127.0.0.1:" + std::to_string(port);
  auto server = Server::BuildAndStart(opts, nullptr);
  ASSERT_TRUE(server != nullptr);

  auto channel = Channel::Connect("127.0.0.1:" + std::to_string(port));
  upb_Arena* a = upb_Arena_New();
  auto* req = example_EchoRequest_new(a);
  SetText(req, a, "x");
  auto r = channel->Call<EchoMethod>("example.EchoService", "Nope", req, 3000);
  upb_Arena_Free(a);
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), StatusCode::kUnimplemented);
  server->Shutdown();
}

TEST(Echo, AsyncCallsPairingAndParallelism) {
  const uint16_t port = 51003;
  Server::Options opts;
  opts.listen_address = "127.0.0.1:" + std::to_string(port);
  auto server = Server::BuildAndStart(opts, nullptr);
  ASSERT_TRUE(server != nullptr);
  ASSERT_TRUE(
      server->RegisterUnaryFor<EchoMethod>(
          "example.EchoService", "Echo",
          [](ServerContext&, const example_EchoRequest* req,
             UnaryDone<example_EchoResponse> done) {
            upb_Arena* a = upb_Arena_New();
            auto* resp = example_EchoResponse_new(a);
            example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
            done(Status::Ok(), resp);
            upb_Arena_Free(a);
          })
          .ok());

  auto channel = Channel::Connect("127.0.0.1:" + std::to_string(port));
  constexpr int kCalls = 100;  // SC-005
  std::atomic<int> completed{0};
  std::mutex mu;
  std::condition_variable cv;

  for (int i = 0; i < kCalls; i++) {
    upb_Arena* a = upb_Arena_New();
    auto* req = example_EchoRequest_new(a);
    const std::string text = "msg-" + std::to_string(i);
    SetText(req, a, text);
    const std::string expect = text;
    channel->CallAsync<EchoMethod>(
        "example.EchoService", "Echo", req, 5000,
        [i, expect, &completed, &mu, &cv](Result<example_EchoResponse> r) {
          EXPECT_TRUE(r.ok());
          upb_StringView got = example_EchoResponse_text(r.value());
          EXPECT_EQ(std::string(got.data, got.size), expect);
          completed.fetch_add(1);
          cv.notify_one();
        });
    upb_Arena_Free(a);
  }
  std::unique_lock<std::mutex> lock(mu);
  cv.wait_for(lock, std::chrono::seconds(10),
              [&] { return completed.load() == kCalls; });
  EXPECT_EQ(completed.load(), kCalls);
  server->Shutdown();
}

TEST(Echo, DeadlineExceededWithServerCancelNotification) {
  const uint16_t port = 51004;
  Server::Options opts;
  opts.listen_address = "127.0.0.1:" + std::to_string(port);
  auto server = Server::BuildAndStart(opts, nullptr);
  ASSERT_TRUE(server != nullptr);

  std::atomic<bool> server_cancelled{false};
  std::atomic<bool> handler_returned_early{false};
  ASSERT_TRUE(
      server->RegisterUnaryFor<EchoMethod>(
          "example.EchoService", "SlowEcho",
          [&](ServerContext& ctx, const example_EchoRequest*,
              UnaryDone<example_EchoResponse> done) {
            ctx.OnCancel([&] {
              server_cancelled.store(true);
              handler_returned_early.store(true);
            });
            // long processing with periodic cancel checks
            for (int i = 0; i < 100 && !ctx.IsCancelled(); i++) {
              std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            if (ctx.IsCancelled()) return;  // never respond
            done(Status::Ok(), nullptr);
          })
          .ok());

  auto channel = Channel::Connect("127.0.0.1:" + std::to_string(port));
  upb_Arena* a = upb_Arena_New();
  auto* req = example_EchoRequest_new(a);
  SetText(req, a, "slow");
  auto r = channel->Call<EchoMethod>("example.EchoService", "SlowEcho", req,
                                     300);  // client deadline: 300ms
  upb_Arena_Free(a);
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), StatusCode::kDeadlineExceeded);  // FR-004
  // server handler observes cancellation (Clarifications #1)
  for (int i = 0; i < 100 && !server_cancelled.load(); i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_TRUE(server_cancelled.load());
  server->Shutdown();
}

TEST(Echo, GracefulShutdownDrainsInFlight) {
  const uint16_t port = 51005;
  Server::Options opts;
  opts.listen_address = "127.0.0.1:" + std::to_string(port);
  opts.shutdown_grace_ms = 5000;  // enough for the slow handler
  auto server = Server::BuildAndStart(opts, nullptr);
  ASSERT_TRUE(server != nullptr);

  std::atomic<bool> slow_done{false};
  ASSERT_TRUE(
      server->RegisterUnaryFor<EchoMethod>(
          "example.EchoService", "SlowEcho",
          [&](ServerContext&, const example_EchoRequest*,
              UnaryDone<example_EchoResponse> done) {
            std::thread([done]() {
              std::this_thread::sleep_for(std::chrono::milliseconds(500));
              done(Status::Ok(), nullptr);
            }).detach();
          })
          .ok());

  auto channel = Channel::Connect("127.0.0.1:" + std::to_string(port));
  upb_Arena* a = upb_Arena_New();
  auto* req = example_EchoRequest_new(a);
  SetText(req, a, "drain");
  std::atomic<bool> got_response{false};
  Status got_status(StatusCode::kInternal, "unset");
  channel->CallAsync<EchoMethod>(
      "example.EchoService", "SlowEcho", req, 10000,
      [&](Result<example_EchoResponse> r) {
        got_status = r.status();
        got_response.store(true);
      });
  upb_Arena_Free(a);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto shutdown_thread = std::thread([&] { server->Shutdown(); });
  shutdown_thread.join();
  // in-flight call completed within the grace window (FR-012)
  for (int i = 0; i < 200 && !got_response.load(); i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  EXPECT_TRUE(got_response.load());
  EXPECT_TRUE(got_status.ok());  // completed NORMALLY (not UNAVAILABLE)
  server->Wait();
}

TEST(Echo, SyncCallOnLoopThreadFastFails) {
  const uint16_t port = 51006;
  Server::Options opts;
  opts.listen_address = "127.0.0.1:" + std::to_string(port);
  auto server = Server::BuildAndStart(opts, nullptr);
  ASSERT_TRUE(server != nullptr);
  ASSERT_TRUE(
      server->RegisterUnaryFor<EchoMethod>(
          "example.EchoService", "Echo",
          [](ServerContext&, const example_EchoRequest* req,
             UnaryDone<example_EchoResponse> done) {
            upb_Arena* a = upb_Arena_New();
            auto* resp = example_EchoResponse_new(a);
            example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
            done(Status::Ok(), resp);
            upb_Arena_Free(a);
          })
          .ok());

  // FR-002 / Clarifications #5: async done-callbacks run on the channel's
  // event-loop thread; a synchronous Call issued from there must fast-fail
  // instead of deadlocking.
  auto channel = Channel::Connect("127.0.0.1:" + std::to_string(port));
  std::atomic<bool> fast_failed{false};
  std::atomic<bool> second_done{false};

  upb_Arena* a = upb_Arena_New();
  auto* req = example_EchoRequest_new(a);
  SetText(req, a, "first");
  channel->CallAsync<EchoMethod>(
      "example.EchoService", "Echo", req, 5000,
      [&](Result<example_EchoResponse> r) {
        EXPECT_TRUE(r.ok());
        // we are on the channel's loop thread now
        upb_Arena* a2 = upb_Arena_New();
        auto* req2 = example_EchoRequest_new(a2);
        SetText(req2, a2, "second");
        auto r2 =
            channel->Call<EchoMethod>("example.EchoService", "Echo", req2, 1000);
        upb_Arena_Free(a2);
        fast_failed.store(!r2.ok() &&
                          r2.status().code() == StatusCode::kInternal);
        second_done.store(true);
      });
  upb_Arena_Free(a);

  for (int i = 0; i < 300 && !second_done.load(); i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(second_done.load());
  EXPECT_TRUE(fast_failed.load());
  server->Shutdown();
}

TEST(Echo, TwoThreadsSameChannelPairing) {
  // SC-005 / US6: two threads drive ONE channel concurrently; every call
  // must pair with its own response.
  const uint16_t port = 51008;
  Server::Options opts;
  opts.listen_address = "127.0.0.1:" + std::to_string(port);
  auto server = Server::BuildAndStart(opts, nullptr);
  ASSERT_TRUE(server != nullptr);
  ASSERT_TRUE(
      server->RegisterUnaryFor<EchoMethod>(
          "example.EchoService", "Echo",
          [](ServerContext&, const example_EchoRequest* req,
             UnaryDone<example_EchoResponse> done) {
            upb_Arena* a = upb_Arena_New();
            auto* resp = example_EchoResponse_new(a);
            example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
            done(Status::Ok(), resp);
            upb_Arena_Free(a);
          })
          .ok());

  auto channel = Channel::Connect("127.0.0.1:" + std::to_string(port));
  constexpr int kPerThread = 50;
  std::atomic<int> failures{0};
  auto worker = [&](int id) {
    for (int i = 0; i < kPerThread; i++) {
      const std::string text =
          "t" + std::to_string(id) + "-" + std::to_string(i);
      upb_Arena* a = upb_Arena_New();
      auto* req = example_EchoRequest_new(a);
      SetText(req, a, text);
      auto r = channel->Call<EchoMethod>("example.EchoService", "Echo", req,
                                         10000);
      upb_Arena_Free(a);
      if (!r.ok()) {
        failures.fetch_add(1);
        continue;
      }
      upb_StringView got = example_EchoResponse_text(r.value());
      if (std::string(got.data, got.size) != text) failures.fetch_add(1);
    }
  };
  std::thread t1(worker, 1);
  std::thread t2(worker, 2);
  t1.join();
  t2.join();
  EXPECT_EQ(failures.load(), 0);
  server->Shutdown();
}

TEST(Echo, DynamicRegistrationWithoutRestart) {
  const uint16_t port = 51007;
  Server::Options opts;
  opts.listen_address = "127.0.0.1:" + std::to_string(port);
  auto server = Server::BuildAndStart(opts, nullptr);
  ASSERT_TRUE(server != nullptr);

  auto channel = Channel::Connect("127.0.0.1:" + std::to_string(port));
  upb_Arena* a = upb_Arena_New();
  auto* req = example_EchoRequest_new(a);
  SetText(req, a, "dyn");
  auto before = channel->Call<EchoMethod>("example.EchoService", "Late", req,
                                          2000);
  upb_Arena_Free(a);
  EXPECT_EQ(before.status().code(), StatusCode::kUnimplemented);

  ASSERT_TRUE(
      server->RegisterUnaryFor<EchoMethod>(
          "example.EchoService", "Late",
          [](ServerContext&, const example_EchoRequest* req,
             UnaryDone<example_EchoResponse> done) {
            upb_Arena* ar = upb_Arena_New();
            auto* resp = example_EchoResponse_new(ar);
            example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
            done(Status::Ok(), resp);
            upb_Arena_Free(ar);
          })
          .ok());

  a = upb_Arena_New();
  auto* req2 = example_EchoRequest_new(a);
  SetText(req2, a, "dyn");
  auto after = channel->Call<EchoMethod>("example.EchoService", "Late", req2,
                                         2000);
  upb_Arena_Free(a);
  EXPECT_TRUE(after.ok());
  server->Shutdown();
}

}  // namespace
