// Typed service interface end-to-end tests (spec 003-typed-service-interface).
//
// US1: interface-inheritance server + RegisterService (driven by the
//      LEGACY client entry points - independence from US2).
// US2: generated proxy client (driven by a LEGACY-registered server).
// US5: channel/client/proxy separation semantics.
// FR-013: handler exceptions contained; FR-004: UNIMPLEMENTED defaults.

#include <atomic>
#include <cstdio>
#include <chrono>
#include <fstream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <upb/mem/arena.h>

#include "echo.service.h"
#include "echo.upb.h"
#include "urpc/client.h"
#include "urpc/server.h"
#include "urpc/unary.h"

#include <gtest/gtest.h>

namespace {

using urpc::Channel;
using urpc::Client;
using urpc::Result;
using urpc::Server;
using urpc::ServerContext;
using urpc::Status;
using urpc::StatusCode;
using urpc::gen::example::EchoServiceProxy;
using urpc::gen::example::IEchoService;

// Legacy traits (FR-014 compatibility surface) - drives servers and
// clients without the generated proxy.
URPC_UNARY_METHOD(LegacyEcho, example, example__, EchoRequest, EchoResponse)

std::atomic<uint16_t> port_cursor{51500};
uint16_t NextPort() { return port_cursor.fetch_add(1); }

example_EchoRequest* MakeReq(upb_Arena* arena, const std::string& text) {
  auto* req = example_EchoRequest_new(arena);
  example_EchoRequest_set_text(
      req, upb_StringView_FromDataAndSize(text.data(), text.size()));
  return req;
}

std::string TextOf(const example_EchoResponse* resp) {
  auto sv = example_EchoResponse_text(resp);
  return std::string(sv.data, sv.size);
}

std::shared_ptr<Server> StartOnPort(uint16_t port) {
  Server::Options opts;
  opts.listen_address = "127.0.0.1:" + std::to_string(port);
  return Server::BuildAndStart(opts, nullptr);
}

std::shared_ptr<Channel> Dial(uint16_t port) {
  return Channel::Connect("127.0.0.1:" + std::to_string(port));
}

// ---- US1 fixtures -----------------------------------------------------------

// Full implementation: every method overridden.
class FullEchoServiceImpl : public IEchoService {
 public:
  void Echo(ServerContext&, const example_EchoRequest* req,
            ::urpc::UnaryDone<example_EchoResponse> done) override {
    upb_Arena* a = upb_Arena_New();
    auto* resp = example_EchoResponse_new(a);
    example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
    done(Status::Ok(), resp);
    upb_Arena_Free(a);
  }
  void SlowEcho(ServerContext&, const example_EchoRequest* req,
                ::urpc::UnaryDone<example_EchoResponse> done) override {
    upb_Arena* a = upb_Arena_New();
    auto* resp = example_EchoResponse_new(a);
    example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
    done(Status::Ok(), resp);
    upb_Arena_Free(a);
  }
};

// Partial implementation: SlowEcho keeps the generated UNIMPLEMENTED
// default (FR-004).
class PartialEchoServiceImpl : public IEchoService {
 public:
  void Echo(ServerContext&, const example_EchoRequest* req,
            ::urpc::UnaryDone<example_EchoResponse> done) override {
    upb_Arena* a = upb_Arena_New();
    auto* resp = example_EchoResponse_new(a);
    example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
    done(Status::Ok(), resp);
    upb_Arena_Free(a);
  }
};

// Handler that raises (FR-013).
class ThrowingEchoServiceImpl : public IEchoService {
 public:
  void Echo(ServerContext&, const example_EchoRequest*,
            ::urpc::UnaryDone<example_EchoResponse>) override {
    throw std::runtime_error("boom");
  }
  // Working method: proves the server still serves after the exception
  // (FR-013 containment is per call, not per connection).
  void SlowEcho(ServerContext&, const example_EchoRequest* req,
                ::urpc::UnaryDone<example_EchoResponse> done) override {
    upb_Arena* a = upb_Arena_New();
    auto* resp = example_EchoResponse_new(a);
    example_EchoResponse_set_text(resp, example_EchoRequest_text(req));
    done(Status::Ok(), resp);
    upb_Arena_Free(a);
  }
};

// Cancel-observing implementation (FR-005). The handler blocks the server
// loop; cancellation is delivered once the loop regains control.
class CancelAwareServiceImpl : public IEchoService {
 public:
  std::atomic<bool> cancelled{false};

  void SlowEcho(ServerContext& ctx, const example_EchoRequest*,
                ::urpc::UnaryDone<example_EchoResponse> done) override {
    ctx.OnCancel([this] { cancelled.store(true); });
    for (int i = 0; i < 100 && !ctx.IsCancelled(); i++)
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
};

// ---- US3: generated shape (compile-time + method table) ---------------------

TEST(TypedServiceCodegen, InterfaceMethodTableMirrorsProto) {
  static_assert(std::size(IEchoService::kMethods) == 2,
                "echo.proto service declares two methods");
  EXPECT_STREQ(IEchoService::kMethods[0].name, "Echo");
  EXPECT_STREQ(IEchoService::kMethods[0].path, "/example.EchoService/Echo");
  EXPECT_STREQ(IEchoService::kMethods[1].name, "SlowEcho");
  EXPECT_STREQ(IEchoService::kMethods[1].path,
               "/example.EchoService/SlowEcho");
}

// ---- US1: server side (legacy client drives the typed server) ---------------

TEST(TypedServiceE2E, InterfaceImplRoutesAllMethods) {
  const uint16_t port = NextPort();
  auto server = StartOnPort(port);
  ASSERT_TRUE(server != nullptr);
  FullEchoServiceImpl impl;
  EXPECT_TRUE(RegisterService(*server, impl).ok());

  auto channel = Dial(port);
  ASSERT_TRUE(channel != nullptr);
  upb_Arena* a = upb_Arena_New();

  auto r1 = channel->Call<LegacyEcho>("example.EchoService", "Echo",
                                      MakeReq(a, "ping"), 3000);
  EXPECT_TRUE(r1.ok());
  EXPECT_EQ(TextOf(r1.value()), "ping");

  auto r2 = channel->Call<LegacyEcho>("example.EchoService", "SlowEcho",
                                      MakeReq(a, "pong"), 3000);
  EXPECT_TRUE(r2.ok());
  EXPECT_EQ(TextOf(r2.value()), "pong");
  upb_Arena_Free(a);
  server->Shutdown();
}

TEST(TypedServiceE2E, UnoverriddenMethodAnswersUnimplemented) {
  const uint16_t port = NextPort();
  auto server = StartOnPort(port);
  ASSERT_TRUE(server != nullptr);
  PartialEchoServiceImpl impl;
  EXPECT_TRUE(RegisterService(*server, impl).ok());

  auto channel = Dial(port);
  upb_Arena* a = upb_Arena_New();

  auto ok = channel->Call<LegacyEcho>("example.EchoService", "Echo",
                                      MakeReq(a, "x"), 3000);
  EXPECT_TRUE(ok.ok());

  auto unimpl = channel->Call<LegacyEcho>("example.EchoService", "SlowEcho",
                                          MakeReq(a, "x"), 3000);
  EXPECT_FALSE(unimpl.ok());
  EXPECT_EQ(unimpl.status().code(), StatusCode::kUnimplemented);
  upb_Arena_Free(a);
  server->Shutdown();
}

TEST(TypedServiceE2E, DuplicateServiceRegistrationRejected) {
  const uint16_t port = NextPort();
  auto server = StartOnPort(port);
  ASSERT_TRUE(server != nullptr);
  FullEchoServiceImpl impl;
  EXPECT_TRUE(RegisterService(*server, impl).ok());
  EXPECT_FALSE(RegisterService(*server, impl).ok());  // FR-003
  server->Shutdown();
}

TEST(TypedServiceE2E, HandlerExceptionBecomesInternal) {
  const uint16_t port = NextPort();
  auto server = StartOnPort(port);
  ASSERT_TRUE(server != nullptr);
  ThrowingEchoServiceImpl impl;
  EXPECT_TRUE(RegisterService(*server, impl).ok());

  auto channel = Dial(port);
  upb_Arena* a = upb_Arena_New();
  auto r = channel->Call<LegacyEcho>("example.EchoService", "Echo",
                                     MakeReq(a, "x"), 3000);
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), StatusCode::kInternal);

  // Server stays alive: a subsequent call to the working method
  // succeeds (FR-013 containment is per call).
  auto r2 = channel->Call<LegacyEcho>("example.EchoService", "SlowEcho",
                                      MakeReq(a, "y"), 3000);
  EXPECT_TRUE(r2.ok());
  upb_Arena_Free(a);
  server->Shutdown();
}

TEST(TypedServiceE2E, CancelObservedThroughInterfaceContext) {
  const uint16_t port = NextPort();
  auto server = StartOnPort(port);
  ASSERT_TRUE(server != nullptr);
  CancelAwareServiceImpl impl;
  EXPECT_TRUE(RegisterService(*server, impl).ok());

  auto channel = Dial(port);
  upb_Arena* a = upb_Arena_New();
  auto r = channel->Call<LegacyEcho>("example.EchoService", "SlowEcho",
                                     MakeReq(a, "slow"), 300);
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), StatusCode::kDeadlineExceeded);

  // Cancellation delivery waits for the (loop-blocking) handler to yield;
  // use a generous window for slow or oversubscribed runners.
  for (int i = 0; i < 400 && !impl.cancelled.load(); i++)
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_TRUE(impl.cancelled.load());
  upb_Arena_Free(a);
  server->Shutdown();
}

// ---- US2: client side (proxy over a LEGACY-registered server) ---------------

class TypedProxyE2E : public ::testing::Test {
 protected:
  void SetUp() override {
    port_ = NextPort();
    Server::Options opts;
    opts.listen_address = "127.0.0.1:" + std::to_string(port_);
    server_ = Server::BuildAndStart(opts, nullptr);
    ASSERT_TRUE(server_ != nullptr);
    ASSERT_TRUE(server_
                    ->RegisterUnaryFor<LegacyEcho>(
                        "example.EchoService", "Echo",
                        [](ServerContext&, const example_EchoRequest* req,
                           ::urpc::UnaryDone<example_EchoResponse> done) {
                          upb_Arena* a = upb_Arena_New();
                          auto* resp = example_EchoResponse_new(a);
                          example_EchoResponse_set_text(
                              resp, example_EchoRequest_text(req));
                          done(Status::Ok(), resp);
                          upb_Arena_Free(a);
                        })
                    .ok());

    channel_ = Channel::Connect("127.0.0.1:" + std::to_string(port_));
    ASSERT_TRUE(channel_ != nullptr);
  }

  void TearDown() override { server_->Shutdown(); }

  uint16_t port_ = 0;
  std::shared_ptr<Server> server_;
  std::shared_ptr<Channel> channel_;
};

TEST_F(TypedProxyE2E, SyncCallThroughProxy) {
  EchoServiceProxy proxy(channel_);
  upb_Arena* a = upb_Arena_New();
  auto r = proxy.Echo(MakeReq(a, "hello"), 3000);
  upb_Arena_Free(a);
  EXPECT_TRUE(r.ok());
  ASSERT_NE(r.value(), nullptr);
  EXPECT_EQ(TextOf(r.value()), "hello");
}

TEST_F(TypedProxyE2E, AsyncCallThroughProxy) {
  EchoServiceProxy proxy(channel_);
  upb_Arena* a = upb_Arena_New();
  std::atomic<bool> done{false};
  proxy.EchoAsync(MakeReq(a, "async"), 3000,
                  [&done](Result<example_EchoResponse> r) {
                    EXPECT_TRUE(r.ok());
                    done.store(true);
                  });
  upb_Arena_Free(a);
  for (int i = 0; i < 300 && !done.load(); i++)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_TRUE(done.load());
}

TEST_F(TypedProxyE2E, ProxyFromClientControlPlane) {
  Client client(channel_);
  auto proxy = client.Proxy<EchoServiceProxy>();
  upb_Arena* a = upb_Arena_New();
  auto r = proxy.Echo(MakeReq(a, "ctl"), 3000);
  upb_Arena_Free(a);
  EXPECT_TRUE(r.ok());
}

TEST_F(TypedProxyE2E, ConcurrentProxyCalls) {
  EchoServiceProxy proxy(channel_);
  constexpr int kCalls = 8;
  std::atomic<int> completed{0};
  std::vector<std::thread> workers;
  for (int t = 0; t < kCalls; t++) {
    workers.emplace_back(
        [&proxy, &completed, t] {
          const std::string text = "t" + std::to_string(t);
          upb_Arena* a = upb_Arena_New();
          auto r = proxy.Echo(MakeReq(a, text), 5000);
          upb_Arena_Free(a);
          EXPECT_TRUE(r.ok());
          if (r.ok()) EXPECT_EQ(TextOf(r.value()), text);
          completed.fetch_add(1);
        });
  }
  for (auto& w : workers) w.join();
  EXPECT_EQ(completed.load(), kCalls);
}

TEST_F(TypedProxyE2E, ProxyDeadlineExceeded) {
  // Slow method registered only for this test (legacy surface).
  ASSERT_TRUE(server_
                  ->RegisterUnaryFor<LegacyEcho>(
                      "example.EchoService", "SlowEcho",
                      [](ServerContext&, const example_EchoRequest*,
                         ::urpc::UnaryDone<example_EchoResponse> done) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(500));
                        done(Status::Ok(), nullptr);
                      })
                  .ok());

  EchoServiceProxy proxy(channel_);
  upb_Arena* a = upb_Arena_New();
  auto r = proxy.SlowEcho(MakeReq(a, "slow"), 100);
  upb_Arena_Free(a);
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), StatusCode::kDeadlineExceeded);
}

// ---- US5: channel/client/proxy separation -----------------------------------

TEST(ProxySeparation, ChannelCloseFailsSubsequentProxyCalls) {
  const uint16_t port = NextPort();
  auto server = StartOnPort(port);
  ASSERT_TRUE(server != nullptr);
  FullEchoServiceImpl impl;
  ASSERT_TRUE(RegisterService(*server, impl).ok());

  auto channel = Dial(port);
  EchoServiceProxy proxy(channel);
  upb_Arena* a = upb_Arena_New();
  auto before = proxy.Echo(MakeReq(a, "x"), 3000);
  EXPECT_TRUE(before.ok());

  channel->Close();  // FR-009: subsequent calls fail immediately
  auto after = proxy.Echo(MakeReq(a, "x"), 3000);
  EXPECT_FALSE(after.ok());
  EXPECT_EQ(after.status().code(), StatusCode::kUnavailable);
  upb_Arena_Free(a);
  server->Shutdown();
}

TEST(ProxySeparation, DestroyedChannelFailsSubsequentProxyCalls) {
  const uint16_t port = NextPort();
  auto server = StartOnPort(port);
  ASSERT_TRUE(server != nullptr);
  FullEchoServiceImpl impl;
  ASSERT_TRUE(RegisterService(*server, impl).ok());

  EchoServiceProxy proxy = [&] {
    auto channel = Dial(port);
    return EchoServiceProxy(channel);
  }();  // channel destroyed here; the proxy survives on a weak reference

  upb_Arena* a = upb_Arena_New();
  auto r = proxy.Echo(MakeReq(a, "x"), 3000);
  upb_Arena_Free(a);
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), StatusCode::kUnavailable);
  server->Shutdown();
}

// US5-2 (compile-time): the proxy exposes no connection management.
template <typename T, typename = void>
struct HasConnect : std::false_type {};
template <typename T>
struct HasConnect<T, std::void_t<decltype(std::declval<T&>().Connect())>>
    : std::true_type {};
template <typename T, typename = void>
struct HasClose : std::false_type {};
template <typename T>
struct HasClose<T, std::void_t<decltype(std::declval<T&>().Close())>>
    : std::true_type {};

TEST(ProxySeparation, ProxySurfaceIsBusinessOnly) {
  static_assert(!HasConnect<EchoServiceProxy>::value,
                "proxy must not expose Connect (FR-009/US5-2)");
  static_assert(!HasClose<EchoServiceProxy>::value,
                "proxy must not expose Close (FR-009/US5-2)");
  SUCCEED();
}

// ---- US5: client control-plane options apply --------------------------------

TEST(ProxySeparation, ClientControlPlaneOptionsApply) {
  const uint16_t port = NextPort();
  auto server = StartOnPort(port);
  ASSERT_TRUE(server != nullptr);
  FullEchoServiceImpl impl;
  ASSERT_TRUE(RegisterService(*server, impl).ok());
  std::fprintf(stderr, "[ccpoa] registered
");

  auto channel = Dial(port);
  Client::Options small;
  small.max_receive_message_size = 1024;
  Client client(channel, small);
  std::fprintf(stderr, "[ccpoa] client ready
");

  EchoServiceProxy proxy(client);
  upb_Arena* a = upb_Arena_New();
  // 4KiB response exceeds the 1KiB client receive limit -> call fails.
  std::fprintf(stderr, "[ccpoa] big call
");
  auto r = proxy.Echo(MakeReq(a, std::string(4096, 'x')), 3000);
  upb_Arena_Free(a);
  std::fprintf(stderr, "[ccpoa] big call returned ok=%d code=%d
",
               (int)r.ok(), (int)r.status().code());
  EXPECT_FALSE(r.ok());

  // Small payloads still work under the same options.
  std::fprintf(stderr, "[ccpoa] small call
");
  auto ok = proxy.Echo(MakeReq(a, "tiny"), 3000);
  std::fprintf(stderr, "[ccpoa] small call returned ok=%d
", (int)ok.ok());
  EXPECT_TRUE(ok.ok());
  std::fprintf(stderr, "[ccpoa] shutdown
");
  server->Shutdown();
  std::fprintf(stderr, "[ccpoa] done
");
}

}  // namespace
