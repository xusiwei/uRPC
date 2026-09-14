#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>

#include "urpc/core/loop.h"
#include "urpc/core/platform.h"

namespace {

using urpc::core::LoopRunner;
using urpc::core::platform::TcpListener;
using urpc::core::StatusCode;

TEST(ParseAddress, IPv4WithPort) {
  sockaddr_storage ss;
  auto st = urpc::core::platform::ParseAddress("127.0.0.1:50051", &ss);
  ASSERT_TRUE(st.ok());
  const auto* sin = reinterpret_cast<const sockaddr_in*>(&ss);
  EXPECT_EQ(sin->sin_family, AF_INET);
  EXPECT_EQ(ntohs(sin->sin_port), 50051);
}

TEST(ParseAddress, IPv6Bracketed) {
  sockaddr_storage ss;
  ASSERT_TRUE(
      urpc::core::platform::ParseAddress("[::1]:1234", &ss).ok());
  const auto* sin6 = reinterpret_cast<const sockaddr_in6*>(&ss);
  EXPECT_EQ(sin6->sin6_family, AF_INET6);
  EXPECT_EQ(ntohs(sin6->sin6_port), 1234);
}

TEST(ParseAddress, TcpSchemePrefixAccepted) {
  sockaddr_storage ss;
  ASSERT_TRUE(
      urpc::core::platform::ParseAddress("tcp://10.0.0.1:1", &ss).ok());
}

TEST(ParseAddress, MalformedRejected) {
  sockaddr_storage ss;
  EXPECT_FALSE(urpc::core::platform::ParseAddress("", &ss).ok());
  EXPECT_FALSE(urpc::core::platform::ParseAddress("nohost", &ss).ok());
  EXPECT_FALSE(urpc::core::platform::ParseAddress("host:notaport", &ss).ok());
  EXPECT_FALSE(urpc::core::platform::ParseAddress("host:0", &ss).ok());
  EXPECT_FALSE(urpc::core::platform::ParseAddress("host:70000", &ss).ok());
  EXPECT_FALSE(
      urpc::core::platform::ParseAddress("999.999.999.999:80", &ss).ok());
}

// Loopback connect + accept + bidirectional byte exchange.
TEST(TcpIntegration, ConnectAcceptAndEcho) {
  LoopRunner runner;
  ASSERT_TRUE(runner.Start());

  TcpListener listener;
  std::atomic<int> accepted{0};
  std::atomic<bool> bound{false};
  runner.Post([&] {
    bound = listener
                .BindAndListen(runner.loop(), "127.0.0.1:0",
                               [&](uv_stream_t*) { accepted.fetch_add(1); })
                .ok();
  });
  for (int i = 0; i < 500 && !bound.load(); i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ASSERT_TRUE(bound.load());
  const uint16_t port = listener.bound_port();
  ASSERT_GT(port, 0);

  std::atomic<bool> connected{false};
  urpc::core::Status got(StatusCode::kInternal, "unset");
  std::mutex mu;
  std::condition_variable cv;
  bool done = false;

  runner.Post([&] {
    urpc::core::platform::TcpConnect(
        runner.loop(), "127.0.0.1:" + std::to_string(port),
        [&](urpc::core::Status st, uv_stream_t* stream) {
          got = st;
          connected = st.ok();
          if (stream != nullptr) {
            uv_close(reinterpret_cast<uv_handle_t*>(stream),
                     [](uv_handle_t* h) { delete h; });
          }
          {
            std::lock_guard<std::mutex> lock(mu);
            done = true;
          }
          cv.notify_one();
        });
  });

  std::unique_lock<std::mutex> lock(mu);
  cv.wait_for(lock, std::chrono::seconds(3), [&] { return done; });
  // give the accept callback a moment to fire as well
  for (int i = 0; i < 100 && accepted.load() == 0; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  EXPECT_TRUE(connected.load());
  EXPECT_TRUE(got.ok());
  EXPECT_EQ(accepted.load(), 1);

  runner.Post([&] { listener.Close(); });
  runner.Stop();
}

TEST(TcpIntegration, ConnectToClosedPortFails) {
  LoopRunner runner;
  ASSERT_TRUE(runner.Start());
  // Bind then close to get a very likely-free port.
  TcpListener holder;
  std::atomic<bool> bound{false};
  runner.Post([&] {
    bound = holder
                .BindAndListen(runner.loop(), "127.0.0.1:0",
                               [](uv_stream_t*) {})
                .ok();
  });
  for (int i = 0; i < 500 && !bound.load(); i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ASSERT_TRUE(bound.load());
  const uint16_t port = holder.bound_port();
  runner.Post([&] { holder.Close(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  std::mutex mu;
  std::condition_variable cv;
  bool done = false;
  urpc::core::Status got = urpc::core::Status::Ok();
  runner.Post([&] {
    urpc::core::platform::TcpConnect(
        runner.loop(), "127.0.0.1:" + std::to_string(port),
        [&](urpc::core::Status st, uv_stream_t* s) {
          got = st;
          if (s != nullptr) {
            uv_close(reinterpret_cast<uv_handle_t*>(s),
                     [](uv_handle_t* h) { delete h; });
          }
          {
            std::lock_guard<std::mutex> lock(mu);
            done = true;
          }
          cv.notify_one();
        });
  });
  std::unique_lock<std::mutex> lock(mu);
  cv.wait_for(lock, std::chrono::seconds(3), [&] { return done; });
  EXPECT_FALSE(got.ok());
  EXPECT_EQ(got.code(), StatusCode::kUnavailable);
  runner.Stop();
}

}  // namespace
