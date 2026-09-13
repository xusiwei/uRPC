// Core streaming tests (spec 004, T006): exercises the multi-message
// mechanism through a real core::Server + core::Channel pair (untyped
// bytes; typed wrappers are covered by the api-layer suite).

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "urpc/core/channel.h"
#include "urpc/core/codec.h"
#include "urpc/core/loop.h"
#include "urpc/core/router.h"
#include "urpc/core/server.h"

#include <gtest/gtest.h>

namespace {

using namespace urpc::core;

std::atomic<uint16_t> port_cursor{51600};
uint16_t NextPort() { return port_cursor.fetch_add(1); }

std::string Framed(const std::string& payload) {
  std::string out;
  EncodeFrame(payload, &out);
  return out;
}

// Collects on_message events in order; terminal status via promise.
struct Sink {
  std::mutex mu;
  std::deque<std::string> messages;
  std::promise<Status> done;
  std::future<Status> future = done.get_future();

  Channel::StreamEvents events() {
    Channel::StreamEvents ev;
    ev.on_message = [this](Status st, std::string msg) {
      if (!st.ok()) return;
      std::lock_guard<std::mutex> lock(mu);
      messages.push_back(std::move(msg));
    };
    ev.on_complete = [this](Status st) { done.set_value(st); };
    return ev;
  }

  std::vector<std::string> TakeMessages() {
    std::lock_guard<std::mutex> lock(mu);
    return {messages.begin(), messages.end()};
  }
};

class CoreStreaming : public ::testing::Test {
 protected:
  void StartServer(std::function<void(Router&)> register_tests) {
    port_ = NextPort();
    register_tests(router_);
    ASSERT_TRUE(server_loop_.Start());
    ASSERT_TRUE(client_loop_.Start());
    server_.emplace(&server_loop_, &router_,
                    Server::Options{"127.0.0.1:" + std::to_string(port_),
                                    1u << 20, 5000});
    ASSERT_TRUE(server_->Start().ok());
    client_.emplace(&client_loop_,
                    Channel::Options{"127.0.0.1:" + std::to_string(port_),
                                     1u << 20});
  }

  void TearDown() override {
    // order matters: drain the server while its loop runs, then destroy
    // the objects, then stop the loops (no loop-thread callbacks may
    // outlive the objects they reference)
    if (server_) server_->Shutdown();
    server_.reset();
    client_.reset();
    client_loop_.Stop();
    server_loop_.Stop();
  }

  Channel& channel() { return *client_; }

  uint16_t port_ = 0;
  LoopRunner server_loop_;
  LoopRunner client_loop_;
  Router router_;
  std::optional<Server> server_;
  std::optional<Channel> client_;
};

// ---- server streaming -------------------------------------------------------
TEST_F(CoreStreaming, ServerStreamingOrderedDelivery) {
  StartServer([](Router& r) {
    ASSERT_TRUE(
        r.RegisterStream(
             "/example.StreamService/Range", MethodForm::kServerStreaming,
             [](StreamCallCtx& call) {
               call.ReadMessage([&call](Status st, bool eos, std::string msg) {
                 if (!st.ok() || eos) return;
                 const int n = std::atoi(msg.c_str());
                 for (int i = 0; i < n; ++i) {
                   call.WriteMessage(std::to_string(i), [](Status) {});
                 }
                 call.Finish(Status::Ok());
               });
             })
            .ok());
  });

  Sink sink;
  const uint64_t id = channel().OpenStream("/example.StreamService/Range",
                                           sink.events(), 5000);
  ASSERT_NE(id, 0u);
  channel().StreamSend(id, Framed("5"), [](Status) {}, true);

  ASSERT_EQ(sink.future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_TRUE(sink.future.get().ok());
  const auto msgs = sink.TakeMessages();
  ASSERT_EQ(msgs.size(), 5u);
  for (int i = 0; i < 5; ++i) EXPECT_EQ(msgs[i], std::to_string(i));
}

// zero-response server stream is a normal completion
TEST_F(CoreStreaming, ServerStreamingZeroResponses) {
  StartServer([](Router& r) {
    ASSERT_TRUE(r.RegisterStream(
                    "/example.StreamService/Empty", MethodForm::kServerStreaming,
                    [](StreamCallCtx& call) {
                      call.ReadMessage([&call](Status st, bool eos,
                                               std::string) {
                        if (!st.ok() || eos) return;
                        call.Finish(Status::Ok());  // no responses
                      });
                    })
                    .ok());
  });

  Sink sink;
  const uint64_t id =
      channel().OpenStream("/example.StreamService/Empty", sink.events(), 5000);
  channel().StreamSend(id, Framed("x"), [](Status) {}, true);
  ASSERT_EQ(sink.future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_TRUE(sink.future.get().ok());
  EXPECT_TRUE(sink.TakeMessages().empty());
}

// ---- client streaming -------------------------------------------------------
TEST_F(CoreStreaming, ClientStreamingSum) {
  StartServer([](Router& r) {
    ASSERT_TRUE(r.RegisterStream(
                    "/example.StreamService/Sum", MethodForm::kClientStreaming,
                    [](StreamCallCtx& call) {
                      auto total = std::make_shared<int64_t>(0);
                      auto arm = std::make_shared<
                          std::function<void(Status, bool, std::string)>>();
                      *arm = [&call, total, arm](Status st, bool eos,
                                                 std::string msg) {
                        if (!st.ok()) return;
                        if (eos) {
                          call.WriteMessage(std::to_string(*total),
                                            [](Status) {});
                          call.Finish(Status::Ok());
                          return;
                        }
                        *total += std::atoll(msg.c_str());
                        call.ReadMessage(*arm);
                      };
                      call.ReadMessage(*arm);
                    })
                    .ok());
  });

  Sink sink;
  const uint64_t id =
      channel().OpenStream("/example.StreamService/Sum", sink.events(), 5000);
  for (int i = 1; i <= 10; ++i) {
    channel().StreamSend(id, Framed(std::to_string(i)), [](Status) {}, false);
  }
  channel().StreamCloseSend(id);
  ASSERT_EQ(sink.future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_TRUE(sink.future.get().ok());
  const auto msgs = sink.TakeMessages();
  ASSERT_EQ(msgs.size(), 1u);
  EXPECT_EQ(msgs[0], "55");
}

// ---- bidi --------------------------------------------------------------------
TEST_F(CoreStreaming, BidiEchoInterleaved) {
  StartServer([](Router& r) {
    ASSERT_TRUE(r.RegisterStream(
                    "/example.StreamService/Chat", MethodForm::kBidi,
                    [](StreamCallCtx& call) {
                      auto arm = std::make_shared<
                          std::function<void(Status, bool, std::string)>>();
                      *arm = [&call, arm](Status st, bool eos,
                                          std::string msg) {
                        if (!st.ok()) return;
                        if (eos) {
                          call.Finish(Status::Ok());
                          return;
                        }
                        call.WriteMessage("echo:" + msg, [](Status) {});
                        call.ReadMessage(*arm);
                      };
                      call.ReadMessage(*arm);
                    })
                    .ok());
  });

  Sink sink;
  const uint64_t id =
      channel().OpenStream("/example.StreamService/Chat", sink.events(), 5000);
  for (int i = 0; i < 5; ++i) {
    channel().StreamSend(id, Framed("m" + std::to_string(i)), [](Status) {},
                         false);
  }
  channel().StreamCloseSend(id);
  ASSERT_EQ(sink.future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_TRUE(sink.future.get().ok());
  const auto msgs = sink.TakeMessages();
  ASSERT_EQ(msgs.size(), 5u);
  for (int i = 0; i < 5; ++i) EXPECT_EQ(msgs[i], "echo:m" + std::to_string(i));
}

// ---- invariants ----------------------------------------------------------------
TEST_F(CoreStreaming, FinishIsOneShotAndPostFinishWritesFail) {
  StartServer([](Router& r) {
    ASSERT_TRUE(r.RegisterStream(
                    "/example.StreamService/Once", MethodForm::kBidi,
                    [](StreamCallCtx& call) {
                      call.Finish(Status::Ok());
                      call.Finish(Status::Ok());  // ignored
                      call.WriteMessage("late",
                                        [](Status st) {
                                          EXPECT_EQ(st.code(),
                                                    StatusCode::kInternal);
                                        });
                    })
                    .ok());
  });

  Sink sink;
  const uint64_t id =
      channel().OpenStream("/example.StreamService/Once", sink.events(), 5000);
  channel().StreamCloseSend(id);
  ASSERT_EQ(sink.future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_TRUE(sink.future.get().ok());
  EXPECT_TRUE(sink.TakeMessages().empty());
}

TEST_F(CoreStreaming, UnregisteredStreamMethodIsUnimplemented) {
  StartServer([](Router&) {});

  Sink sink;
  const uint64_t id = channel().OpenStream("/example.StreamService/Nope",
                                           sink.events(), 5000);
  channel().StreamSend(id, Framed("x"), [](Status) {}, true);
  ASSERT_EQ(sink.future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_EQ(sink.future.get().code(), StatusCode::kUnimplemented);
}

TEST_F(CoreStreaming, ClientTimeoutCoversWholeStream) {
  StartServer([](Router& r) {
    ASSERT_TRUE(r.RegisterStream(
                    "/example.StreamService/Hang", MethodForm::kBidi,
                    [](StreamCallCtx&) {
                      // never reads, never finishes
                    })
                    .ok());
  });

  Sink sink;
  const uint64_t id =
      channel().OpenStream("/example.StreamService/Hang", sink.events(), 300);
  channel().StreamSend(id, Framed("x"), [](Status) {}, false);
  ASSERT_EQ(sink.future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_EQ(sink.future.get().code(), StatusCode::kDeadlineExceeded);
}


// ---- backpressure: 10k chained writes arrive complete and in order -------
// (spec 004 T028 / FR-008 / SC-004). The producer chains writes on the
// delivery callbacks (flow-correct pattern); the send queue never grows
// beyond its bound.
TEST_F(CoreStreaming, ServerStreamingTenThousandChained) {
  StartServer([](Router& r) {
    ASSERT_TRUE(r.RegisterStream(
                    "/example.StreamService/Flood", MethodForm::kServerStreaming,
                    [](StreamCallCtx& call) {
                      call.ReadMessage([&call](Status st, bool eos,
                                               std::string msg) {
                        if (!st.ok() || eos) return;
                        const int n = std::atoi(msg.c_str());
                        auto arm = std::make_shared<
                            std::function<void(Status, int)>>();
                        // flow-correct producer: write the next message
                        // only after the previous one was delivered
                        *arm = [&call, n, arm](Status st, int i) mutable {
                          if (!st.ok()) return;
                          if (i >= n) {
                            call.Finish(Status::Ok());
                            return;
                          }
                          call.WriteMessage(std::to_string(i),
                                            [arm, i](Status ok) mutable {
                                              (*arm)(ok, i + 1);
                                            });
                        };
                        (*arm)(Status::Ok(), 0);
                      });
                    })
                    .ok());
  });

  Sink sink;
  const uint64_t id =
      channel().OpenStream("/example.StreamService/Flood", sink.events(), 30000);
  channel().StreamSend(id, Framed("10000"), [](Status) {}, true);
  ASSERT_EQ(sink.future.wait_for(std::chrono::seconds(30)),
            std::future_status::ready);
  EXPECT_TRUE(sink.future.get().ok());
  const auto msgs = sink.TakeMessages();
  ASSERT_EQ(msgs.size(), 10000u);
  for (int i = 0; i < 10000; ++i) {
    if (msgs[i] != std::to_string(i)) {
      ADD_FAILURE() << "order broken at " << i << ": " << msgs[i];
      break;
    }
  }
}

// ---- graceful shutdown with an in-flight stream (FR-016) -------------------
TEST_F(CoreStreaming, ShutdownDrainsInFlightStream) {
  StartServer([](Router& r) {
    ASSERT_TRUE(r.RegisterStream(
                    "/example.StreamService/Hang", MethodForm::kBidi,
                    [](StreamCallCtx&) {
                      // never finishes: the server must force-drain it
                    })
                    .ok());
  });

  Sink sink;
  const uint64_t id =
      channel().OpenStream("/example.StreamService/Hang", sink.events(), 0);
  channel().StreamSend(id, Framed("x"), [](Status) {}, true);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // Shutdown must complete (force-finish the in-flight stream) within the
  // configured grace period instead of hanging forever.
  server_->Shutdown();
  SUCCEED();
}

}  // namespace
