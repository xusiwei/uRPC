// Typed streaming API end-to-end tests (spec 004, US1/US2/US3).

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <upb/mem/arena.h>

#include "streaming.service.h"
#include "streaming.upb.h"
#include "urpc/client.h"
#include "urpc/server.h"
#include "urpc/stream.h"

#include <gtest/gtest.h>

namespace {

using urpc::Channel;
using urpc::Result;
using urpc::Server;
using urpc::ServerContext;
using urpc::Status;
using urpc::StatusCode;
using urpc::gen::example::ChatMethod;
using urpc::gen::example::RangeMethod;
using urpc::gen::example::SumMethod;

std::atomic<uint16_t> sport_cursor{52000};
uint16_t NextSport() { return sport_cursor.fetch_add(1); }

class StreamingApiE2E : public ::testing::Test {
 protected:
  void SetUp() override {
    port_ = NextSport();
    Server::Options opts;
    opts.listen_address = "127.0.0.1:" + std::to_string(port_);
    server_ = Server::BuildAndStart(opts, nullptr);
    ASSERT_TRUE(server_ != nullptr);
    channel_ = Channel::Connect("127.0.0.1:" + std::to_string(port_));
    ASSERT_TRUE(channel_ != nullptr);
  }
  void TearDown() override { server_->Shutdown(); }

  uint16_t port_ = 0;
  std::shared_ptr<Server> server_;
  std::shared_ptr<Channel> channel_;
};

// ---- US1: server streaming ---------------------------------------------------

TEST_F(StreamingApiE2E, ServerStreamingOrderedValues) {
  ASSERT_TRUE(server_
                  ->RegisterServerStreamingFor<RangeMethod>(
                      "example.StreamService", "Range",
                      [](ServerContext&, const example_RangeRequest* req,
                         urpc::ServerWriter<example_RangeValue>& writer) {
                        const uint32_t n =
                            example_RangeRequest_count(req) > 100
                                ? 100
                                : example_RangeRequest_count(req);
                        for (uint32_t i = 0; i < n; ++i) {
                          upb_Arena* a = upb_Arena_New();
                          auto* v = example_RangeValue_new(a);
                          example_RangeValue_set_value(v, i);
                          writer.Write(v);
                          upb_Arena_Free(a);
                        }
                      })
                  .ok());

  upb_Arena* ra = upb_Arena_New();
  auto* req = example_RangeRequest_new(ra);
  example_RangeRequest_set_count(req, 5);
  auto reader = urpc::OpenClientReader<RangeMethod>(
      channel_, "example.StreamService", "Range", req, 5000);
  upb_Arena_Free(ra);
  std::vector<uint32_t> got;
  for (;;) {
    Result<example_RangeValue> r = reader.Read();
    EXPECT_TRUE(r.status().ok());
    if (r.value() == nullptr) break;  // end of stream
    got.push_back(example_RangeValue_value(r.value()));
  }
  ASSERT_EQ(got.size(), 5u);
  for (uint32_t i = 0; i < 5; ++i) EXPECT_EQ(got[i], i);
  EXPECT_TRUE(reader.Finish().ok());
}

TEST_F(StreamingApiE2E, ServerStreamingExplicitZeroResponses) {
  ASSERT_TRUE(server_
                  ->RegisterServerStreamingFor<RangeMethod>(
                      "example.StreamService", "Range",
                      [](ServerContext&, const example_RangeRequest* req,
                         urpc::ServerWriter<example_RangeValue>& writer) {
                        if (example_RangeRequest_count(req) == 0) {
                          writer.Finish(Status::Ok());  // explicit empty
                          return;
                        }
                        const uint32_t n = example_RangeRequest_count(req);
                        for (uint32_t i = 0; i < n; ++i) {
                          upb_Arena* a = upb_Arena_New();
                          auto* v = example_RangeValue_new(a);
                          example_RangeValue_set_value(v, i);
                          writer.Write(v);
                          upb_Arena_Free(a);
                        }
                      })
                  .ok());

  upb_Arena* ra = upb_Arena_New();
  auto* req = example_RangeRequest_new(ra);
  example_RangeRequest_set_count(req, 0);
  auto reader = urpc::OpenClientReader<RangeMethod>(
      channel_, "example.StreamService", "Range", req, 5000);
  upb_Arena_Free(ra);
  EXPECT_TRUE(reader.Read().value() == nullptr);  // immediate eos
  EXPECT_TRUE(reader.Finish().ok());
}

// ---- US2: client streaming ---------------------------------------------------

TEST_F(StreamingApiE2E, ClientStreamingSum) {
  ASSERT_TRUE(server_
                  ->RegisterClientStreamingFor<SumMethod>(
                      "example.StreamService", "Sum",
                      [](ServerContext&,
                         urpc::ServerReader<example_AddRequest>& reader,
                         urpc::UnaryDone<example_TotalResponse> done) {
                        auto total = std::make_shared<int64_t>(0);
                        auto count = std::make_shared<uint32_t>(0);
                        auto arm = std::make_shared<
                            std::function<void(Status, bool,
                                               const example_AddRequest*)>>();
                        *arm = [arm, total, count, done, reader](
                                   Status st, bool eos,
                                   const example_AddRequest* msg) mutable {
                          if (!st.ok()) {
                            done(st, nullptr);
                            return;
                          }
                          if (eos) {
                            upb_Arena* a = upb_Arena_New();
                            auto* out = example_TotalResponse_new(a);
                            example_TotalResponse_set_total(out, *total);
                            example_TotalResponse_set_count(out, *count);
                            done(Status::Ok(), out);
                            upb_Arena_Free(a);
                            return;
                          }
                          *total += example_AddRequest_value(msg);
                          (*count)++;
                          reader.ReadMessage(*arm);
                        };
                        reader.ReadMessage(*arm);
                      })
                  .ok());

  auto writer = urpc::OpenClientWriter<SumMethod>(
      channel_, "example.StreamService", "Sum", 5000);
  for (int i = 1; i <= 10; ++i) {
    upb_Arena* a = upb_Arena_New();
    auto* v = example_AddRequest_new(a);
    example_AddRequest_set_value(v, i);
    EXPECT_TRUE(writer.Write(v));
    upb_Arena_Free(a);
  }
  writer.WritesDone();
  Result<example_TotalResponse> r = writer.Finish();
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(example_TotalResponse_total(r.value()), 55);
  EXPECT_EQ(example_TotalResponse_count(r.value()), 10);
}

// ---- US3: bidi ----------------------------------------------------------------

TEST_F(StreamingApiE2E, BidiEchoInterleaved) {
  ASSERT_TRUE(server_
                  ->RegisterBidiFor<ChatMethod>(
                      "example.StreamService", "Chat",
                      [](ServerContext&,
                         urpc::ServerReaderWriter<example_ChatMsg,
                                                  example_ChatMsg>& stream) {
                        auto arm = std::make_shared<
                            std::function<void(Status, bool,
                                               const example_ChatMsg*)>>();
                        *arm = [arm, stream](Status st, bool eos,
                                             const example_ChatMsg* msg) mutable {
                          if (!st.ok()) return;
                          if (eos) {
                            stream.Finish(Status::Ok());
                            return;
                          }
                          upb_StringView sv = example_ChatMsg_text(msg);
                          std::fprintf(stderr, "[bidi-srv] text=%.*s\n",
                                       (int)sv.size,
                                       sv.size ? sv.data : "");
                          upb_Arena* a = upb_Arena_New();
                          auto* out = example_ChatMsg_new(a);
                          example_ChatMsg_set_text(
                              out, example_ChatMsg_text(msg));
                          example_ChatMsg_set_seq(out,
                                                  example_ChatMsg_seq(msg));
                          stream.Write(out);
                          upb_Arena_Free(a);
                          stream.ReadMessage(*arm);
                        };
                        stream.ReadMessage(*arm);
                      })
                  .ok());

  auto stream = urpc::OpenClientReaderWriter<ChatMethod>(
      channel_, "example.StreamService", "Chat", 5000);
  for (uint32_t i = 0; i < 5; ++i) {
    upb_Arena* a = upb_Arena_New();
    auto* m = example_ChatMsg_new(a);
    const std::string text = "m" + std::to_string(i);
    example_ChatMsg_set_text(
        m, upb_StringView_FromDataAndSize(text.data(), text.size()));
    example_ChatMsg_set_seq(m, i);
    EXPECT_TRUE(stream.Write(m));
    upb_Arena_Free(a);
  }
  stream.WritesDone();
  std::vector<std::string> got;
  for (;;) {
    Result<example_ChatMsg> r = stream.Read();
    EXPECT_TRUE(r.status().ok());
    if (r.value() == nullptr) break;
    upb_StringView sv = example_ChatMsg_text(r.value());
    got.push_back(std::string(sv.data, sv.size));
  }
  ASSERT_EQ(got.size(), 5u);
  for (uint32_t i = 0; i < 5; ++i) EXPECT_EQ(got[i], "m" + std::to_string(i));
  EXPECT_TRUE(stream.Finish().ok());
}

TEST_F(StreamingApiE2E, HandlerExceptionFinishesInternal) {
  ASSERT_TRUE(server_
                  ->RegisterServerStreamingFor<RangeMethod>(
                      "example.StreamService", "Range",
                      [](ServerContext&, const example_RangeRequest*,
                         urpc::ServerWriter<example_RangeValue>&) {
                        throw std::runtime_error("boom");
                      })
                  .ok());

  upb_Arena* ra = upb_Arena_New();
  auto* req = example_RangeRequest_new(ra);
  example_RangeRequest_set_count(req, 3);
  auto reader = urpc::OpenClientReader<RangeMethod>(
      channel_, "example.StreamService", "Range", req, 5000);
  upb_Arena_Free(ra);
  Result<example_RangeValue> first = reader.Read();
  EXPECT_EQ(first.status().code(), StatusCode::kInternal);
}

// server stays alive after a failing stream: a subsequent call succeeds
TEST_F(StreamingApiE2E, ServerAliveAfterHandlerException) {
  EXPECT_TRUE(server_
                  ->RegisterServerStreamingFor<RangeMethod>(
                      "example.StreamService", "Range",
                      [](ServerContext&, const example_RangeRequest*,
                         urpc::ServerWriter<example_RangeValue>&) {
                        throw std::runtime_error("boom");
                      })
                  .ok());
  EXPECT_TRUE(server_
                  ->RegisterClientStreamingFor<SumMethod>(
                      "example.StreamService", "Sum",
                      [](ServerContext&,
                         urpc::ServerReader<example_AddRequest>& reader,
                         urpc::UnaryDone<example_TotalResponse> done) {
                        auto arm = std::make_shared<
                            std::function<void(Status, bool,
                                               const example_AddRequest*)>>();
                        *arm = [arm, done, &reader](Status st, bool eos,
                                                    const example_AddRequest*) {
                          if (eos) {
                            upb_Arena* a = upb_Arena_New();
                            auto* out = example_TotalResponse_new(a);
                            example_TotalResponse_set_total(out, 1);
                            done(Status::Ok(), out);
                            upb_Arena_Free(a);
                            return;
                          }
                          reader.ReadMessage(*arm);
                        };
                        reader.ReadMessage(*arm);
                      })
                  .ok());

  upb_Arena* ra = upb_Arena_New();
  auto* req = example_RangeRequest_new(ra);
  example_RangeRequest_set_count(req, 1);
  auto reader = urpc::OpenClientReader<RangeMethod>(
      channel_, "example.StreamService", "Range", req, 5000);
  upb_Arena_Free(ra);
  EXPECT_EQ(reader.Read().status().code(), StatusCode::kInternal);

  auto writer = urpc::OpenClientWriter<SumMethod>(
      channel_, "example.StreamService", "Sum", 5000);
  writer.WritesDone();
  Result<example_TotalResponse> r = writer.Finish();
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(example_TotalResponse_total(r.value()), 1);
}

}  // namespace
