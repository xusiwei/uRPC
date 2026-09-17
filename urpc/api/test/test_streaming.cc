// Typed streaming API end-to-end tests (spec 004, US1/US2/US3).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
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
using urpc::gen::example::DownloadMethod;
using urpc::gen::example::UploadMethod;

std::atomic<uint16_t> sport_cursor{22000};
uint16_t NextSport() { return sport_cursor.fetch_add(1); }

// Chunk payload convention (streaming.proto): every byte of chunk i is
// (i & 0xFF).
std::string PayloadOf(uint32_t idx, size_t n) {
  return std::string(n, static_cast<char>(idx & 0xFF));
}

// Builds a chunk with `payload` COPIED into the arena, so callers may
// pass temporaries safely.
example_Chunk* NewChunk(upb_Arena* a, const std::string& payload) {
  auto* c = example_Chunk_new(a);
  char* buf =
      static_cast<char*>(upb_Arena_Malloc(a, payload.size()));
  if (buf == nullptr) return nullptr;
  std::memcpy(buf, payload.data(), payload.size());
  example_Chunk_set_data(
      c, upb_StringView_FromDataAndSize(buf, payload.size()));
  return c;
}

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
                  ->RegisterServerStreamingFor<DownloadMethod>(
                      "example.StreamService", "Download",
                      [](ServerContext&, const example_DownloadRequest* req,
                         urpc::ServerWriter<example_Chunk>& writer) {
                        const uint64_t file_size =
                            example_DownloadRequest_file_size(req);
                        const uint32_t chunk_size =
                            example_DownloadRequest_chunk_size(req) == 0
                                ? 1
                                : example_DownloadRequest_chunk_size(req);
                        uint64_t sent = 0;
                        uint32_t idx = 0;
                        while (sent < file_size) {
                          const uint64_t take =
                              std::min<uint64_t>(chunk_size, file_size - sent);
                          upb_Arena* a = upb_Arena_New();
                          auto* c = NewChunk(
                              a, PayloadOf(idx, static_cast<size_t>(take)));
                          writer.Write(c);
                          upb_Arena_Free(a);
                          sent += take;
                          ++idx;
                        }
                      })
                  .ok());

  upb_Arena* ra = upb_Arena_New();
  auto* req = example_DownloadRequest_new(ra);
  example_DownloadRequest_set_file_size(req, 5);
  auto reader = urpc::OpenClientReader<DownloadMethod>(
      channel_, "example.StreamService", "Download", req, 5000);
  upb_Arena_Free(ra);
  std::vector<std::string> got;
  for (;;) {
    Result<example_Chunk> r = reader->Read();
    EXPECT_TRUE(r.status().ok());
    if (r.value() == nullptr) break;  // end of stream
    upb_StringView d = example_Chunk_data(r.value());
    got.push_back(std::string(d.data, d.size));
  }
  ASSERT_EQ(got.size(), 5u);
  for (uint32_t i = 0; i < 5; ++i) {
    EXPECT_EQ(got[i], PayloadOf(i, 1));
  }
  EXPECT_TRUE(reader->Finish().ok());
}

TEST_F(StreamingApiE2E, ServerStreamingExplicitZeroResponses) {
  ASSERT_TRUE(server_
                  ->RegisterServerStreamingFor<DownloadMethod>(
                      "example.StreamService", "Download",
                      [](ServerContext&, const example_DownloadRequest* req,
                         urpc::ServerWriter<example_Chunk>& writer) {
                        if (example_DownloadRequest_file_size(req) == 0) {
                          writer.Finish(Status::Ok());  // explicit empty
                          return;
                        }
                        const uint64_t file_size =
                            example_DownloadRequest_file_size(req);
                        uint64_t sent = 0;
                        uint32_t idx = 0;
                        while (sent < file_size) {
                          upb_Arena* a = upb_Arena_New();
                          auto* c = NewChunk(a, PayloadOf(idx, 1));
                          writer.Write(c);
                          upb_Arena_Free(a);
                          ++sent;
                          ++idx;
                        }
                      })
                  .ok());

  upb_Arena* ra = upb_Arena_New();
  auto* req = example_DownloadRequest_new(ra);
  example_DownloadRequest_set_file_size(req, 0);
  auto reader = urpc::OpenClientReader<DownloadMethod>(
      channel_, "example.StreamService", "Download", req, 5000);
  upb_Arena_Free(ra);
  EXPECT_TRUE(reader->Read().value() == nullptr);  // immediate eos
  EXPECT_TRUE(reader->Finish().ok());
}

// Large transfer through the bounded send queue: 200 chunks exceed the
// 64-deep queue (FR-008), so the handler must chain writes on their
// delivery callbacks; every byte must arrive complete and in order.
TEST_F(StreamingApiE2E, ServerStreamingLargeFileFlowControl) {
  constexpr uint64_t kChunkSize = 1024;
  constexpr uint64_t kChunks = 200;
  ASSERT_TRUE(server_
                  ->RegisterServerStreamingFor<DownloadMethod>(
                      "example.StreamService", "Download",
                      [kChunkSize](ServerContext&,
                         const example_DownloadRequest* req,
                         urpc::ServerWriter<example_Chunk>& writer) {
                        const uint64_t file_size =
                            example_DownloadRequest_file_size(req);
                        auto arm = std::make_shared<
                            std::function<void(uint64_t, uint32_t)>>();
                        *arm = [arm, writer, file_size,
                                kChunkSize](uint64_t sent,
                                            uint32_t idx) mutable {
                          if (sent >= file_size) return;
                          const uint64_t take = std::min<uint64_t>(
                              kChunkSize, file_size - sent);
                          upb_Arena* a = upb_Arena_New();
                          auto* c =
                              NewChunk(a, PayloadOf(idx,
                                                    static_cast<size_t>(take)));
                          writer.Write(
                              c, [arm, writer, next = sent + take,
                                  next_idx = idx + 1, a](Status st) mutable {
                                upb_Arena_Free(a);
                                if (!st.ok()) return;
                                (*arm)(next, next_idx);
                              });
                        };
                        (*arm)(0, 0);
                      })
                  .ok());

  upb_Arena* ra = upb_Arena_New();
  auto* req = example_DownloadRequest_new(ra);
  example_DownloadRequest_set_file_size(req, kChunks * kChunkSize);
  example_DownloadRequest_set_chunk_size(req, kChunkSize);
  auto reader = urpc::OpenClientReader<DownloadMethod>(
      channel_, "example.StreamService", "Download", req, 30000);
  upb_Arena_Free(ra);
  uint64_t got_bytes = 0;
  uint32_t idx = 0;
  for (;;) {
    Result<example_Chunk> r = reader->Read();
    EXPECT_TRUE(r.status().ok());
    if (r.value() == nullptr) break;
    upb_StringView d = example_Chunk_data(r.value());
    ASSERT_EQ(d.size, static_cast<size_t>(kChunkSize));
    EXPECT_EQ(std::string(d.data, d.size), PayloadOf(idx, kChunkSize));
    got_bytes += d.size;
    ++idx;
  }
  EXPECT_EQ(got_bytes, kChunks * kChunkSize);
  EXPECT_TRUE(reader->Finish().ok());
}

// ---- US2: client streaming ---------------------------------------------------

TEST_F(StreamingApiE2E, ClientStreamingUpload) {
  ASSERT_TRUE(server_
                  ->RegisterClientStreamingFor<UploadMethod>(
                      "example.StreamService", "Upload",
                      [](ServerContext&,
                         urpc::ServerReader<example_Chunk>& reader,
                         urpc::UnaryDone<example_UploadResponse> done) {
                        auto bytes = std::make_shared<uint64_t>(0);
                        auto count = std::make_shared<uint32_t>(0);
                        auto arm = std::make_shared<
                            std::function<void(Status, bool,
                                               const example_Chunk*)>>();
                        *arm = [arm, bytes, count, done, reader](
                                   Status st, bool eos,
                                   const example_Chunk* msg) mutable {
                          if (!st.ok()) {
                            done(st, nullptr);
                            return;
                          }
                          if (eos) {
                            upb_Arena* a = upb_Arena_New();
                            auto* out = example_UploadResponse_new(a);
                            example_UploadResponse_set_bytes_received(
                                out, *bytes);
                            example_UploadResponse_set_chunk_count(out,
                                                                   *count);
                            done(Status::Ok(), out);
                            upb_Arena_Free(a);
                            return;
                          }
                          *bytes += example_Chunk_data(msg).size;
                          (*count)++;
                          reader.ReadMessage(*arm);
                        };
                        reader.ReadMessage(*arm);
                      })
                  .ok());

  auto writer = urpc::OpenClientWriter<UploadMethod>(
      channel_, "example.StreamService", "Upload", 5000);
  for (uint32_t i = 1; i <= 10; ++i) {
    upb_Arena* a = upb_Arena_New();
    auto* c = NewChunk(a, PayloadOf(i, i));
    EXPECT_TRUE(writer->Write(c));
    upb_Arena_Free(a);
  }
  writer->WritesDone();
  Result<example_UploadResponse> r = writer->Finish();
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(example_UploadResponse_bytes_received(r.value()), 55u);
  EXPECT_EQ(example_UploadResponse_chunk_count(r.value()), 10u);
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
    EXPECT_TRUE(stream->Write(m));
    upb_Arena_Free(a);
  }
  stream->WritesDone();
  std::vector<std::string> got;
  for (;;) {
    Result<example_ChatMsg> r = stream->Read();
    EXPECT_TRUE(r.status().ok());
    if (r.value() == nullptr) break;
    upb_StringView sv = example_ChatMsg_text(r.value());
    got.push_back(std::string(sv.data, sv.size));
  }
  ASSERT_EQ(got.size(), 5u);
  for (uint32_t i = 0; i < 5; ++i) EXPECT_EQ(got[i], "m" + std::to_string(i));
  EXPECT_TRUE(stream->Finish().ok());
}

TEST_F(StreamingApiE2E, HandlerExceptionFinishesInternal) {
  ASSERT_TRUE(server_
                  ->RegisterServerStreamingFor<DownloadMethod>(
                      "example.StreamService", "Download",
                      [](ServerContext&, const example_DownloadRequest*,
                         urpc::ServerWriter<example_Chunk>&) {
                        throw std::runtime_error("boom");
                      })
                  .ok());

  upb_Arena* ra = upb_Arena_New();
  auto* req = example_DownloadRequest_new(ra);
  example_DownloadRequest_set_file_size(req, 3);
  auto reader = urpc::OpenClientReader<DownloadMethod>(
      channel_, "example.StreamService", "Download", req, 5000);
  upb_Arena_Free(ra);
  Result<example_Chunk> first = reader->Read();
  EXPECT_EQ(first.status().code(), StatusCode::kInternal);
}

// server stays alive after a failing stream: a subsequent call succeeds
TEST_F(StreamingApiE2E, ServerAliveAfterHandlerException) {
  EXPECT_TRUE(server_
                  ->RegisterServerStreamingFor<DownloadMethod>(
                      "example.StreamService", "Download",
                      [](ServerContext&, const example_DownloadRequest*,
                         urpc::ServerWriter<example_Chunk>&) {
                        throw std::runtime_error("boom");
                      })
                  .ok());
  EXPECT_TRUE(server_
                  ->RegisterClientStreamingFor<UploadMethod>(
                      "example.StreamService", "Upload",
                      [](ServerContext&,
                         urpc::ServerReader<example_Chunk>& reader,
                         urpc::UnaryDone<example_UploadResponse> done) {
                        auto arm = std::make_shared<
                            std::function<void(Status, bool,
                                               const example_Chunk*)>>();
                        *arm = [arm, done, &reader](Status st, bool eos,
                                                    const example_Chunk*) {
                          if (eos) {
                            upb_Arena* a = upb_Arena_New();
                            auto* out = example_UploadResponse_new(a);
                            example_UploadResponse_set_bytes_received(out, 1);
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
  auto* req = example_DownloadRequest_new(ra);
  example_DownloadRequest_set_file_size(req, 1);
  auto reader = urpc::OpenClientReader<DownloadMethod>(
      channel_, "example.StreamService", "Download", req, 5000);
  upb_Arena_Free(ra);
  EXPECT_EQ(reader->Read().status().code(), StatusCode::kInternal);

  auto writer = urpc::OpenClientWriter<UploadMethod>(
      channel_, "example.StreamService", "Upload", 5000);
  writer->WritesDone();
  Result<example_UploadResponse> r = writer->Finish();
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(example_UploadResponse_bytes_received(r.value()), 1u);
}

}  // namespace
