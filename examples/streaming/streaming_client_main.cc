// urpc_streaming_client - example streaming client (spec 004 US1-US3).
// Exercises Download (server streaming, large-file), Upload (client
// streaming, large-file) and Chat (bidi) against the example server;
// prints per-form results.

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include <upb/mem/arena.h>

#include "streaming.upb.h"
#include "streaming.service.h"
#include "urpc/client.h"
#include "urpc/stream.h"

namespace {

using urpc::Channel;
using urpc::Result;
using urpc::Status;
using urpc::gen::example::ChatMethod;
using urpc::gen::example::DownloadMethod;
using urpc::gen::example::UploadMethod;

constexpr const char* kService = "example.StreamService";

int RunDownload(std::shared_ptr<Channel>& ch, uint64_t file_size,
                uint32_t chunk_size) {
  upb_Arena* a = upb_Arena_New();
  auto* req = example_DownloadRequest_new(a);
  example_DownloadRequest_set_file_size(req, file_size);
  example_DownloadRequest_set_chunk_size(req, chunk_size);
  auto reader = urpc::OpenClientReader<DownloadMethod>(
      ch, kService, "Download", req, 30000);
  upb_Arena_Free(a);

  uint64_t got_bytes = 0;
  uint32_t chunks = 0;
  bool corrupt = false;
  for (;;) {
    Result<example_Chunk> r = reader->Read();
    if (!r.status().ok()) {
      std::fprintf(stderr, "Download failed: %s\n",
                   r.status().message().c_str());
      return 1;
    }
    if (r.value() == nullptr) break;
    upb_StringView d = example_Chunk_data(r.value());
    // Chunk payload convention (streaming.proto): every byte of chunk i
    // is (i & 0xFF).
    const char expected = static_cast<char>(chunks & 0xFF);
    for (size_t i = 0; i < d.size && !corrupt; ++i) {
      corrupt = d.data[i] != expected;
    }
    got_bytes += d.size;
    ++chunks;
  }
  Status fin = reader->Finish();
  if (!fin.ok() || corrupt || got_bytes != file_size) {
    std::fprintf(stderr,
                 "Download mismatch: %llu bytes in %u chunks%s, status %s\n",
                 (unsigned long long)got_bytes, chunks,
                 corrupt ? " (pattern corrupt)" : "", fin.message().c_str());
    return 1;
  }
  std::printf("[Download] %llu bytes in %u chunks verified\n",
              (unsigned long long)got_bytes, chunks);
  return 0;
}

int RunUpload(std::shared_ptr<Channel>& ch, uint64_t file_size,
              uint32_t chunk_size) {
  auto writer = urpc::OpenClientWriter<UploadMethod>(ch, kService, "Upload",
                                                     30000);
  uint64_t sent = 0;
  uint32_t idx = 0;
  while (sent < file_size) {
    const uint64_t take = std::min<uint64_t>(chunk_size, file_size - sent);
    upb_Arena* a = upb_Arena_New();
    auto* c = example_Chunk_new(a);
    auto payload = std::make_unique<char[]>(static_cast<size_t>(take));
    std::memset(payload.get(), static_cast<int>(idx & 0xFF),
                static_cast<size_t>(take));
    example_Chunk_set_data(
        c, upb_StringView_FromDataAndSize(payload.get(),
                                          static_cast<size_t>(take)));
    if (!writer->Write(c)) {
      std::fprintf(stderr, "Upload: write failed at chunk %u\n", idx);
      upb_Arena_Free(a);
      return 1;
    }
    upb_Arena_Free(a);
    sent += take;
    ++idx;
  }
  writer->WritesDone();
  Result<example_UploadResponse> r = writer->Finish();
  if (!r.ok() || example_UploadResponse_bytes_received(r.value()) != sent ||
      example_UploadResponse_chunk_count(r.value()) != idx) {
    std::fprintf(stderr, "Upload mismatch\n");
    return 1;
  }
  std::printf("[Upload]   %llu bytes in %u chunks accepted\n",
              (unsigned long long)sent, idx);
  return 0;
}

int RunChat(std::shared_ptr<Channel>& ch, uint32_t rounds) {
  auto stream = urpc::OpenClientReaderWriter<ChatMethod>(ch, kService, "Chat",
                                                         30000);
  for (uint32_t i = 0; i < rounds; ++i) {
    upb_Arena* a = upb_Arena_New();
    auto* m = example_ChatMsg_new(a);
    const std::string text = "m" + std::to_string(i);
    example_ChatMsg_set_text(
        m, upb_StringView_FromDataAndSize(text.data(), text.size()));
    example_ChatMsg_set_seq(m, i);
    if (!stream->Write(m)) {
      std::fprintf(stderr, "Chat: write failed at %u\n", i);
      upb_Arena_Free(a);
      return 1;
    }
    upb_Arena_Free(a);
    Result<example_ChatMsg> r = stream->Read();
    if (!r.status().ok() || r.value() == nullptr ||
        example_ChatMsg_seq(r.value()) != i) {
      std::fprintf(stderr, "Chat: bad echo at %u\n", i);
      return 1;
    }
  }
  stream->WritesDone();
  Status fin = stream->Finish();
  if (!fin.ok()) {
    std::fprintf(stderr, "Chat finish failed: %s\n", fin.message().c_str());
    return 1;
  }
  std::printf("[Chat]     %u rounds echoed in order\n", rounds);
  return 0;
}

}  // namespace

int main(int argc, char* argv[]) {
  const char* target = argc > 1 ? argv[1] : "127.0.0.1:50052";
  auto channel = Channel::Connect(target);

  int rc = 0;
  rc |= RunDownload(channel, 8ull << 20, 64u << 10);  // 8 MiB / 64 KiB
  rc |= RunUpload(channel, 4ull << 20, 64u << 10);    // 4 MiB / 64 KiB
  rc |= RunChat(channel, 5);
  if (rc == 0) std::printf("all streaming forms ok\n");
  return rc;
}
