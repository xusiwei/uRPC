// urpc_streaming_server - example streaming server (spec 004 US1-US3).
// Registers example.StreamService/{Download,Upload,Chat} via the
// generated typed interface and serves until killed.

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

#include <upb/mem/arena.h>

#include "streaming.upb.h"
#include "streaming.service.h"
#include "urpc/server.h"
#include "urpc/stream.h"

namespace {

using urpc::Server;
using urpc::ServerContext;
using urpc::ServerWriter;
using urpc::ServerReader;
using urpc::ServerReaderWriter;
using urpc::Status;
using urpc::StatusCode;
using urpc::UnaryDone;

constexpr uint64_t kMaxFileSize = 64ull * 1024 * 1024;
constexpr uint32_t kDefaultChunkSize = 64 * 1024;
constexpr uint32_t kMaxChunkSize = 1024 * 1024;

// Chunk payload convention (streaming.proto): every byte of chunk i is
// (i & 0xFF). Returns a heap buffer of n bytes.
std::unique_ptr<char[]> FillPattern(uint32_t idx, size_t n) {
  auto buf = std::make_unique<char[]>(n);
  std::memset(buf.get(), static_cast<int>(idx & 0xFF), n);
  return buf;
}

class StreamServiceImpl : public urpc::gen::example::IStreamService {
 protected:
  // US1: one request -> stream of file chunks (large-file download).
  // Writes are chained on their delivery callbacks, so the bounded send
  // queue (FR-008) stays within its bound for any file size.
  void Download(ServerContext&, const example_DownloadRequest* req,
                ServerWriter<example_Chunk>& writer) override {
    uint64_t file_size = example_DownloadRequest_file_size(req);
    if (file_size > kMaxFileSize) file_size = kMaxFileSize;
    uint32_t chunk_size = example_DownloadRequest_chunk_size(req);
    if (chunk_size == 0) chunk_size = kDefaultChunkSize;
    if (chunk_size > kMaxChunkSize) chunk_size = kMaxChunkSize;
    const uint32_t status_every = example_DownloadRequest_status_every(req);

    auto arm =
        std::make_shared<std::function<void(uint64_t sent, uint32_t idx)>>();
    *arm = [arm, writer, file_size, chunk_size,
            status_every](uint64_t sent, uint32_t idx) mutable {
      if (sent >= file_size) return;  // done; auto-finish follows
      const uint64_t take =
          std::min<uint64_t>(chunk_size, file_size - sent);
      upb_Arena* a = upb_Arena_New();
      auto* c = example_Chunk_new(a);
      auto payload = FillPattern(idx, static_cast<size_t>(take));
      example_Chunk_set_data(
          c, upb_StringView_FromDataAndSize(payload.get(),
                                            static_cast<size_t>(take)));
      const bool more = sent + take < file_size;
      writer.Write(c, [arm, writer, next_sent = sent + take,
                       next_idx = idx + 1, more,
                       status_every](Status st) mutable {
        if (!st.ok()) return;  // dropped / stream ended: stop producing
        if (status_every > 0 && next_idx % status_every == 0 && more) {
          writer.Finish(Status(StatusCode::kInternal,
                               "download aborted (status_every)"));
          return;
        }
        (*arm)(next_sent, next_idx);
      });
      upb_Arena_Free(a);
    };
    (*arm)(0, 0);
  }

  // US2: stream of file chunks -> one receipt (large-file upload).
  void Upload(ServerContext&, ServerReader<example_Chunk>& reader,
              UnaryDone<example_UploadResponse> done) override {
    auto bytes = std::make_shared<uint64_t>(0);
    auto count = std::make_shared<uint32_t>(0);
    auto arm = std::make_shared<
        std::function<void(Status, bool, const example_Chunk*)>>();
    *arm = [arm, bytes, count, done, reader](Status st, bool eos,
                                             const example_Chunk* msg) mutable {
      if (!st.ok()) {
        done(st, nullptr);
        return;
      }
      if (eos) {
        upb_Arena* a = upb_Arena_New();
        auto* out = example_UploadResponse_new(a);
        example_UploadResponse_set_bytes_received(out, *bytes);
        example_UploadResponse_set_chunk_count(out, *count);
        done(Status::Ok(), out);
        upb_Arena_Free(a);
        return;
      }
      *bytes += example_Chunk_data(msg).size;
      (*count)++;
      reader.ReadMessage(*arm);
    };
    reader.ReadMessage(*arm);
  }

  // US3: echo every message back with its sequence number
  void Chat(ServerContext&,
            ServerReaderWriter<example_ChatMsg, example_ChatMsg>& stream)
      override {
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
  }
};

}  // namespace

int main(int argc, char* argv[]) {
  const char* addr = argc > 1 ? argv[1] : "127.0.0.1:50052";
  Server::Options opts;
  opts.listen_address = addr;
  auto server = Server::BuildAndStart(opts, nullptr);
  if (server == nullptr) {
    std::fprintf(stderr, "streaming server: failed to start on %s\n", addr);
    return 1;
  }
  StreamServiceImpl impl;
  auto st = urpc::gen::example::RegisterService(*server, impl);
  if (!st.ok()) {
    std::fprintf(stderr, "streaming server: register failed: %s\n",
                 st.message().c_str());
    return 1;
  }
  std::printf("streaming server ready on %s\n", addr);
  std::fflush(stdout);
  server->Wait();
  return 0;
}
