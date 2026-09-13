// urpc_streaming_server — example streaming server (spec 004 US1-US3).
// Registers example.StreamService/{Range,Sum,Chat} via the generated
// typed interface and serves until killed.

#include <atomic>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>

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
using urpc::UnaryDone;

class StreamServiceImpl : public urpc::gen::example::IStreamService {
 protected:
  // US1: single request -> stream of values 0..count-1
  void Range(ServerContext&, const example_RangeRequest* req,
             ServerWriter<example_RangeValue>& writer) override {
    const uint32_t n = example_RangeRequest_count(req) > 1000
                           ? 1000
                           : example_RangeRequest_count(req);
    for (uint32_t i = 0; i < n; ++i) {
      upb_Arena* a = upb_Arena_New();
      auto* v = example_RangeValue_new(a);
      example_RangeValue_set_value(v, i);
      writer.Write(v);
      upb_Arena_Free(a);
    }
  }

  // US2: stream of values -> single total
  void Sum(ServerContext&, ServerReader<example_AddRequest>& reader,
           UnaryDone<example_TotalResponse> done) override {
    auto total = std::make_shared<int64_t>(0);
    auto count = std::make_shared<uint32_t>(0);
    auto arm = std::make_shared<
        std::function<void(Status, bool, const example_AddRequest*)>>();
    *arm = [arm, total, count, done, reader](Status st, bool eos,
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
