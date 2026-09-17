// urpc_echo_server — example unary server (US3).
// Registers example.EchoService/{Echo,SlowEcho} and serves until killed.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>

#include <upb/mem/arena.h>

#include "echo.upb.h"
#include "urpc/server.h"
#include "urpc/unary.h"

namespace {

using urpc::Server;
using urpc::ServerContext;
using urpc::Status;
using urpc::UnaryDone;

URPC_UNARY_METHOD(EchoMethod, example, example__, EchoRequest, EchoResponse)

void HandleEcho(ServerContext&, const example_EchoRequest* req,
                UnaryDone<example_EchoResponse> done) {
  upb_Arena* arena = upb_Arena_New();
  auto* resp = example_EchoResponse_new(arena);
  example_EchoResponse_set_text(
      resp, example_EchoRequest_text(req));
  done(Status::Ok(), resp);
  upb_Arena_Free(arena);  // payload already copied by the framework
}

// SlowEcho: ~200ms before replying (deadline/cancel aid); observes cancel.
void HandleSlowEcho(ServerContext& ctx,
                    const example_EchoRequest* req,
                    UnaryDone<example_EchoResponse> done) {
  const std::string text(example_EchoRequest_text(req).data,
                         example_EchoRequest_text(req).size);
  auto* cancelled = new std::atomic<bool>(false);
  ctx.OnCancel([cancelled] { cancelled->store(true); });

  std::thread([text, cancelled, done]() mutable {
    for (int i = 0; i < 20; i++) {
      if (cancelled->load()) {
        delete cancelled;
        return;  // cancelled: never respond
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    upb_Arena* arena = upb_Arena_New();
    auto* resp = example_EchoResponse_new(arena);
    example_EchoResponse_set_text(
        resp, upb_StringView_FromDataAndSize(text.data(), text.size()));
    done(Status::Ok(), resp);
    upb_Arena_Free(arena);  // payload copied by the framework
    delete cancelled;
  }).detach();
}

}  // namespace

int main(int argc, char** argv) {
  const std::string address = argc > 1 ? argv[1] : "127.0.0.1:50051";

  urpc::Server::Options opts;
  opts.listen_address = address;
  Status err;
  auto server = Server::BuildAndStart(opts, &err);
  if (server == nullptr) {
    std::fprintf(stderr, "echo_server: start failed: %s\n",
                 err.ToString().c_str());
    return 2;
  }
  std::fprintf(stderr, "echo_server: listening on %s\n", address.c_str());

  if (!server->RegisterUnaryFor<EchoMethod>("example.EchoService", "Echo",
                                            &HandleEcho).ok()) {
    return 3;
  }
  if (!server->RegisterUnaryFor<EchoMethod>("example.EchoService", "SlowEcho",
                                            &HandleSlowEcho).ok()) {
    return 3;
  }

  server->Wait();  // serve until the process is killed (example lifecycle)
  return 0;
}
