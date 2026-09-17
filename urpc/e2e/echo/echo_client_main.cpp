// urpc_echo_client — example unary client (US3).
// Performs a set of calls against the echo server and exits 0 on success.

#include <cstdio>
#include <memory>
#include <string>

#include <upb/mem/arena.h>

#include "echo.upb.h"
#include "urpc/client.h"
#include "urpc/unary.h"

namespace {

using urpc::Channel;
using urpc::Result;

URPC_UNARY_METHOD(EchoMethod, example, example__, EchoRequest, EchoResponse)

Result<example_EchoResponse> DoEcho(
    Channel* channel, const std::string& text, uint64_t timeout_ms) {
  upb_Arena* arena = upb_Arena_New();
  auto* req = example_EchoRequest_new(arena);
  example_EchoRequest_set_text(
      req, upb_StringView_FromDataAndSize(text.data(), text.size()));
  auto result = channel->Call<EchoMethod>("example.EchoService", "Echo", req,
                                          timeout_ms);
  upb_Arena_Free(arena);  // request encoded before the call
  return result;
}

}  // namespace

int main(int argc, char** argv) {
  const std::string target = argc > 1 ? argv[1] : "127.0.0.1:50051";
  const bool expect_error = argc > 2 && std::string(argv[2]) == "--expect-error";

  auto channel = Channel::Connect(target);
  int failures = 0;

  if (expect_error) {
    // interop error-path mode: unknown method must map to UNIMPLEMENTED
    upb_Arena* a = upb_Arena_New();
    auto* req = example_EchoRequest_new(a);
    example_EchoRequest_set_text(req, upb_StringView_FromString("x"));
    auto r = channel->Call<EchoMethod>("example.EchoService", "DoesNotExist",
                                       req, 3000);
    upb_Arena_Free(a);
    if (r.ok() || r.status().code() != urpc::StatusCode::kUnimplemented) {
      std::fprintf(stderr, "echo_client: expected UNIMPLEMENTED, got: %s\n",
                   r.status().ToString().c_str());
      return 1;
    }
    std::printf("echo_client: error path ok (UNIMPLEMENTED)\n");
    return 0;
  }

  const char* messages[] = {"hello, urpc", "", "unicode: \xe4\xbd\xa0\xe5\xa5\xbd"};
  for (const char* m : messages) {
    auto r = DoEcho(channel.get(), m, 3000);
    if (!r.ok()) {
      std::fprintf(stderr, "echo_client: call failed: %s\n",
                   r.status().ToString().c_str());
      failures++;
      continue;
    }
    upb_StringView got = example_EchoResponse_text(r.value());
    if (std::string(got.data, got.size) != m) {
      std::fprintf(stderr, "echo_client: mismatch: sent '%s' got '%.*s'\n", m,
                   static_cast<int>(got.size), got.data);
      failures++;
    } else {
      std::printf("echo_client: roundtrip ok: '%s'\n", m);
    }
  }

  if (failures > 0) {
    std::fprintf(stderr, "echo_client: %d failures\n", failures);
    return 1;
  }
  std::printf("echo_client: all calls succeeded\n");
  return 0;
}
