// urpc_streaming_client — example streaming client (spec 004 US1-US3).
// Exercises Range (server streaming), Sum (client streaming) and Chat
// (bidi) against the example server; prints per-form results.

#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

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
using urpc::gen::example::RangeMethod;
using urpc::gen::example::SumMethod;

constexpr const char* kService = "example.StreamService";

int RunRange(std::shared_ptr<Channel>& ch, uint32_t n) {
  upb_Arena* a = upb_Arena_New();
  auto* req = example_RangeRequest_new(a);
  example_RangeRequest_set_count(req, n);
  auto reader = urpc::OpenClientReader<RangeMethod>(
      ch, kService, "Range", req, 10000);
  upb_Arena_Free(a);
  std::vector<uint32_t> got;
  for (;;) {
    Result<example_RangeValue> r = reader->Read();
    if (!r.status().ok()) {
      std::fprintf(stderr, "Range failed: %s\n",
                   r.status().message().c_str());
      return 1;
    }
    if (r.value() == nullptr) break;
    got.push_back(example_RangeValue_value(r.value()));
  }
  Status fin = reader->Finish();
  if (!fin.ok() || got.size() != n) {
    std::fprintf(stderr, "Range mismatch: %zu values, status %s\n",
                 got.size(), fin.message().c_str());
    return 1;
  }
  for (uint32_t i = 0; i < n; ++i) {
    if (got[i] != i) {
      std::fprintf(stderr, "Range out of order at %u\n", i);
      return 1;
    }
  }
  std::printf("[Range ] requested %u, received %zu values in order\n", n,
              got.size());
  return 0;
}

int RunSum(std::shared_ptr<Channel>& ch, uint32_t n) {
  auto writer = urpc::OpenClientWriter<SumMethod>(ch, kService, "Sum",
                                                      10000);
  int64_t expect = 0;
  for (uint32_t i = 1; i <= n; ++i) {
    upb_Arena* a = upb_Arena_New();
    auto* v = example_AddRequest_new(a);
    example_AddRequest_set_value(v, i);
    if (!writer->Write(v)) {
      std::fprintf(stderr, "Sum: write failed at %u\n", i);
      upb_Arena_Free(a);
      return 1;
    }
    expect += i;
    upb_Arena_Free(a);
  }
  writer->WritesDone();
  Result<example_TotalResponse> r = writer->Finish();
  if (!r.ok() || example_TotalResponse_total(r.value()) != expect ||
      example_TotalResponse_count(r.value()) != n) {
    std::fprintf(stderr, "Sum mismatch\n");
    return 1;
  }
  std::printf("[Sum   ] sent %u values, total = %lld\n", n,
              (long long)example_TotalResponse_total(r.value()));
  return 0;
}

int RunChat(std::shared_ptr<Channel>& ch, uint32_t rounds) {
  auto stream = urpc::OpenClientReaderWriter<ChatMethod>(ch, kService, "Chat",
                                                         10000);
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
  std::printf("[Chat  ] %u rounds echoed in order\n", rounds);
  return 0;
}

}  // namespace

int main(int argc, char* argv[]) {
  const char* target = argc > 1 ? argv[1] : "127.0.0.1:50052";
  auto channel = Channel::Connect(target);

  int rc = 0;
  rc |= RunRange(channel, 5);
  rc |= RunSum(channel, 10);
  rc |= RunChat(channel, 5);
  if (rc == 0) std::printf("all streaming forms ok\n");
  return rc;
}
