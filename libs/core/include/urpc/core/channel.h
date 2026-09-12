#pragma once

#include <functional>
#include <memory>
#include <string>

#include "urpc/core/h2_session.h"
#include "urpc/core/status.h"

namespace urpc {
namespace core {

class LoopRunner;

// gRPC-compatible unary client channel (core layer, untyped bytes).
// Thread-safe: Call/Cancel may be invoked from any thread; work is posted
// onto the channel's loop thread (research.md #3). A single HTTP/2
// connection multiplexes all calls (FR-005).
class Channel {
 public:
  struct Options {
    std::string address;
    size_t max_receive_size = 4u * 1024 * 1024;
  };

  Channel(LoopRunner* loop, Options options);
  ~Channel();
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  // Asynchronous unary call. `request` is the FRAMED request message
  // (5-byte prefix + payload; see codec.h). `done` fires exactly once on
  // the loop thread with the response payload (unframed) or a Status.
  // Returns a call id (non-zero) usable with Cancel().
  uint64_t Call(const std::string& path, const std::string& framed_request,
                uint64_t timeout_ms,
                std::function<void(Status, std::string)> done);

  // Best-effort cancel; the done-callback still fires exactly once
  // (with CANCELLED mapped to a Status the caller can distinguish via
  // stream reset — surfaced as UNAVAILABLE per spec closed set).
  void Cancel(uint64_t call_id);

  // Updates the receive-size limit (constitution FR-007); applies to
  // calls started afterwards. Callable from any thread.
  void set_max_receive_size(size_t n);

  bool OnLoopThread() const;

 private:
  struct Impl;
  Impl* impl_;
};

}  // namespace core
}  // namespace urpc
