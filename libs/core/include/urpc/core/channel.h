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

  // --- streaming (spec 004, research.md D5) --------------------------------
  // Terminal event fan-out for a streaming call. on_message fires per
  // response message (unframed) in order; on_complete fires exactly once
  // with the terminal status (trailers grpc-status / timeout / cancel /
  // connection loss).
  struct StreamEvents {
    std::function<void(Status, std::string)> on_message;
    std::function<void(Status)> on_complete;
  };
  // Opens a request stream (headers without END_STREAM) and returns a
  // stream handle for StreamSend/StreamCloseSend/Cancel. `timeout_ms`
  // covers the whole stream lifetime (0 = none).
  uint64_t OpenStream(const std::string& path, StreamEvents events,
                      uint64_t timeout_ms);
  // Sends one framed message; `close` rides END_STREAM on this message.
  // on_flushed fires once the message reached the transport boundary
  // (backpressure; RESOURCE_EXHAUSTED when the 2-deep queue is full).
  void StreamSend(uint64_t stream_id, std::string framed_message,
                  std::function<void(Status)> on_flushed, bool close);
  // Half-closes the request side (no more messages).
  void StreamCloseSend(uint64_t stream_id);

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
