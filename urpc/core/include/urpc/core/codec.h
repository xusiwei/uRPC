#pragma once

#include <cstddef>
#include <queue>
#include <cstdint>
#include <string>

#include "urpc/core/status.h"

namespace urpc {
namespace core {

// gRPC message framing: 5-byte prefix (1 compressed flag + 4 big-endian
// length) followed by the encoded message (contracts/wire-protocol.md).

// Appends the framed form of `msg` to `out`.
void EncodeFrame(const std::string& msg, std::string* out);
void EncodeFrame(const uint8_t* data, size_t len, std::string* out);

// Incremental decoder for a stream of length-prefixed messages.
// Enforces a configurable maximum message size (default 4 MiB, FR-007).
class FrameDecoder {
 public:
  explicit FrameDecoder(size_t max_message_size = 4u * 1024 * 1024)
      : max_message_size_(max_message_size) {}

  // Feeds received bytes; returns non-ok Status on framing violations
  // (bad flag byte, declared length over the limit, junk after a message
  // for unary single-message use is judged by the caller via message_count()).
  Status Consume(const uint8_t* data, size_t len);
  Status Consume(const std::string& data) {
    return Consume(reinterpret_cast<const uint8_t*>(data.data()), data.size());
  }

  bool HasMessage() const { return !messages_.empty(); }
  std::string TakeMessage() {
    std::string m = std::move(messages_.front());
    messages_.pop();
    message_count_++;
    return m;
  }
  size_t message_count() const { return message_count_; }
  size_t max_message_size() const { return max_message_size_; }

 private:
  size_t max_message_size_;
  std::string buffer_;       // unconsumed bytes
  size_t needed_ = 0;        // payload bytes still needed (0 = want header)
  bool have_header_ = false;
  uint8_t flag_ = 0;
  // std::queue needs <queue>
  std::queue<std::string> messages_;
  size_t message_count_ = 0;
};

}  // namespace core
}  // namespace urpc
