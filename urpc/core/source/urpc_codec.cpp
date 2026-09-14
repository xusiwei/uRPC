#include "urpc/core/codec.h"

#include <cstring>

namespace urpc {
namespace core {

namespace {

const size_t kPrefixLen = 5;

void AppendU32BE(std::string* out, uint32_t v) {
  out->push_back(static_cast<char>((v >> 24) & 0xff));
  out->push_back(static_cast<char>((v >> 16) & 0xff));
  out->push_back(static_cast<char>((v >> 8) & 0xff));
  out->push_back(static_cast<char>(v & 0xff));
}

}  // namespace

void EncodeFrame(const uint8_t* data, size_t len, std::string* out) {
  out->push_back('\0');  // compressed flag: 0 (no compression, spec scope)
  AppendU32BE(out, static_cast<uint32_t>(len));
  out->append(reinterpret_cast<const char*>(data), len);
}

void EncodeFrame(const std::string& msg, std::string* out) {
  EncodeFrame(reinterpret_cast<const uint8_t*>(msg.data()), msg.size(), out);
}

Status FrameDecoder::Consume(const uint8_t* data, size_t len) {
  buffer_.append(reinterpret_cast<const char*>(data), len);

  while (true) {
    if (!have_header_) {
      if (buffer_.size() < kPrefixLen) return Status::Ok();
      const uint8_t* p = reinterpret_cast<const uint8_t*>(buffer_.data());
      flag_ = p[0];
      if (flag_ != 0) {
        return Status(StatusCode::kInternal,
                      "compressed messages are not supported");
      }
      uint32_t mlen = (static_cast<uint32_t>(p[1]) << 24) |
                      (static_cast<uint32_t>(p[2]) << 16) |
                      (static_cast<uint32_t>(p[3]) << 8) |
                      static_cast<uint32_t>(p[4]);
      if (mlen > max_message_size_) {
        return Status(StatusCode::kResourceExhausted,
                      "message of " + std::to_string(mlen) +
                          " bytes exceeds limit of " +
                          std::to_string(max_message_size_));
      }
      needed_ = mlen;
      have_header_ = true;
      buffer_.erase(0, kPrefixLen);
    }
    if (buffer_.size() < needed_) return Status::Ok();
    messages_.push(buffer_.substr(0, needed_));
    buffer_.erase(0, needed_);
    have_header_ = false;
    needed_ = 0;
  }
}

}  // namespace core
}  // namespace urpc
