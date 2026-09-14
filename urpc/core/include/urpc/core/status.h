#pragma once

#include <string>
#include <utility>

namespace urpc {
namespace core {

// gRPC status codes used by this feature (spec FR-003: closed set).
enum class StatusCode : int {
  kOk = 0,
  kDeadlineExceeded = 4,
  kResourceExhausted = 8,
  kUnimplemented = 12,
  kInternal = 13,
  kUnavailable = 14,
  kDataLoss = 15,
};

const char* StatusCodeName(StatusCode code);

class Status {
 public:
  Status() = default;
  Status(StatusCode code, std::string message)
      : code_(code), message_(std::move(message)) {}

  static Status Ok() { return Status(); }

  bool ok() const { return code_ == StatusCode::kOk; }
  StatusCode code() const { return code_; }
  const std::string& message() const { return message_; }

  std::string ToString() const;

 private:
  StatusCode code_ = StatusCode::kOk;
  std::string message_;
};

}  // namespace core
}  // namespace urpc
