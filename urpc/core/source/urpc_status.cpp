#include "urpc/core/status.h"

namespace urpc {
namespace core {

const char* StatusCodeName(StatusCode code) {
  switch (code) {
    case StatusCode::kOk: return "OK";
    case StatusCode::kDeadlineExceeded: return "DEADLINE_EXCEEDED";
    case StatusCode::kUnimplemented: return "UNIMPLEMENTED";
    case StatusCode::kInternal: return "INTERNAL";
    case StatusCode::kUnavailable: return "UNAVAILABLE";
    case StatusCode::kResourceExhausted: return "RESOURCE_EXHAUSTED";
    case StatusCode::kDataLoss: return "DATA_LOSS";
  }
  return "UNKNOWN";
}

std::string Status::ToString() const {
  if (message_.empty()) return StatusCodeName(code_);
  return std::string(StatusCodeName(code_)) + ": " + message_;
}

}  // namespace core
}  // namespace urpc
