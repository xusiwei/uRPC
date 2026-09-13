#include "urpc/core/router.h"

namespace urpc {
namespace core {

Status Router::RegisterUnary(const std::string& path, UnaryHandler handler) {
  if (path.empty() || path.front() != '/' || handler == nullptr) {
    return Status(StatusCode::kInternal, "invalid registration: " + path);
  }
  std::lock_guard<std::mutex> lock(mu_);
  auto next = std::make_shared<std::map<std::string, HandlerEntry>>(*snapshot_);
  if (next->count(path) != 0) {
    return Status(StatusCode::kInternal,
                  "duplicate method path: " + path + " (rejected)");
  }
  HandlerEntry entry;
  entry.form = MethodForm::kUnary;
  entry.unary = std::move(handler);
  (*next)[path] = std::move(entry);
  snapshot_ = std::move(next);  // atomic snapshot swap (readers lock-free)
  return Status::Ok();
}

Status Router::RegisterStream(const std::string& path, MethodForm form,
                              StreamHandler handler) {
  if (path.empty() || path.front() != '/' || handler == nullptr) {
    return Status(StatusCode::kInternal, "invalid registration: " + path);
  }
  if (form == MethodForm::kUnary) {
    return Status(StatusCode::kInternal,
                  "RegisterStream requires a streaming form: " + path);
  }
  std::lock_guard<std::mutex> lock(mu_);
  auto next = std::make_shared<std::map<std::string, HandlerEntry>>(*snapshot_);
  if (next->count(path) != 0) {
    return Status(StatusCode::kInternal,
                  "duplicate method path: " + path + " (rejected)");
  }
  HandlerEntry entry;
  entry.form = form;
  entry.stream = std::move(handler);
  (*next)[path] = std::move(entry);
  snapshot_ = std::move(next);
  return Status::Ok();
}

std::optional<HandlerEntry> Router::Find(const std::string& path) const {
  std::shared_ptr<const std::map<std::string, HandlerEntry>> snap;
  {
    std::lock_guard<std::mutex> lock(mu_);
    snap = snapshot_;
  }
  auto it = snap->find(path);
  if (it == snap->end()) return std::nullopt;
  return it->second;
}

size_t Router::size() const {
  std::shared_ptr<const std::map<std::string, HandlerEntry>> snap;
  {
    std::lock_guard<std::mutex> lock(mu_);
    snap = snapshot_;
  }
  return snap->size();
}

}  // namespace core
}  // namespace urpc
