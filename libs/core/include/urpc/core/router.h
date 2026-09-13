#pragma once

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "urpc/core/status.h"

namespace urpc {
namespace core {

// Call shape of a method (spec 004); mirrors urpc::MethodForm (api layer)
// without pulling api headers into core.
enum class MethodForm {
  kUnary,            // 1 request  -> 1 response
  kServerStreaming,  // 1 request  -> N responses
  kClientStreaming,  // N requests -> 1 response
  kBidi              // N requests -> N responses
};

// Per-call context handed to core unary handlers (untyped: bytes in/out;
// typed wrapping lives in libs/api on top of upb generated code).
class ServerCallCtx {
 public:
  virtual ~ServerCallCtx() = default;

  // Completes the call exactly once. Subsequent calls return an error
  // Status (late writes are rejected, spec edge case).
  virtual Status Respond(Status status, const std::string& payload) = 0;

  virtual bool IsCancelled() const = 0;
  // Registers a cancellation callback (deadline, client cancel, forced
  // shutdown). Returns false when already cancelled.
  virtual bool OnCancel(std::function<void()> cb) = 0;
  // Remaining time in ms; 0 when no deadline is set or it has expired.
  virtual uint64_t TimeRemainingMs() const = 0;

  virtual const std::string& path() const = 0;
};

using UnaryHandler =
    std::function<void(ServerCallCtx&, const std::string& request)>;

// Streaming core call context (spec 004, research.md D1): untyped
// multi-message read/write with explicit half-close and a one-shot
// Finish. Event callbacks fire on the loop thread, strictly in order.
class StreamCallCtx : public ServerCallCtx {
 public:
  // Request side (client -> server). One event per call; messages fire
  // strictly in order; after the peer half-closes exactly one trailing
  // event with eos=true (st ok, empty msg) is delivered. Framing
  // violations / cancellation deliver a non-ok st terminal event.
  virtual void ReadMessage(
      std::function<void(Status st, bool eos, std::string msg)> cb) = 0;
  // Response side (server -> client). `msg` is an UNFRAMED payload; the
  // implementation frames and queues it. `cb` fires exactly once, after
  // the message has been handed to the transport (backpressure boundary,
  // FR-008) or failed. Memory stays bounded: at most 2 messages are held
  // per stream beyond the in-flight one.
  virtual void WriteMessage(std::string msg,
                            std::function<void(Status)> cb) = 0;
  // Server half-close: no more response messages. The stream stays open
  // for Finish() or peer-driven termination.
  virtual void WriteDone() = 0;
  // Terminal: trailers with grpc-status/grpc-message. Exactly once per
  // stream; read/write calls afterwards are rejected.
  virtual void Finish(Status st) = 0;
};

using StreamHandler = std::function<void(StreamCallCtx&)>;

// Path ("/<service>/<method>") → handler registry with copy-on-write
// snapshots so the read path is lock-free (data-model.md: Router).
// Resolved router entry: exactly one of unary/stream is usable, per form.
struct HandlerEntry {
  MethodForm form = MethodForm::kUnary;
  UnaryHandler unary;
  StreamHandler stream;
};

class Router {
 public:
  Status RegisterUnary(const std::string& path, UnaryHandler handler);
  // form must not be kUnary (use RegisterUnary); duplicate path ->
  // ALREADY_EXISTS (same as RegisterUnary).
  Status RegisterStream(const std::string& path, MethodForm form,
                        StreamHandler handler);
  std::optional<HandlerEntry> Find(const std::string& path) const;

  size_t size() const;

 private:
  mutable std::mutex mu_;
  std::shared_ptr<const std::map<std::string, HandlerEntry>> snapshot_ =
      std::make_shared<const std::map<std::string, HandlerEntry>>();
};

}  // namespace core
}  // namespace urpc
