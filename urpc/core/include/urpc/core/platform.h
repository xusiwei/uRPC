#pragma once

#include <functional>
#include <string>

#include <uv.h>

#include "urpc/core/status.h"

namespace urpc {
namespace core {
namespace platform {

// Parses "127.0.0.1:50051" / "[::1]:50051" into sockaddr. Returns non-ok
// Status on malformed input. Numeric addresses only (spec: direct dial,
// name resolution out of scope).
Status ParseAddress(const std::string& address, sockaddr_storage* out);

// Asynchronous TCP connect on the given loop. Invokes cb exactly once with
// the connected stream (ownership transferred) or an error Status.
void TcpConnect(uv_loop_t* loop, const std::string& address,
                std::function<void(Status, uv_stream_t*)> cb);

// TCP listener bound to an address; accept callbacks fire on the loop
// thread with the accepted stream (ownership transferred to the callback).
class TcpListener {
 public:
  using AcceptCallback = std::function<void(uv_stream_t*)>;

  TcpListener() = default;
  ~TcpListener();

  TcpListener(const TcpListener&) = delete;
  TcpListener& operator=(const TcpListener&) = delete;

  Status BindAndListen(uv_loop_t* loop, const std::string& address,
                      AcceptCallback on_accept, int backlog = 128);
  // Idempotent; must run on the loop thread. `on_closed` (optional) fires
  // on the loop thread once the handle is fully closed — the object must
  // outlive until then (uv_close is asynchronous).
  void Close(std::function<void()> on_closed = nullptr);

  // Port actually bound (useful with port 0); 0 when not bound.
  uint16_t bound_port() const;

 private:
  static void OnConnection(uv_stream_t* server, int status);
  static void OnClosedTramp(uv_handle_t* handle);

  uv_tcp_t handle_{};
  uv_loop_t* loop_ = nullptr;
  AcceptCallback on_accept_;
  std::function<void()> on_closed_;  // set by Close(); one-shot
};

}  // namespace platform
}  // namespace core
}  // namespace urpc
