#pragma once

#include <functional>
#include <memory>
#include <string>

#include "urpc/core/h2_session.h"
#include "urpc/core/router.h"
#include "urpc/core/status.h"

namespace urpc {
namespace core {

class LoopRunner;

// gRPC-compatible unary server on top of libuv + nghttp2 (core layer,
// untyped bytes; typed C++ API lives in libs/api).
class Server {
 public:
  struct Options {
    std::string address = "127.0.0.1:50051";
    size_t max_receive_size = 4u * 1024 * 1024;  // FR-007
    uint64_t shutdown_grace_ms = 10000;          // FR-012
  };

  // loop/router must outlive the Server. Start/Shutdown may be called from
  // any thread; the loop must be running.
  Server(LoopRunner* loop, Router* router, Options options);
  ~Server();

  // Binds and listens; blocks the CALLER until the attempt completes.
  Status Start();
  // Graceful drain (FR-012): stop accepting, wait in-flight up to grace,
  // force-cancel the rest (clients observe UNAVAILABLE).
  void Shutdown();
  // Blocks until the server is fully stopped (after Shutdown).
  void Wait();
  bool IsRunning() const;

 private:
  struct Conn;
  struct Impl;
  struct ConnHook;  // uv read trampoline access
  friend struct ConnHook;
  Impl* impl_;
};

}  // namespace core
}  // namespace urpc
