#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "urpc/core/status.h"

namespace urpc {

// Public alias: the canonical Status type (urpc::core::Status).
using Status = ::urpc::core::Status;
using StatusCode = ::urpc::core::StatusCode;

template <typename Res>
using UnaryDone = std::function<void(Status, const Res*)>;

template <typename Res> class ServerWriter;
template <typename Req> class ServerReader;
template <typename Req, typename Res> class ServerReaderWriter;

template <typename M>
using UnaryHandlerFn = std::function<void(
    class ServerContext&, const typename M::ReqType*, UnaryDone<typename M::ResType>)>;

// User-facing call view (contracts/server-api.md). Wraps the core call;
// methods are safe to use only from handler invocation context.
class ServerContext {
 public:
  ServerContext();
  ~ServerContext();
  ServerContext(ServerContext&&);
  ServerContext& operator=(ServerContext&&);

  bool IsCancelled() const;
  // Registers a cancellation callback (deadline hit / client cancel /
  // forced shutdown). Returns false when already cancelled.
  bool OnCancel(std::function<void()> cb);
  // Approximate remaining time; 0 when no deadline is set or already hit.
  uint64_t TimeRemainingMs() const;

  const std::string& method_path() const;

  class Impl;  // wraps core ServerCallCtx
  explicit ServerContext(std::unique_ptr<Impl> impl);
  Impl* impl() const { return impl_.get(); }

 private:
  std::unique_ptr<Impl> impl_;
};

class Server {
 public:
  struct Options {
    std::string listen_address = "127.0.0.1:50051";
    size_t max_receive_message_size = 4u * 1024 * 1024;
    uint64_t shutdown_grace_ms = 10000;
  };

  Server();
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  // Typed registration is provided by urpc/unary.h as
  // RegisterUnaryFor<M>() (M = URPC_UNARY_METHOD traits).
  template <typename M>
  ::urpc::Status RegisterUnaryFor(const std::string& service,
                                  const std::string& method,
                                  UnaryHandlerFn<M> handler);

  // Typed streaming registration (spec 004; defined in urpc/stream.h).
  template <typename M>
  ::urpc::Status RegisterServerStreamingFor(
      const std::string& service, const std::string& method,
      std::function<void(ServerContext&, const typename M::ReqType*,
                         ServerWriter<typename M::ResType>&)> handler);
  template <typename M>
  ::urpc::Status RegisterClientStreamingFor(
      const std::string& service, const std::string& method,
      std::function<void(ServerContext&, ServerReader<typename M::ReqType>&,
                         UnaryDone<typename M::ResType>)> handler);
  template <typename M>
  ::urpc::Status RegisterBidiFor(
      const std::string& service, const std::string& method,
      std::function<void(ServerContext&,
                         ServerReaderWriter<typename M::ReqType,
                                            typename M::ResType>&)> handler);

  // Builds and starts the server. Returns nullptr on failure and sets
  // the last error (queryable via GetLastError on the builder).
  static std::shared_ptr<Server> BuildAndStart(
      const Options& options, ::urpc::Status* error = nullptr);

  void Shutdown();  // graceful drain (FR-012)
  void Wait();
  bool IsRunning() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

 public:
  // internal: used by urpc/detail bridges
  Impl* impl() const { return impl_.get(); }
};

}  // namespace urpc
