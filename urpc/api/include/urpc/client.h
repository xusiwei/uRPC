#pragma once

#include <functional>
#include <memory>
#include <string>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <future>

#include <upb/mem/arena.h>
#include <upb/message/message.h>
#include <upb/wire/encode.h>


#include "urpc/core/status.h"
#include "urpc/core/codec.h"
#include "urpc/detail/raw.h"

namespace urpc {

// Public alias: the canonical Status type (urpc::core::Status).
using Status = ::urpc::core::Status;
using StatusCode = ::urpc::core::StatusCode;

// Response of a unary call: status + typed payload (arena-backed; the
// message stays valid as long as the Result holds it).
template <typename Res>
class Result {
 public:
  Result() = default;
  Result(::urpc::Status status, std::shared_ptr<const Res> value)
      : status_(std::move(status)), value_(std::move(value)) {}

  bool ok() const { return status_.ok() && value_ != nullptr; }
  const ::urpc::Status& status() const { return status_; }
  const Res* value() const { return value_.get(); }

 private:
  ::urpc::Status status_;
  std::shared_ptr<const Res> value_;
};

class Channel {
 public:
  struct Options {
    std::string target = "127.0.0.1:50051";
    size_t max_receive_message_size = 4u * 1024 * 1024;
  };

  // Lazy connection: the first call triggers the dial. Never fails.
  static std::shared_ptr<Channel> Connect(const std::string& target);

  void set_max_receive_message_size(size_t n);

  // Control-plane: closes the channel. Subsequent typed calls (and calls
  // through proxies referencing this channel) fail immediately with
  // UNAVAILABLE (FR-009); calls already in flight are abandoned. May be
  // called from any thread; idempotent.
  void Close();
  bool closed() const;

  // --- typed unary calls (M = URPC_UNARY_METHOD traits from unary.h) --------
  // Async (primary form; any thread): callback fires exactly once.
  template <typename M>
  uint64_t CallAsync(const std::string& service, const std::string& method,
                     const typename M::ReqType* request, uint64_t timeout_ms,
                     std::function<void(Result<typename M::ResType>)> done) {
    if (closed()) {
      Fail(done, Status(StatusCode::kUnavailable, "channel closed"));
      return 0;
    }
    upb_Arena* arena = upb_Arena_New();
    if (arena == nullptr) {
      Fail(done, Status(StatusCode::kInternal, "arena alloc failed"));
      return 0;
    }
    char* buf = nullptr;
    size_t n = 0;
    upb_EncodeStatus es =
        upb_Encode(reinterpret_cast<const upb_Message*>(request), M::ReqTable(),
                   0, arena, &buf, &n);
    if (es != kUpb_EncodeStatus_Ok) {
      upb_Arena_Free(arena);
      Fail(done, Status(StatusCode::kInternal, "request encode failed"));
      return 0;
    }
    std::string framed;
    urpc::core::EncodeFrame(std::string(buf, n), &framed);
    upb_Arena_Free(arena);

    const std::string path = "/" + service + "/" + method;
    return detail::ChannelCallRaw(
        this, path, framed, timeout_ms,
        [done = std::move(done)](Status st, std::string payload) {
          if (getenv("URPC_WIRE_DEBUG")) {
            std::fprintf(stderr, "[api] resp payload(%zu):", payload.size());
            for (size_t i = 0; i < payload.size(); i++)
              std::fprintf(stderr, " %02x", (unsigned char)payload[i]);
            std::fprintf(stderr, "\n");
          }
          if (!st.ok()) {
            done(Result<typename M::ResType>(st, nullptr));
            return;
          }
          upb_Arena* ra = upb_Arena_New();
          if (ra == nullptr) {
            done(Result<typename M::ResType>(
                Status(StatusCode::kInternal, "arena alloc failed"), nullptr));
            return;
          }
          auto* res = M::ParseResponse(payload.data(), payload.size(), ra);
          if (res == nullptr) {
            upb_Arena_Free(ra);
            done(Result<typename M::ResType>(
                Status(StatusCode::kDataLoss, "response decode failed"),
                nullptr));
            return;
          }
          std::shared_ptr<const typename M::ResType> holder(
              res, [ra](const typename M::ResType*) { upb_Arena_Free(ra); });
          done(Result<typename M::ResType>(st, std::move(holder)));
        });
  }

  // Sync convenience form. MUST NOT be used on a urpc event-loop thread:
  // fast-fails with an INTERNAL status instead of deadlocking (FR-002).
  template <typename M>
  Result<typename M::ResType> Call(const std::string& service,
                                   const std::string& method,
                                   const typename M::ReqType* request,
                                   uint64_t timeout_ms) {
    if (closed()) {
      return Result<typename M::ResType>(
          Status(StatusCode::kUnavailable, "channel closed"), nullptr);
    }
    if (detail::ChannelOnLoopThread(this)) {
      return Result<typename M::ResType>(
          Status(StatusCode::kInternal,
                 "sync Call() is not allowed on the urpc event-loop thread"),
          nullptr);
    }
    std::promise<Result<typename M::ResType>> p;
    auto fut = p.get_future();
    CallAsync<M>(service, method, request, timeout_ms,
                 [&p](Result<typename M::ResType> r) {
                   p.set_value(std::move(r));
                 });
    return fut.get();
  }

  Channel();
  ~Channel();
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  class Impl;
  Impl* impl() const { return impl_.get(); }  // internal: detail bridge

 private:
  template <typename Res>
  static void Fail(std::function<void(Result<Res>)>& done, Status st) {
    if (done) done(Result<Res>(std::move(st), nullptr));
  }

  std::unique_ptr<Impl> impl_;
};

// Control-plane client (spec 003 / FR-010). Holds the connection handle
// (weak: the Channel owner keeps it alive; destroying/closing the channel
// makes every proxy referencing this client fail immediately, FR-009) and
// carries connection/call policy as the mount point for control-plane
// behavior. Proxies are created from here; the proxy is the business-plane
// surface and exposes no connection management.
class Client {
 public:
  struct Options {
    // In-class default ctor instead of an NSDMI: a default argument
    // `Options options = Options{}` on Client's constructors would use
    // the NSDMI before the end of the enclosing class - GCC/Clang
    // reject that (MSVC does not), breaking linux/macos builds.
    Options() : max_receive_message_size(4u * 1024 * 1024) {}
    size_t max_receive_message_size;
  };

  Client() = default;
  explicit Client(std::shared_ptr<Channel> channel,
                  Options options = Options{})
      : channel_(channel), options_(options) {
    if (auto ch = std::shared_ptr<Channel>(channel_))
      ch->set_max_receive_message_size(options_.max_receive_message_size);
  }

  // Factory: connect + apply control-plane options.
  static Client Connect(const std::string& target, Options options = Options{}) {
    return Client(Channel::Connect(target), std::move(options));
  }

  // Control plane: closes the underlying channel (FR-009 semantics).
  void Close() {
    if (auto ch = channel_.lock()) ch->Close();
  }

  // Factory for the business-plane proxy of a generated service.
  template <class P>
  P Proxy() const {
    return P(channel_);
  }

  // internal: channel reference for generated proxy constructors.
  const std::weak_ptr<Channel>& channel_ref() const { return channel_; }

 private:
  std::weak_ptr<Channel> channel_;
  Options options_;
};

}  // namespace urpc
