#pragma once

// Typed streaming API (spec 004, research.md D6): gRPC-C++ shaped
// Reader/Writer pairs over the core multi-message mechanism.
//
// Server side: handlers run on the urpc event-loop thread; Write() queues
// (bounded, FR-008) and Finish() may be implicit (registration wrapper).
// The views wrap the heap-living core stream context, so copies captured
// by callbacks stay valid for the whole stream lifetime.
// Client side: synchronous blocking forms - MUST NOT be used on a urpc
// event-loop thread (fast-fail, FR-002).

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <upb/mem/arena.h>
#include <upb/message/message.h>
#include <upb/wire/decode.h>
#include <upb/wire/encode.h>

#include "urpc/client.h"
#include "urpc/core/codec.h"
#include "urpc/core/server.h"
#include "urpc/detail/raw.h"
#include "urpc/server.h"
#include "urpc/service.h"

namespace urpc {
namespace detail {

struct ServerStreamAccess;  // constructs the typed views below

// Shared between the typed writer/reader and the registration wrapper's
// auto-finish logic (FR-011: nothing written -> UNIMPLEMENTED).
struct ServerWriteState {
  std::atomic<bool> wrote{false};
  std::atomic<bool> finished{false};
  // Writes queued but not yet delivered to the transport; the drain-aware
  // auto-finish defers trailers until this hits zero after the handler
  // returned (so async chained producers can outlive the handler).
  std::atomic<size_t> in_flight{0};
  // Set by the server-streaming wrapper once its handler returned; the
  // last delivery then finishes the stream with Ok. Loop thread only.
  bool handler_returned = false;
  core::StreamCallCtx* finish_ctx = nullptr;

  // Fires exactly once per accepted Write: with Ok once the frame left
  // the transport, or a non-OK status when the message was dropped
  // (send queue full, FR-008) or the stream ended first.
  void OnDelivered() {
    if (in_flight.fetch_sub(1) == 1) MaybeAutoFinish();
  }

  void MaybeAutoFinish() {
    if (in_flight.load() != 0) return;
    if (!handler_returned || finish_ctx == nullptr) return;
    if (finished.exchange(true)) return;
    finish_ctx->Finish(Status::Ok());
  }
};

// Shared client-stream state: ordered message queue + terminal event.
struct ClientStreamState {
  std::mutex mu;
  std::condition_variable cv;
  std::deque<std::string> messages;  // raw (unframed) payloads in order
  bool terminated = false;
  Status terminal;

  void NotifyTerminal(Status st) {
    {
      std::lock_guard<std::mutex> lock(mu);
      terminal = st;
      terminated = true;
    }
    cv.notify_all();
  }
};

}  // namespace detail

// ---- server side -------------------------------------------------------------

// Server-streaming writer (one registered handler per stream).
template <typename Res>
class ServerWriter {
 public:
  bool Write(const Res* msg) { return Write(msg, nullptr); }

  // Flow-correct variant: `delivered` fires exactly once -- Ok once the
  // message left the transport, or a non-OK status when it was dropped
  // (send queue full, FR-008) or the stream ended first. Chaining the
  // next write on `delivered` keeps arbitrarily long streams within the
  // bounded send queue.
  bool Write(const Res* msg, std::function<void(Status)> delivered) {
    auto fail = [&delivered](StatusCode c, const char* m) {
      if (delivered) delivered(Status(c, m));
      return false;
    };
    if (msg == nullptr) return fail(StatusCode::kInternal, "null message");
    if (state_->finished.load()) {
      return fail(StatusCode::kInternal, "stream already finished");
    }
    upb_Arena* arena = upb_Arena_New();
    if (arena == nullptr) {
      return fail(StatusCode::kInternal, "arena alloc failed");
    }
    char* buf = nullptr;
    size_t n = 0;
    upb_EncodeStatus es =
        upb_Encode(reinterpret_cast<const upb_Message*>(msg), table_, 0,
                   arena, &buf, &n);
    if (es != kUpb_EncodeStatus_Ok) {
      upb_Arena_Free(arena);
      return fail(StatusCode::kInternal, "response encode failed");
    }
    std::string payload(buf, n);
    upb_Arena_Free(arena);
    state_->wrote.store(true);
    state_->in_flight.fetch_add(1);
    auto state = state_;
    ctx_->WriteMessage(std::move(payload),
                       [state, cb = std::move(delivered)](Status st) mutable {
                         if (cb) cb(st);
                         state->OnDelivered();
                       });
    return true;
  }

  // Early non-OK termination (at most once). Normal returns finish via
  // the registration wrapper.
  bool Finish(Status st) {
    if (state_->finished.exchange(true)) return false;
    ctx_->Finish(st);
    return true;
  }

 private:
  friend class detail::ServerStreamAccess;
  template <typename ReqT, typename ResT>
  friend class ServerReaderWriter;
  ServerWriter(core::StreamCallCtx* ctx, const upb_MiniTable* table,
               std::shared_ptr<detail::ServerWriteState> state)
      : ctx_(ctx), table_(table), state_(std::move(state)) {}

  core::StreamCallCtx* ctx_;
  const upb_MiniTable* table_;
  std::shared_ptr<detail::ServerWriteState> state_;
};

// Client-streaming reader: messages arrive in order; re-arm ReadMessage
// per message. `msg` stays valid until the next ReadMessage call.
template <typename Req>
class ServerReader {
 public:
  using ReadCb = std::function<void(Status st, bool eos, const Req* msg)>;

  void ReadMessage(ReadCb cb) {
    // Self-contained callback: captures copies (ctx pointer, decoder,
    // shared arena) instead of `this`, so an armed read survives the
    // handler's view copy going out of scope.
    ctx_->ReadMessage([ctx = ctx_, decoder = decoder_, arena = arena,
                       cb = std::move(cb)](Status st, bool eos,
                                           std::string data) mutable {
      if (eos || !st.ok()) {
        cb(st, eos, nullptr);
        return;
      }
      arena = std::shared_ptr<upb_Arena>(upb_Arena_New(), upb_Arena_Free);
      const Req* req =
          (arena != nullptr) ? decoder(arena.get(), data.data(), data.size())
                             : nullptr;
      if (req == nullptr) {
        cb(Status(StatusCode::kDataLoss, "request decode failed"), false,
           nullptr);
        return;
      }
      cb(st, false, req);
    });
  }

 private:
  friend class detail::ServerStreamAccess;
  template <typename ReqT, typename ResT>
  friend class ServerReaderWriter;
  using Decoder = std::function<const Req*(upb_Arena*, const char*, size_t)>;
  ServerReader(core::StreamCallCtx* ctx, Decoder decoder)
      : ctx_(ctx), decoder_(std::move(decoder)),
        arena(upb_Arena_New(), upb_Arena_Free) {}
  core::StreamCallCtx* ctx_;
  Decoder decoder_;
  std::shared_ptr<upb_Arena> arena;
};

// Bidirectional server view: independent read + write halves.
template <typename Req, typename Res>
class ServerReaderWriter : public ServerReader<Req>, public ServerWriter<Res> {
 public:
 private:
  friend class detail::ServerStreamAccess;
  using ReaderBase = ServerReader<Req>;
  using WriterBase = ServerWriter<Res>;
  ServerReaderWriter(core::StreamCallCtx* ctx,
                     typename ReaderBase::Decoder rdec,
                     const upb_MiniTable* wtable,
                     std::shared_ptr<detail::ServerWriteState> state)
      : ReaderBase(ctx, std::move(rdec)),
        WriterBase(ctx, wtable, std::move(state)) {}
};

namespace detail {

struct ServerStreamAccess {
  template <typename Res>
  static ServerWriter<Res> MakeWriter(core::StreamCallCtx* ctx,
                                      const upb_MiniTable* table,
                                      std::shared_ptr<ServerWriteState> state) {
    return ServerWriter<Res>(ctx, table, std::move(state));
  }
  template <typename Req>
  static ServerReader<Req>
  MakeReader(core::StreamCallCtx* ctx,
             std::function<Req*(upb_Arena*, const char*, size_t)> dec) {
    return ServerReader<Req>(ctx, std::move(dec));
  }
  template <typename Req, typename Res>
  static ServerReaderWriter<Req, Res>
  MakeReaderWriter(core::StreamCallCtx* ctx,
                   std::function<Req*(upb_Arena*, const char*, size_t)> dec,
                   const upb_MiniTable* table,
                   std::shared_ptr<ServerWriteState> state) {
    return ServerReaderWriter<Req, Res>(ctx, std::move(dec), table,
                                        std::move(state));
  }
};

// Construction of typed client streams (friend of the client views).
template <typename M>
struct ClientStreamFactory;
}  // namespace detail

// ---- client side (synchronous; off-loop only) --------------------------------

namespace detail {

// Concrete client-stream handle shared between the typed facade methods.
struct ClientStreamImpl {
  // Non-owning: the caller (typed facade / test / proxy) keeps the
  // Channel alive. Owning a shared_ptr here would let ~Channel run on
  // the loop thread when the last reference drops inside a callback
  // (Stop-on-loop-thread detach -> teardown race).
  explicit ClientStreamImpl(Channel* ch) : channel(ch) {}
  Channel* channel;
  uint64_t id = 0;
  ClientStreamState state;
};

inline void WaitTerminal(const std::shared_ptr<ClientStreamImpl>& impl) {
  std::unique_lock<std::mutex> lock(impl->state.mu);
  impl->state.cv.wait(lock, [&] { return impl->state.terminated; });
}

inline void FastFailIfLoopThread(const ClientStreamImpl& impl, const char* what) {
  (void)impl;
  (void)what;
}

}  // namespace detail

// Server-streaming read end: Read() per message, Finish() for the status.
template <typename Res>
class ClientReader {
 public:
  using Decoder = std::function<Res*(upb_Arena*, const char*, size_t)>;

  Result<Res> Read() {
    std::unique_lock<std::mutex> lock(impl_->state.mu);
    impl_->state.cv.wait(lock, [&] {
      return !impl_->state.messages.empty() || impl_->state.terminated;
    });
    if (!impl_->state.messages.empty()) {
      std::string payload = std::move(impl_->state.messages.front());
      impl_->state.messages.pop_front();
      lock.unlock();
      return Decode(std::move(payload));
    }
    if (impl_->state.terminal.ok()) {
      return Result<Res>(Status::Ok(), nullptr);  // end of stream
    }
    return Result<Res>(impl_->state.terminal, nullptr);
  }

  Status Finish() {
    if (detail::ChannelOnLoopThreadRaw(impl_->channel)) {
      return Status(StatusCode::kInternal,
                    "sync Finish() is not allowed on the urpc event-loop "
                    "thread");
    }
    detail::WaitTerminal(impl_);
    return impl_->state.terminal;
  }

 private:
  template <typename T>
  friend struct detail::ClientStreamFactory;
  template <typename ReqT, typename ResT>
  friend class ClientReaderWriter;
  ClientReader(std::shared_ptr<detail::ClientStreamImpl> impl, Decoder dec)
      : impl_(std::move(impl)), decoder_(std::move(dec)) {}

  Result<Res> Decode(std::string payload) {
    upb_Arena* arena = upb_Arena_New();
    if (arena == nullptr) {
      return Result<Res>(Status(StatusCode::kInternal, "arena alloc failed"),
                         nullptr);
    }
    Res* res = decoder_(arena, payload.data(), payload.size());
    if (res == nullptr) {
      upb_Arena_Free(arena);
      return Result<Res>(Status(StatusCode::kDataLoss, "decode failed"),
                         nullptr);
    }
    std::shared_ptr<const Res> holder(res,
                                      [arena](const Res*) { upb_Arena_Free(arena); });
    return Result<Res>(Status::Ok(), std::move(holder));
  }

  std::shared_ptr<detail::ClientStreamImpl> impl_;
  Decoder decoder_;
};

// Client-streaming write end: blocking Write (flow-correct), WritesDone,
// then the single Finish response.
template <typename Req, typename Res>
class ClientWriter {
 public:
  bool Write(const Req* req) {
    if (req == nullptr || impl_->state.terminated) return false;
    if (detail::ChannelOnLoopThreadRaw(impl_->channel)) return false;
    upb_Arena* arena = upb_Arena_New();
    if (arena == nullptr) return false;
    char* buf = nullptr;
    size_t n = 0;
    upb_EncodeStatus es =
        upb_Encode(reinterpret_cast<const upb_Message*>(req), req_table_, 0,
                   arena, &buf, &n);
    if (es != kUpb_EncodeStatus_Ok) {
      upb_Arena_Free(arena);
      return false;
    }
    std::string framed;
    urpc::core::EncodeFrame(std::string(buf, n), &framed);
    upb_Arena_Free(arena);
    std::promise<Status> delivered;
    auto fut = delivered.get_future();
    detail::ChannelStreamSendRaw(impl_->channel, impl_->id, framed,
                                 [&delivered](Status st) {
                                   delivered.set_value(st);
                                 },
                                 false);
    return fut.get().ok();
  }

  void WritesDone() {
    if (writes_done_.exchange(true)) return;
    detail::ChannelStreamCloseSendRaw(impl_->channel, impl_->id);
  }

  Result<Res> Finish() {
    WritesDone();
    if (detail::ChannelOnLoopThreadRaw(impl_->channel)) {
      return Result<Res>(Status(StatusCode::kInternal,
                                "sync Finish() is not allowed on the urpc "
                                "event-loop thread"),
                         nullptr);
    }
    detail::WaitTerminal(impl_);
    if (!impl_->state.terminal.ok()) {
      return Result<Res>(impl_->state.terminal, nullptr);
    }
    std::unique_lock<std::mutex> lock(impl_->state.mu);
    if (impl_->state.messages.empty()) {
      return Result<Res>(Status(StatusCode::kDataLoss, "missing response"),
                         nullptr);
    }
    std::string payload = std::move(impl_->state.messages.front());
    impl_->state.messages.clear();
    lock.unlock();
    upb_Arena* arena = upb_Arena_New();
    if (arena == nullptr) {
      return Result<Res>(Status(StatusCode::kInternal, "arena alloc failed"),
                         nullptr);
    }
    Res* res = res_decoder_(arena, payload.data(), payload.size());
    if (res == nullptr) {
      upb_Arena_Free(arena);
      return Result<Res>(Status(StatusCode::kDataLoss, "decode failed"),
                         nullptr);
    }
    std::shared_ptr<const Res> holder(
        res, [arena](const Res*) { upb_Arena_Free(arena); });
    return Result<Res>(Status::Ok(), std::move(holder));
  }

 private:
  template <typename T>
  friend struct detail::ClientStreamFactory;
  template <typename ReqT, typename ResT>
  friend class ClientReaderWriter;
  ClientWriter(std::shared_ptr<detail::ClientStreamImpl> impl,
               const upb_MiniTable* req_table,
               std::function<Res*(upb_Arena*, const char*, size_t)> res_decoder)
      : impl_(std::move(impl)),
        req_table_(req_table),
        res_decoder_(std::move(res_decoder)) {}

  std::shared_ptr<detail::ClientStreamImpl> impl_;
  const upb_MiniTable* req_table_;
  std::function<Res*(upb_Arena*, const char*, size_t)> res_decoder_;
  std::atomic<bool> writes_done_{false};
};

// Bidirectional client view: independent read and write halves.
template <typename Req, typename Res>
class ClientReaderWriter {
 public:
  Result<Res> Read() { return reader_.Read(); }
  bool Write(const Req* req) { return writer_.Write(req); }
  void WritesDone() { writer_.WritesDone(); }
  Status Finish() {
    writer_.WritesDone();
    if (detail::ChannelOnLoopThreadRaw(impl_->channel)) {
      return Status(StatusCode::kInternal,
                    "sync Finish() is not allowed on the urpc event-loop "
                    "thread");
    }
    detail::WaitTerminal(impl_);
    return impl_->state.terminal;
  }

 private:
  template <typename T>
  friend struct detail::ClientStreamFactory;
  ClientReaderWriter(std::shared_ptr<detail::ClientStreamImpl> impl,
                     std::function<Res*(upb_Arena*, const char*, size_t)> res_dec,
                     const upb_MiniTable* req_table)
      : impl_(std::move(impl)),
        reader_(impl_, std::move(res_dec)),
        writer_(impl_, req_table,
                [](upb_Arena*, const char*, size_t) { return nullptr; }) {}

  std::shared_ptr<detail::ClientStreamImpl> impl_;
  ClientReader<Res> reader_;
  ClientWriter<Req, Res> writer_;
};

namespace detail {

// Construction of typed client streams from method traits; the only code
// allowed to touch the private typed-view constructors.
template <typename M>
struct ClientStreamFactory {
  using Req = typename M::ReqType;
  using Res = typename M::ResType;

  static std::shared_ptr<ClientStreamImpl> Open(
      Channel* channel, const std::string& service, const std::string& method,
      uint64_t timeout_ms) {
    auto impl = std::make_shared<ClientStreamImpl>(channel);
    const std::string path = "/" + service + "/" + method;
    impl->id = ChannelOpenStreamRaw(
        channel, path,
        [impl](Status st, std::string payload) {
          if (!st.ok()) return;
          std::lock_guard<std::mutex> lock(impl->state.mu);
          impl->state.messages.push_back(std::move(payload));
          impl->state.cv.notify_all();
        },
        [impl](Status st) { impl->state.NotifyTerminal(st); },
        timeout_ms);
    return impl;
  }

  static void SendSingleRequest(std::shared_ptr<ClientStreamImpl> impl,
                                const Req* request) {
    upb_Arena* arena = upb_Arena_New();
    if (arena == nullptr) return;
    char* buf = nullptr;
    size_t n = 0;
    upb_EncodeStatus es = upb_Encode(reinterpret_cast<const upb_Message*>(request),
                                     M::ReqTable(), 0, arena, &buf, &n);
    if (es != kUpb_EncodeStatus_Ok) {
      upb_Arena_Free(arena);
      return;
    }
    std::string framed;
    urpc::core::EncodeFrame(std::string(buf, n), &framed);
    upb_Arena_Free(arena);
    ChannelStreamSendRaw(impl->channel, impl->id, framed, nullptr, true);
  }

  static Res* DecodeRes(upb_Arena* arena, const char* data, size_t size) {
    return M::ParseResponse(data, size, arena);
  }

  static ClientReader<Res> MakeReader(std::shared_ptr<ClientStreamImpl> impl) {
    return ClientReader<Res>(std::move(impl), &ClientStreamFactory::DecodeRes);
  }

  static ClientWriter<Req, Res>
  MakeWriter(std::shared_ptr<ClientStreamImpl> impl) {
    return ClientWriter<Req, Res>(std::move(impl), M::ReqTable(),
                                  &ClientStreamFactory::DecodeRes);
  }

  static ClientReaderWriter<Req, Res>
  MakeReaderWriter(std::shared_ptr<ClientStreamImpl> impl) {
    return ClientReaderWriter<Req, Res>(std::move(impl),
                                        &ClientStreamFactory::DecodeRes,
                                        M::ReqTable());
  }
};

}  // namespace detail

// Typed stream openings from method traits (M = generated or
// URPC_UNARY_METHOD-style traits with ReqTable/ResTable/ParseRequest/
// ParseResponse). Server-streaming: sends the single request and returns
// the read end.
template <typename M>
std::unique_ptr<ClientReader<typename M::ResType>> OpenClientReader(
    std::shared_ptr<Channel> channel, const std::string& service,
    const std::string& method, const typename M::ReqType* request,
    uint64_t timeout_ms) {
  using Factory = detail::ClientStreamFactory<M>;
  auto impl = Factory::Open(channel.get(), service, method, timeout_ms);
  Factory::SendSingleRequest(impl, request);
  return std::unique_ptr<ClientReader<typename M::ResType>>(
      new ClientReader<typename M::ResType>(Factory::MakeReader(std::move(impl))));
}

// Client-streaming: opens the stream, returns the write end.
template <typename M>
std::unique_ptr<ClientWriter<typename M::ReqType, typename M::ResType>>
OpenClientWriter(
    std::shared_ptr<Channel> channel, const std::string& service,
    const std::string& method, uint64_t timeout_ms) {
  using Factory = detail::ClientStreamFactory<M>;
  auto impl = Factory::Open(channel.get(), service, method, timeout_ms);
  return std::unique_ptr<ClientWriter<typename M::ReqType, typename M::ResType>>(
      new ClientWriter<typename M::ReqType, typename M::ResType>(
          Factory::MakeWriter(std::move(impl))));
}

// Bidirectional: opens the stream, returns the combined view.
template <typename M>
std::unique_ptr<ClientReaderWriter<typename M::ReqType, typename M::ResType>>
OpenClientReaderWriter(std::shared_ptr<Channel> channel,
                       const std::string& service, const std::string& method,
                       uint64_t timeout_ms) {
  using Factory = detail::ClientStreamFactory<M>;
  auto impl = Factory::Open(channel.get(), service, method, timeout_ms);
  return std::unique_ptr<
      ClientReaderWriter<typename M::ReqType, typename M::ResType>>(
      new ClientReaderWriter<typename M::ReqType, typename M::ResType>(
          Factory::MakeReaderWriter(std::move(impl))));
}

// ---- server-side registration wrappers (FR-011/FR-013) ------------------------

template <typename M>
::urpc::Status Server::RegisterServerStreamingFor(
    const std::string& service, const std::string& method,
    std::function<void(ServerContext&, const typename M::ReqType*,
                       ServerWriter<typename M::ResType>&)>
        handler) {
  using Res = typename M::ResType;
  return detail::RegisterStreamRaw(
      this, service, method, MethodForm::kServerStreaming,
      [handler](ServerContext& ctx, core::StreamCallCtx& core_ctx) {
        auto state = std::make_shared<detail::ServerWriteState>();
        ServerWriter<Res> writer = detail::ServerStreamAccess::MakeWriter<Res>(
            &core_ctx, M::ResTable(), state);
        core_ctx.ReadMessage(
            [pc = &core_ctx, &ctx, handler, state,
             writer](Status st, bool eos, std::string data) mutable {
              if (!st.ok() || eos) return;
              upb_Arena* arena = upb_Arena_New();
              if (arena == nullptr) {
                pc->Finish(Status(StatusCode::kInternal, "arena alloc failed"));
                return;
              }
              const auto* req =
                  M::ParseRequest(data.data(), data.size(), arena);
              if (req == nullptr) {
                upb_Arena_Free(arena);
                pc->Finish(Status(StatusCode::kDataLoss,
                                  "request decode failed"));
                return;
              }
              handler(ctx, req, writer);
              upb_Arena_Free(arena);
              if (state->finished.load()) return;
              if (!state->wrote.load()) {
                pc->Finish(Status(StatusCode::kUnimplemented,
                                  "method not implemented: no responses "
                                  "written"));
                return;
              }
              // Drain-aware auto-finish: while writes are still traversing
              // the bounded send queue, defer the trailers until the last
              // delivery (a chained producer keeps writing meanwhile).
              state->handler_returned = true;
              state->finish_ctx = pc;
              state->MaybeAutoFinish();
            });
      });
}

template <typename M>
::urpc::Status Server::RegisterClientStreamingFor(
    const std::string& service, const std::string& method,
    std::function<void(ServerContext&, ServerReader<typename M::ReqType>&,
                       UnaryDone<typename M::ResType>)>
        handler) {
  using Req = typename M::ReqType;
  using Res = typename M::ResType;
  return detail::RegisterStreamRaw(
      this, service, method, MethodForm::kClientStreaming,
      [handler](ServerContext& ctx, core::StreamCallCtx& core_ctx) {
        auto state = std::make_shared<detail::ServerWriteState>();
        ServerReader<Req> reader =
            detail::ServerStreamAccess::MakeReader<Req>(
                &core_ctx, [](upb_Arena* arena, const char* data, size_t size) {
                  return M::ParseRequest(data, size, arena);
                });
        UnaryDone<Res> done = [&core_ctx, state](Status st, const Res* res) {
          state->finished.store(true);
          if (!st.ok() || res == nullptr) {
            core_ctx.Finish(st.ok() ? Status(StatusCode::kInternal,
                                           "missing response message")
                                  : st);
            return;
          }
          upb_Arena* arena = upb_Arena_New();
          if (arena == nullptr) {
            core_ctx.Finish(Status(StatusCode::kInternal, "arena alloc failed"));
            return;
          }
          char* buf = nullptr;
          size_t n = 0;
          upb_EncodeStatus es =
              upb_Encode(reinterpret_cast<const upb_Message*>(res),
                         M::ResTable(), 0, arena, &buf, &n);
          if (es != kUpb_EncodeStatus_Ok) {
            upb_Arena_Free(arena);
            core_ctx.Finish(
                Status(StatusCode::kInternal, "response encode failed"));
            return;
          }
          std::string payload(buf, n);
          upb_Arena_Free(arena);
          state->wrote.store(true);
          core_ctx.WriteMessage(payload, [state](Status) {});
          core_ctx.Finish(Status::Ok());
        };
        // The handler owns termination through `done` (contracts S2):
        // it is asynchronous (returns after arming reads), so there is no
        // auto-finish on return here.
        handler(ctx, reader, std::move(done));
      });
}

template <typename M>
::urpc::Status Server::RegisterBidiFor(
    const std::string& service, const std::string& method,
    std::function<void(ServerContext&,
                       ServerReaderWriter<typename M::ReqType,
                                          typename M::ResType>&)>
        handler) {
  using Req = typename M::ReqType;
  using Res = typename M::ResType;
  return detail::RegisterStreamRaw(
      this, service, method, MethodForm::kBidi,
      [handler](ServerContext& ctx, core::StreamCallCtx& core_ctx) {
        auto state = std::make_shared<detail::ServerWriteState>();
        ServerReaderWriter<Req, Res> stream =
            detail::ServerStreamAccess::MakeReaderWriter<Req, Res>(
                &core_ctx,
                [](upb_Arena* arena, const char* data, size_t size) {
                  return M::ParseRequest(data, size, arena);
                },
                M::ResTable(), state);
        // The handler owns termination (contracts S2): bidi handlers are
        // asynchronous; call stream.Finish() when the stream is done.
        handler(ctx, stream);
      });
}

}  // namespace urpc
