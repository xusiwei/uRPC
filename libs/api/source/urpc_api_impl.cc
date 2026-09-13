#include "urpc/client.h"
#include "urpc/core/channel.h"
#include "urpc/core/loop.h"
#include "urpc/core/router.h"
#include "urpc/core/server.h"
#include "urpc/detail/raw.h"
#include "urpc/unary.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <memory>
#include <mutex>

namespace urpc {

// ---- ServerContext -----------------------------------------------------------
class ServerContext::Impl {
 public:
  core::ServerCallCtx* ctx = nullptr;
};

ServerContext::ServerContext() : impl_(std::make_unique<Impl>()) {}
ServerContext::~ServerContext() = default;
ServerContext::ServerContext(ServerContext&&) = default;
ServerContext& ServerContext::operator=(ServerContext&&) = default;
ServerContext::ServerContext(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

bool ServerContext::IsCancelled() const {
  return impl_->ctx != nullptr && impl_->ctx->IsCancelled();
}
bool ServerContext::OnCancel(std::function<void()> cb) {
  return impl_->ctx != nullptr && impl_->ctx->OnCancel(std::move(cb));
}
uint64_t ServerContext::TimeRemainingMs() const {
  return impl_->ctx != nullptr ? impl_->ctx->TimeRemainingMs() : 0;
}
const std::string& ServerContext::method_path() const {
  static const std::string empty;
  return impl_->ctx != nullptr ? impl_->ctx->path() : empty;
}

// ---- Server -------------------------------------------------------------------
class Server::Impl {
 public:
  core::LoopRunner loop;
  core::Router router;
  std::unique_ptr<core::Server> server;
  Options options;
};

Server::Server() : impl_(std::make_unique<Impl>()) {}
Server::~Server() {
  if (impl_->server) impl_->server->Shutdown();
}

std::shared_ptr<Server> Server::BuildAndStart(const Options& options,
                                              Status* error) {
  auto s = std::make_shared<Server>();
  s->impl_->options = options;
  if (!s->impl_->loop.Start()) {
    if (error) *error = Status(StatusCode::kInternal, "loop start failed");
    return nullptr;
  }
  s->impl_->server = std::make_unique<core::Server>(
      &s->impl_->loop, &s->impl_->router,
      core::Server::Options{options.listen_address,
                            options.max_receive_message_size,
                            options.shutdown_grace_ms});
  Status st = s->impl_->server->Start();
  if (!st.ok()) {
    if (error) *error = st;
    s->impl_->loop.Stop();
    return nullptr;
  }
  if (error) *error = Status::Ok();
  return s;
}

void Server::Shutdown() {
  if (impl_->server) impl_->server->Shutdown();
}
void Server::Wait() {
  if (impl_->server) impl_->server->Wait();
}
bool Server::IsRunning() const {
  return impl_->server && impl_->server->IsRunning();
}

// ---- Channel -------------------------------------------------------------------
class Channel::Impl {
 public:
  core::LoopRunner loop;
  std::unique_ptr<core::Channel> channel;
  Options options;
  std::atomic<bool> closed{false};
};

Channel::Channel() : impl_(std::make_unique<Impl>()) {}
Channel::~Channel() {
  if (getenv("URPC_WIRE_DEBUG"))
    std::fprintf(stderr, "[dbg] ~Channel begin\n");
  impl_->loop.Stop();
  if (getenv("URPC_WIRE_DEBUG"))
    std::fprintf(stderr, "[dbg] ~Channel end\n");
}

std::shared_ptr<Channel> Channel::Connect(const std::string& target) {
  auto c = std::make_shared<Channel>();
  c->impl_->options.target = target;
  if (!c->impl_->loop.Start()) {
    return nullptr;
  }
  c->impl_->channel = std::make_unique<core::Channel>(
      &c->impl_->loop,
      core::Channel::Options{
          target, c->impl_->options.max_receive_message_size});
  return c;
}

void Channel::Close() {
  if (impl_->closed.exchange(true)) return;  // idempotent
  impl_->channel.reset();                    // core dtor stops its loop
  impl_->loop.Stop();
}

bool Channel::closed() const { return impl_->closed.load(); }

void Channel::set_max_receive_message_size(size_t n) {
  impl_->options.max_receive_message_size = n;
  if (impl_->channel != nullptr)
    impl_->channel->set_max_receive_size(n);  // active calls re-read it
}

// ---- detail bridge (both Impl classes are complete here) ----------------------
namespace detail {

Status RegisterUnaryRaw(
    Server* server, const std::string& service, const std::string& method,
    std::function<void(ServerContext&, const std::string&, RawDone)>
        raw_handler) {
  const std::string path = "/" + service + "/" + method;
  core::UnaryHandler core_handler =
      [raw_handler = std::move(raw_handler)](core::ServerCallCtx& cctx,
                                             const std::string& request) {
        auto impl = std::make_unique<ServerContext::Impl>();
        impl->ctx = &cctx;
        ServerContext ctx(std::move(impl));
        raw_handler(ctx, request,
                    [&cctx](Status st, const std::string& pl) {
                      cctx.Respond(st, pl);
                    });
      };
  return server->impl()->router.RegisterUnary(path, std::move(core_handler));
}

uint64_t ChannelCallRaw(Channel* channel, const std::string& path,
                        const std::string& framed_request, uint64_t timeout_ms,
                        std::function<void(Status, std::string)> done) {
  return channel->impl()->channel->Call(path, framed_request, timeout_ms,
                                       std::move(done));
}

bool ChannelOnLoopThread(Channel* channel) {
  return channel->impl()->loop.OnLoopThread();
}

}  // namespace detail


// ---- streaming bridges (spec 004) --------------------------------------------
namespace {

// Server-side ServerContext view over a streaming core ctx.
class StreamServerCtx {
 public:
  static std::unique_ptr<ServerContext::Impl> Wrap(core::ServerCallCtx* ctx) {
    auto impl = std::make_unique<ServerContext::Impl>();
    impl->ctx = ctx;
    return impl;
  }
};

}  // namespace

namespace detail {

Status RegisterStreamRaw(
    Server* server, const std::string& service, const std::string& method,
    MethodForm form,
    std::function<void(ServerContext&, core::StreamCallCtx&)> body) {
  if (server == nullptr || server->impl() == nullptr) {
    return Status(StatusCode::kInternal, "null server");
  }
  core::MethodForm core_form;
  switch (form) {
    case MethodForm::kServerStreaming:
      core_form = core::MethodForm::kServerStreaming;
      break;
    case MethodForm::kClientStreaming:
      core_form = core::MethodForm::kClientStreaming;
      break;
    case MethodForm::kBidi:
      core_form = core::MethodForm::kBidi;
      break;
    default:
      return Status(StatusCode::kInternal,
                    "RegisterStreamRaw requires a streaming form");
  }
  const std::string path = "/" + service + "/" + method;
  return server->impl()->router.RegisterStream(
      path, core_form, [body](core::StreamCallCtx& core_ctx) {
        ServerContext ctx(StreamServerCtx::Wrap(&core_ctx));
        try {
          body(ctx, core_ctx);
        } catch (...) {
          // handler exception containment (FR-013): finish the stream,
          // never propagate into the event loop
          core_ctx.Finish(Status(StatusCode::kInternal,
                                 "service handler raised an exception"));
        }
      });
}

uint64_t ChannelOpenStreamRaw(
    Channel* channel, const std::string& path,
    std::function<void(Status, std::string)> on_message,
    std::function<void(Status)> on_complete, uint64_t timeout_ms) {
  if (getenv("URPC_WIRE_DEBUG"))
  if (channel == nullptr || channel->impl() == nullptr ||
      channel->closed()) {
    if (on_complete)
      on_complete(Status(StatusCode::kUnavailable, "channel closed"));
    return 0;
  }
  core::Channel::StreamEvents events;
  events.on_message = std::move(on_message);
  events.on_complete = std::move(on_complete);
  return channel->impl()->channel->OpenStream(path, std::move(events),
                                              timeout_ms);
}

void ChannelStreamSendRaw(Channel* channel, uint64_t stream_id,
                          const std::string& framed_message,
                          std::function<void(Status)> on_flushed, bool close) {
  if (channel == nullptr || channel->impl() == nullptr) {
    if (on_flushed)
      on_flushed(Status(StatusCode::kUnavailable, "channel closed"));
    return;
  }
  channel->impl()->channel->StreamSend(stream_id, framed_message,
                                       std::move(on_flushed), close);
}

void ChannelStreamCloseSendRaw(Channel* channel, uint64_t stream_id) {
  if (channel == nullptr || channel->impl() == nullptr) return;
  channel->impl()->channel->StreamCloseSend(stream_id);
}

bool ChannelOnLoopThreadRaw(Channel* channel) {
  return channel != nullptr && channel->impl() != nullptr &&
         channel->impl()->channel->OnLoopThread();
}

}  // namespace detail

}  // namespace urpc
