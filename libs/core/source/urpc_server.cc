#include "urpc/core/server.h"

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <mutex>

#include <uv.h>

#include "urpc/core/codec.h"
#include "urpc/core/log.h"
#include "urpc/core/loop.h"
#include "urpc/core/platform.h"

namespace urpc {
namespace core {

namespace {
constexpr const char* kContentType = "application/grpc";

void UvAlloc(uv_handle_t*, size_t suggested, uv_buf_t* buf) {
  buf->base = new char[suggested];
  buf->len = suggested;
}

void UvWriteCb(uv_write_t* req, int) {
  delete[] static_cast<char*>(req->data);
  delete req;
}

uint64_t ParseGrpcTimeout(const std::string& v) {
  // gRPC format: value + unit (H hours, M minutes, S seconds, m millis,
  // u micros, n nanos) — contracts/wire-protocol.md.
  if (v.size() < 2) return 0;
  const char unit = v.back();
  uint64_t n = 0;
  try {
    n = std::stoull(v.substr(0, v.size() - 1));
  } catch (...) {
    return 0;
  }
  switch (unit) {
    case 'H': return n * 3600000ull;
    case 'M': return n * 60000ull;
    case 'S': return n * 1000ull;
    case 'm': return n;
    case 'u': return n / 1000ull;
    case 'n': return n / 1000000ull;
    default: return 0;
  }
}
}  // namespace

// ---- Server::Impl ------------------------------------------------------------
struct Server::Impl {
  struct Conn;  // defined below
  LoopRunner* loop;
  Router* router;
  Options options;

  std::unique_ptr<platform::TcpListener> listener;
  bool listening = false;

  std::mutex mu;
  std::condition_variable cv;
  std::deque<std::unique_ptr<Conn>> conns;
  bool shutdown_requested = false;
  bool drained = false;
  Status start_status = Status::Ok();
  bool start_done = false;

  uint64_t RemainingMsOf(uint64_t timer_id) const;
  void OnConnIdle();
  void FinishShutdown();
};

// ---- per-connection state (defined here; opaque in the header) -------------
struct Server::Impl::Conn : public H2Session::Handler {
  Server* server = nullptr;
  uv_tcp_t* socket = nullptr;
  std::unique_ptr<H2Session> session;
  bool closed = false;

  struct Call {
    std::string path;
    std::unique_ptr<FrameDecoder> decoder;
    uint64_t deadline_timer = 0;
    uint64_t started_ms = 0;
    bool responded = false;
    bool request_done = false;
    bool cancel_fired = false;
    std::vector<std::function<void()>> cancel_cbs;
  };
  std::map<int32_t, Call> calls;
  std::map<int32_t, std::unique_ptr<ServerCallCtx>> ctxs;

  void OnHeadersComplete(int32_t sid, const H2Session::HeaderMap& headers,
                         bool end_stream) override {
    auto ct = headers.find("content-type");
    auto pt = headers.find(":path");
    if (ct == headers.end() || pt == headers.end() ||
        ct->second.rfind(kContentType, 0) != 0) {
      TrailersOnly(sid, StatusCode::kInternal, "bad request headers");
      return;
    }
    Call& call = calls[sid];
    call.path = pt->second;
    call.started_ms = server->impl_->loop->NowMs();
    call.decoder =
        std::make_unique<FrameDecoder>(server->impl_->options.max_receive_size);

    auto to = headers.find("grpc-timeout");
    if (to != headers.end()) {
      const uint64_t ms = ParseGrpcTimeout(to->second);
      if (ms > 0) {
        call.deadline_timer = server->impl_->loop->SetTimer(
            ms, [this, sid] { OnDeadline(sid); });
      }
    }
    if (end_stream) FinishRequest(sid);
  }

  void OnData(int32_t sid, const uint8_t* data, size_t len,
              bool end_stream) override {
    auto it = calls.find(sid);
    if (it == calls.end()) return;
    Call& call = it->second;
    if (call.request_done) return;
    if (data != nullptr && len > 0) {
      Status st = call.decoder->Consume(data, len);
      if (!st.ok()) {
        RespondError(sid, st);
        return;
      }
    }
    if (end_stream) FinishRequest(sid);
  }

  void OnStreamClose(int32_t sid, uint32_t) override {
    auto it = calls.find(sid);
    if (it != calls.end()) {
      FireCancel(it->second);
      if (it->second.deadline_timer != 0) {
        server->impl_->loop->CancelTimer(it->second.deadline_timer);
      }
      calls.erase(it);
      ctxs.erase(sid);
    }
    server->impl_->OnConnIdle();
  }

  void OnWrite(const uint8_t* data, size_t len) override {
    if (getenv("URPC_WIRE_DEBUG"))
      fprintf(stderr, "[srv] OnWrite %zu bytes socket=%p closed=%d\n", len,
              (void*)socket, (int)closed);
    if (socket == nullptr || closed) return;
    auto* req = new uv_write_t();
    auto* copy = new char[len];
    memcpy(copy, data, len);
    req->data = copy;
    uv_buf_t b = uv_buf_init(copy, static_cast<unsigned>(len));
    uv_write(req, reinterpret_cast<uv_stream_t*>(socket), &b, 1, &UvWriteCb);
  }

  void FinishRequest(int32_t sid) {
    auto it = calls.find(sid);
    if (it == calls.end() || it->second.request_done) return;
    Call& call = it->second;
    call.request_done = true;
    if (getenv("URPC_WIRE_DEBUG"))
      fprintf(stderr, "[srv] FinishRequest sid=%d path=%s ms=%llu\n", (int)sid,
              call.path.c_str(),
              (unsigned long long)server->impl_->loop->NowMs());

    auto handler = server->impl_->router->Find(call.path);
    if (!handler.has_value()) {
      if (getenv("URPC_WIRE_DEBUG"))
        fprintf(stderr, "[srv] no handler for %s\n", call.path.c_str());
      TrailersOnly(sid, StatusCode::kUnimplemented,
                   "unknown method: " + call.path);
      return;
    }
    if (call.decoder->message_count() != 0) {
      TrailersOnly(sid, StatusCode::kInternal, "multiple request messages");
      return;
    }
    if (!call.decoder->HasMessage()) {
      TrailersOnly(sid, StatusCode::kInternal, "missing request message");
      return;
    }
    std::string request = call.decoder->TakeMessage();
    auto ctx = std::make_unique<Ctx>(this, sid);
    Ctx* raw = ctx.get();
    ctxs[sid] = std::move(ctx);
    if (getenv("URPC_WIRE_DEBUG"))
      fprintf(stderr, "[srv] dispatch sid=%d\n", (int)sid);
    (*handler)(*raw, request);
  }

  void Respond(int32_t sid, Status status, const std::string& payload) {
    auto it = calls.find(sid);
    if (it == calls.end() || it->second.responded) return;  // gone/late write
    Call& call = it->second;
    call.responded = true;
    StopDeadline(call);
    const uint64_t dur = server->impl_->loop->NowMs() - call.started_ms;
    if (status.ok()) {
      std::string framed;
      EncodeFrame(payload, &framed);
      session->SendHeaders(
          sid, {{":status", "200"}, {"content-type", kContentType}}, false);
      session->SendData(sid, framed, false);
      session->SendTrailers(sid, {{"grpc-status", "0"}});
    } else {
      SendTrailersOnly(sid, status.code(), status.message());
    }
    log::Info(log::LogCategory::kCall, "call_end",
              "path=" + call.path + " sid=" + std::to_string(sid) +
                  " status=" + StatusCodeName(status.code()) +
                  " us=" + std::to_string(dur));
  }

  // Emits a trailers-only response (initial HEADERS carry grpc-status and
  // END_STREAM). The caller owns the responded-guard.
  void SendTrailersOnly(int32_t sid, StatusCode code, const std::string& msg) {
    if (getenv("URPC_WIRE_DEBUG"))
      fprintf(stderr, "[srv] SendTrailersOnly sid=%d code=%d\n", (int)sid,
              (int)code);
    session->SendHeaders(sid,
                         {{":status", "200"},
                          {"content-type", kContentType},
                          {"grpc-status", std::to_string(static_cast<int>(code))},
                          {"grpc-message", msg}},
                         true);
    log::Info(log::LogCategory::kCall, "call_end",
              "sid=" + std::to_string(sid) + " status=" + StatusCodeName(code));
  }

  void TrailersOnly(int32_t sid, StatusCode code, const std::string& msg) {
    if (getenv("URPC_WIRE_DEBUG"))
      fprintf(stderr, "[srv] TrailersOnly sid=%d code=%d\n", (int)sid,
              (int)code);
    auto it = calls.find(sid);
    if (it != calls.end()) {
      if (it->second.responded) return;
      it->second.responded = true;
      StopDeadline(it->second);
    }
    SendTrailersOnly(sid, code, msg);
  }

  void RespondError(int32_t sid, Status st) {
    auto it = calls.find(sid);
    if (it == calls.end() || it->second.responded) return;
    it->second.responded = true;
    StopDeadline(it->second);
    TrailersOnly(sid, st.code(), st.message());
  }

  void OnDeadline(int32_t sid) {
    auto it = calls.find(sid);
    if (it == calls.end()) return;
    FireCancel(it->second);
    if (!it->second.responded) {
      it->second.responded = true;
      it->second.deadline_timer = 0;
      SendTrailersOnly(sid, StatusCode::kDeadlineExceeded,
                       "deadline exceeded");
    }
  }

  void StopDeadline(Call& call) {
    if (call.deadline_timer != 0) {
      server->impl_->loop->CancelTimer(call.deadline_timer);
      call.deadline_timer = 0;
    }
  }

  void FireCancel(Call& call) {
    if (call.cancel_fired) return;
    call.cancel_fired = true;
    for (auto& cb : call.cancel_cbs) cb();
    call.cancel_cbs.clear();
  }

  // ---- ServerCallCtx view ----------------------------------------------------
  class Ctx : public ServerCallCtx {
   public:
    Ctx(Conn* conn, int32_t sid) : conn_(conn), sid_(sid) {}
    Status Respond(Status status, const std::string& payload) override {
      conn_->Respond(sid_, status, payload);
      return Status::Ok();
    }
    bool IsCancelled() const override {
      auto it = conn_->calls.find(sid_);
      return it == conn_->calls.end() || it->second.cancel_fired;
    }
    bool OnCancel(std::function<void()> cb) override {
      auto it = conn_->calls.find(sid_);
      if (it == conn_->calls.end() || it->second.cancel_fired) return false;
      it->second.cancel_cbs.push_back(std::move(cb));
      return true;
    }
    uint64_t TimeRemainingMs() const override {
      // exact remaining time is owned by the deadline timer; a cancelled
      // call reports 0
      auto it = conn_->calls.find(sid_);
      if (it == conn_->calls.end() || it->second.cancel_fired) return 0;
      if (it->second.deadline_timer == 0) return 0;  // no deadline
      // approximate: deadline = started + timeout; timeout unknown here,
      // but the timer id presence is the contract for "has deadline"
      return conn_->server->impl_->RemainingMsOf(it->second.deadline_timer);
    }
    const std::string& path() const override {
      static const std::string empty;
      auto it = conn_->calls.find(sid_);
      return it != conn_->calls.end() ? it->second.path : empty;
    }

   private:
    Conn* conn_;
    int32_t sid_;
  };
};

// ---- Impl method bodies (need complete Conn) -------------------------------
uint64_t Server::Impl::RemainingMsOf(uint64_t timer_id) const {
  // Deadline bookkeeping lives in the timer; handlers should rely on
  // cancellation callbacks instead of polling remaining time.
  return timer_id != 0 ? 1 : 0;
}

void Server::Impl::OnConnIdle() {
  std::function<void()> close_listener;
  {
    std::lock_guard<std::mutex> lock(mu);
    for (auto it = conns.begin(); it != conns.end();) {
      if ((*it)->closed) {
        it = conns.erase(it);
      } else {
        ++it;
      }
    }
    // Last connection finished during shutdown: close the listener too
    // (same loop thread) and complete the drain handshake in its close
    // callback — the handle must be fully closed before ~TcpListener.
    if (shutdown_requested && conns.empty() && !drained && listening) {
      listening = false;
      close_listener = [this]() {
        listener->Close([this]() {
          std::lock_guard<std::mutex> lk(mu);
          if (!drained) {
            drained = true;
            cv.notify_all();
          }
        });
      };
    }
  }
  if (close_listener) close_listener();
}

void Server::Impl::FinishShutdown() {
  if (getenv("URPC_WIRE_DEBUG"))
    std::fprintf(stderr, "[dbg] FinishShutdown begin\n");
  std::function<void()> close_listener;
  {
    std::lock_guard<std::mutex> lock(mu);
    if (drained) return;
    for (auto& conn : conns) {
      conn->closed = true;
      for (auto& [sid, call] : conn->calls) {
        conn->FireCancel(call);
      }
      if (conn->socket != nullptr) {
        uv_close(reinterpret_cast<uv_handle_t*>(conn->socket), nullptr);
        conn->socket = nullptr;
      }
    }
    conns.clear();
    if (listening) {
      // close listener on this (loop) thread; drain completes in its
      // close callback so the handle is fully released before teardown
      listening = false;
      close_listener = [this]() {
        listener->Close([this]() {
          std::lock_guard<std::mutex> lk(mu);
          if (!drained) {
            drained = true;
            cv.notify_all();
          }
        });
      };
    } else {
      drained = true;
      cv.notify_all();
    }
  }
  if (close_listener) close_listener();
  log::Info(log::LogCategory::kConnection, "server_stopped",
            "address=" + options.address);
}

// ---- uv trampolines -----------------------------------------------------------
// Conn access hook for the uv read trampoline (keeps Impl private).
struct Server::ConnHook {
  static void OnRead(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
    auto* conn = static_cast<Server::Impl::Conn*>(stream->data);
    if (nread > 0 && conn != nullptr) {
      conn->session->Consume(reinterpret_cast<const uint8_t*>(buf->base),
                             static_cast<size_t>(nread));
    } else if (nread < 0 && conn != nullptr) {
      uv_read_stop(stream);
      if (!conn->closed) {
        conn->closed = true;
        for (auto& [sid, call] : conn->calls) conn->FireCancel(call);
        conn->server->impl_->OnConnIdle();
      }
    }
    delete[] buf->base;
  }
};

// ---- Server --------------------------------------------------------------------
Server::Server(LoopRunner* loop, Router* router, Options options)
    : impl_(new Impl()) {
  impl_->loop = loop;
  impl_->router = router;
  impl_->options = std::move(options);
}

Server::~Server() { Shutdown(); delete impl_; }

Status Server::Start() {
  std::unique_lock<std::mutex> lock(impl_->mu);
  if (impl_->start_done) return impl_->start_status;
  impl_->listener = std::make_unique<platform::TcpListener>();
  auto* listener = impl_->listener.get();
  impl_->loop->Post([this, listener] {
    auto st = listener->BindAndListen(
        impl_->loop->loop(), impl_->options.address,
        [this](uv_stream_t* stream) {
          auto* conn = new Impl::Conn();
          conn->server = this;
          conn->socket = reinterpret_cast<uv_tcp_t*>(stream);
          conn->socket->data = conn;
          conn->session =
              std::make_unique<H2Session>(H2Session::Role::kServer, conn);
          conn->session->Flush();  // initial SETTINGS
          {
            std::lock_guard<std::mutex> lk(impl_->mu);
            impl_->conns.emplace_back(conn);
          }
          uv_read_start(stream, &UvAlloc,
                      &Server::ConnHook::OnRead);
        });
    {
      std::lock_guard<std::mutex> lk(impl_->mu);
      impl_->start_status = st;
      impl_->listening = st.ok();
      impl_->start_done = true;
      impl_->cv.notify_all();
    }
    if (st.ok()) {
      log::Info(log::LogCategory::kConnection, "server_listening",
                "address=" + impl_->options.address);
    }
  });
  impl_->cv.wait(lock, [this] { return impl_->start_done; });
  return impl_->start_status;
}

void Server::Shutdown() {
  std::unique_lock<std::mutex> lock(impl_->mu);
  if (!impl_->start_done) return;
  if (impl_->shutdown_requested) {
    impl_->cv.wait(lock, [this] { return impl_->drained; });
    return;
  }
  impl_->shutdown_requested = true;
  if (getenv("URPC_WIRE_DEBUG"))
    std::fprintf(stderr, "[dbg] Server::Shutdown begin (conns=%zu)\n",
                 impl_->conns.size());
  // All uv handle/timer operations below must run on the loop thread;
  // Shutdown() itself may be called from any thread. drained is set only
  // after the listener handle is FULLY closed (uv_close is async — the
  // handle memory must stay alive until the close callback ran), so the
  // later ~TcpListener (any thread) finds loop_ == nullptr and the uv
  // loop no longer references it.
  impl_->loop->Post([this] {
    std::lock_guard<std::mutex> lk(impl_->mu);
    auto finish = [this]() {
      std::lock_guard<std::mutex> lk2(impl_->mu);
      if (!impl_->drained) {
        impl_->drained = true;
        impl_->cv.notify_all();
      }
    };
    // Graceful drain (FR-012): connections with no in-flight calls close
    // immediately; the grace window protects in-flight work only.
    for (auto it = impl_->conns.begin(); it != impl_->conns.end();) {
      if ((*it)->calls.empty()) {
        (*it)->closed = true;
        if ((*it)->socket != nullptr) {
          uv_close(reinterpret_cast<uv_handle_t*>((*it)->socket), nullptr);
          (*it)->socket = nullptr;
        }
        it = impl_->conns.erase(it);
      } else {
        ++it;
      }
    }
    if (!impl_->conns.empty()) {
      // in-flight work remains: arm the grace timer, drain completes when
      // the last conn goes idle or the grace timer fires
      const uint64_t grace = impl_->options.shutdown_grace_ms;
      if (getenv("URPC_WIRE_DEBUG"))
        std::fprintf(stderr, "[dbg] Shutdown: arming grace timer (%llums)\n",
                     (unsigned long long)grace);
      impl_->loop->SetTimer(grace, [this] { impl_->FinishShutdown(); });
      return;
    }
    if (impl_->listening) {
      impl_->listening = false;
      // conn set already empty: close listener and complete on its close
      // callback (loop thread) so the handle is fully released first
      impl_->listener->Close([finish]() { finish(); });
      return;
    }
    finish();
  });
  impl_->cv.wait(lock, [this] { return impl_->drained; });
  if (getenv("URPC_WIRE_DEBUG"))
    std::fprintf(stderr, "[dbg] Server::Shutdown done (drained)\n");
}

void Server::Wait() {
  std::unique_lock<std::mutex> lock(impl_->mu);
  impl_->cv.wait(lock, [this] { return impl_->drained; });
}

bool Server::IsRunning() const { return !impl_->drained; }

}  // namespace core
}  // namespace urpc
