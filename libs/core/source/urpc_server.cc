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
    bool responded = false;      // terminal state sent (unary or Finish)
    bool request_done = false;   // unary: full request received
    bool cancel_fired = false;
    std::vector<std::function<void()>> cancel_cbs;
    // ---- streaming (spec 004) ----
    MethodForm form = MethodForm::kUnary;
    HandlerEntry entry;         // valid when has_handler
    bool has_handler = false;
    bool dispatched = false;     // streaming handler invoked
    bool request_half_closed = false;
    bool read_finished = false;  // eos/error already delivered
    std::function<void(Status, bool, std::string)> read_cb;
    std::deque<std::string> pending_msgs;   // decoded, not yet read
    struct OutMsg {
      std::string framed;
      std::function<void(Status)> cb;
    };
    std::deque<OutMsg> out_queue;           // bound: 64 (FR-008)
    bool out_inflight = false;
    bool response_headers_sent = false;
    bool write_closed = false;
    Status finish_pending;                  // deferred: send after drain
    bool has_finish_pending = false;
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
    if (getenv("URPC_WIRE_DEBUG"))
      fprintf(stderr, "[srv] OnHeadersComplete sid=%d end=%d path=%s\n",
              (int)sid, (int)end_stream, pt->second.c_str());
    Call& call = calls[sid];
    call.path = pt->second;
    call.started_ms = server->impl_->loop->NowMs();
    call.decoder =
        std::make_unique<FrameDecoder>(server->impl_->options.max_receive_size);
    auto entry = server->impl_->router->Find(call.path);
    call.has_handler = entry.has_value();
    if (call.has_handler) {
      call.form = entry->form;
      call.entry = std::move(*entry);
    }

    auto to = headers.find("grpc-timeout");
    if (to != headers.end()) {
      const uint64_t ms = ParseGrpcTimeout(to->second);
      if (ms > 0) {
        call.deadline_timer = server->impl_->loop->SetTimer(
            ms, [this, sid] { OnDeadline(sid); });
      }
    }
    // Client-streaming and bidi handlers start at stream open so they can
    // read while messages arrive (research.md D2). Server-streaming and
    // unary dispatch after their single request message (FinishRequest /
    // first message below).
    if (call.has_handler &&
        (call.form == MethodForm::kClientStreaming ||
         call.form == MethodForm::kBidi)) {
      DispatchStream(sid, nullptr);
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
    if (getenv("URPC_WIRE_DEBUG"))
      fprintf(stderr, "[srv] OnData sid=%d len=%zu end=%d msgs=%zu\n",
              (int)sid, len, (int)end_stream, call.decoder->message_count());
    if (call.form != MethodForm::kUnary) {
      while (call.decoder->HasMessage()) {
        std::string msg = call.decoder->TakeMessage();
        if (call.form == MethodForm::kServerStreaming) {
          if (call.dispatched) {
            FinishStream(sid, Status(StatusCode::kInternal,
                                     "multiple request messages"));
            return;
          }
          DispatchStream(sid, &msg);
        } else {
          DeliverRead(sid, Status::Ok(), false, std::move(msg));
        }
      }
    }
    if (end_stream) FinishRequest(sid);
  }

  void OnStreamClose(int32_t sid, uint32_t) override {
    auto it = calls.find(sid);
    if (it != calls.end()) {
      CancelStreamIo(sid);
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

    if (!call.has_handler) {
      if (getenv("URPC_WIRE_DEBUG"))
        fprintf(stderr, "[srv] no handler for %s\n", call.path.c_str());
      TrailersOnly(sid, StatusCode::kUnimplemented,
                   "unknown method: " + call.path);
      return;
    }
    if (call.form == MethodForm::kUnary) {
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
      call.entry.unary(*raw, request);
      return;
    }
    // ---- streaming forms: END_STREAM completes the request side ----
    call.request_half_closed = true;
    if (call.form == MethodForm::kServerStreaming) {
      if (!call.dispatched) {
        if (!call.decoder->HasMessage()) {
          TrailersOnly(sid, StatusCode::kInternal, "missing request message");
          return;
        }
        std::string msg = call.decoder->TakeMessage();
        DispatchStream(sid, &msg);
        return;
      }
      DeliverRead(sid, Status::Ok(), true, std::string());
      return;
    }
    // client-streaming / bidi: handler dispatched at stream open
    if (!call.dispatched) {
      TrailersOnly(sid, StatusCode::kUnimplemented,
                   "unknown method: " + call.path);
      return;
    }
    DeliverRead(sid, Status::Ok(), true, std::string());
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
    if (it->second.responded) return;
    it->second.responded = true;
    it->second.deadline_timer = 0;
    if (it->second.form != MethodForm::kUnary &&
        it->second.response_headers_sent) {
      // mid-stream deadline: trailers after already-started responses
      session->SendTrailers(
          sid, {{"grpc-status",
                 std::to_string(static_cast<int>(
                     StatusCode::kDeadlineExceeded))},
                {"grpc-message", "deadline exceeded"}});
      log::Info(log::LogCategory::kCall, "call_end",
                "sid=" + std::to_string(sid) +
                    " status=DEADLINE_EXCEEDED");
      return;
    }
    SendTrailersOnly(sid, StatusCode::kDeadlineExceeded,
                     "deadline exceeded");
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

  // ---- streaming dispatch + read/write machinery (spec 004) --------------

  // Invokes the streaming handler once per stream. `first_msg` carries the
  // single request message for server-streaming, nullptr otherwise.
  void DispatchStream(int32_t sid, const std::string* first_msg) {
    auto it = calls.find(sid);
    if (it == calls.end() || it->second.dispatched) return;
    Call& call = it->second;
    call.dispatched = true;
    auto ctx = std::make_unique<StreamCtx>(this, sid);
    StreamCtx* raw = ctx.get();
    if (first_msg != nullptr) raw->stash_first_message(*first_msg);
    ctxs[sid] = std::move(ctx);
    if (getenv("URPC_WIRE_DEBUG"))
      fprintf(stderr, "[srv] dispatch stream sid=%d form=%d\n", (int)sid,
              (int)call.form);
    call.entry.stream(*raw);
  }

  // Read-side fan-out (contracts §4): at most one pending callback;
  // undelivered decoded messages queue bounded (RESOURCE_EXHAUSTED beyond
  // 64 keeps memory bounded, FR-008).
  void DeliverRead(int32_t sid, Status st, bool eos, std::string msg) {
    auto it = calls.find(sid);
    if (it == calls.end()) return;
    Call& call = it->second;
    if (call.read_finished) return;
    if (!st.ok() || eos) {
      call.read_finished = true;
      if (call.read_cb) {
        auto cb = std::move(call.read_cb);
        call.read_cb = nullptr;
        cb(st, eos, std::move(msg));
      }
      return;
    }
    if (call.read_cb) {
      auto cb = std::move(call.read_cb);
      call.read_cb = nullptr;
      cb(st, false, std::move(msg));
      return;
    }
    if (call.pending_msgs.size() >= 64) {
      FinishStream(sid, Status(StatusCode::kResourceExhausted,
                               "too many unread request messages"));
      return;
    }
    call.pending_msgs.push_back(std::move(msg));
  }

  // Write-side pump (research.md D4): one in-flight DATA provider per
  // stream; the next queued message is handed to nghttp2 after the
  // previous body was fully consumed (backpressure boundary, FR-008;
  // bound = 2 queued + 1 in flight).
  void PumpOut(int32_t sid) {
    auto it = calls.find(sid);
    if (it == calls.end()) return;
    Call& call = it->second;
    if (call.out_inflight || call.out_queue.empty() || call.responded ||
        call.write_closed) {
      return;
    }
    Call::OutMsg out = std::move(call.out_queue.front());
    call.out_queue.pop_front();
    call.out_inflight = true;
    if (!call.response_headers_sent) {
      call.response_headers_sent = true;
      session->SendHeaders(
          sid, {{":status", "200"}, {"content-type", kContentType}}, false);
    }
    session->SendData(sid, out.framed, false,
                      [this, sid, cb = std::move(out.cb)]() mutable {
                        auto it2 = calls.find(sid);
                        if (it2 != calls.end())
                          it2->second.out_inflight = false;
                        if (cb) cb(Status::Ok());
                        PumpOut(sid);
                        MaybeSendDeferredFinish(sid);
                      });
  }

  // Sends trailers once the response queue fully drained (Finish called
  // while messages were still queued: Finish ends the stream AFTER the
  // queued messages, never dropping them).
  void MaybeSendDeferredFinish(int32_t sid) {
    auto it = calls.find(sid);
    if (it == calls.end()) return;
    Call& call = it->second;
    if (!call.has_finish_pending) return;
    if (call.out_inflight || !call.out_queue.empty()) return;
    call.has_finish_pending = false;
    session->SendTrailers(
        sid, {{"grpc-status",
               std::to_string(static_cast<int>(
                   call.finish_pending.code()))},
              {"grpc-message", call.finish_pending.message()}});
    log::Info(log::LogCategory::kCall, "call_end",
              "path=" + call.path + " sid=" + std::to_string(sid) +
                  " status=" + StatusCodeName(call.finish_pending.code()));
  }

  // Terminal for streaming calls: exactly once. Trailers-only when no
  // response headers went out yet; otherwise trailers (both half-close
  // the stream).
  void FinishStream(int32_t sid, Status st) {
    auto it = calls.find(sid);
    if (it == calls.end()) return;
    Call& call = it->second;
    if (call.responded) return;
    call.responded = true;
    call.write_closed = true;
    call.read_finished = true;
    StopDeadline(call);
    // Queued response messages go out first; trailers follow once the
    // queue drains (or immediately when nothing is outstanding).
    if (call.out_inflight || !call.out_queue.empty()) {
      call.finish_pending = st;
      call.has_finish_pending = true;
      return;
    }
    const uint64_t dur = server->impl_->loop->NowMs() - call.started_ms;
    if (!call.response_headers_sent) {
      SendTrailersOnly(sid, st.code(), st.message());
      (void)dur;
      return;
    }
    session->SendTrailers(sid,
                          {{"grpc-status",
                            std::to_string(static_cast<int>(st.code()))},
                           {"grpc-message", st.message()}});
    log::Info(log::LogCategory::kCall, "call_end",
              "path=" + call.path + " sid=" + std::to_string(sid) +
                  " status=" + StatusCodeName(st.code()) +
                  " us=" + std::to_string(dur));
  }

  void CancelStreamIo(int32_t sid) {
    auto it = calls.find(sid);
    if (it == calls.end()) return;
    Call& call = it->second;
    call.read_finished = true;
    if (call.read_cb) {
      auto cb = std::move(call.read_cb);
      call.read_cb = nullptr;
      cb(Status(StatusCode::kUnavailable, "stream cancelled"), false,
         std::string());
    }
    while (!call.out_queue.empty()) {
      auto cb = std::move(call.out_queue.front().cb);
      call.out_queue.pop_front();
      if (cb) cb(Status(StatusCode::kUnavailable, "stream cancelled"));
    }
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

  // ---- streaming ctx view (spec 004) -----------------------------------------
  class StreamCtx : public StreamCallCtx {
   public:
    using ReadCb = std::function<void(Status, bool, std::string)>;
    using WriteCb = std::function<void(Status)>;

    StreamCtx(Conn* conn, int32_t sid) : conn_(conn), sid_(sid) {}

    void stash_first_message(std::string msg) {
      first_msg_ = std::move(msg);
      has_first_msg_ = true;
    }

    void ReadMessage(ReadCb cb) override {
      auto it = conn_->calls.find(sid_);
      if (it == conn_->calls.end() || it->second.read_finished) {
        cb(Status(StatusCode::kUnavailable, "stream closed"),
           it != conn_->calls.end() && it->second.request_half_closed,
           std::string());
        return;
      }
      Call& call = it->second;
      if (has_first_msg_) {
        has_first_msg_ = false;
        cb(Status::Ok(), false, std::move(first_msg_));
        return;
      }
      if (!call.pending_msgs.empty()) {
        std::string m = std::move(call.pending_msgs.front());
        call.pending_msgs.pop_front();
        cb(Status::Ok(), false, std::move(m));
        return;
      }
      if (call.request_half_closed) {
        call.read_finished = true;
        cb(Status::Ok(), true, std::string());
        return;
      }
      if (call.read_cb) {
        cb(Status(StatusCode::kInternal, "concurrent read"), false,
           std::string());
        return;
      }
      call.read_cb = std::move(cb);
    }

    void WriteMessage(std::string msg, WriteCb cb) override {
      auto it = conn_->calls.find(sid_);
      if (it == conn_->calls.end()) {
        if (cb) cb(Status(StatusCode::kUnavailable, "stream closed"));
        return;
      }
      Call& call = it->second;
      if (call.responded || call.write_closed) {
        if (cb) cb(Status(StatusCode::kInternal, "stream finished"));
        return;
      }
      if (call.out_queue.size() >= 64) {
        if (cb) cb(Status(StatusCode::kResourceExhausted, "send queue full"));
        return;
      }
      std::string framed;
      EncodeFrame(msg, &framed);
      call.out_queue.push_back(Call::OutMsg{std::move(framed), std::move(cb)});
      conn_->PumpOut(sid_);
    }

    void WriteDone() override {
      auto it = conn_->calls.find(sid_);
      if (it != conn_->calls.end()) it->second.write_closed = true;
    }

    void Finish(Status st) override { conn_->FinishStream(sid_, st); }
    // Unary-shaped terminal is never used on streaming forms; map it to
    // Finish so the one-shot guard stays in one place.
    Status Respond(Status status, const std::string& payload) override {
      (void)payload;
      conn_->FinishStream(sid_, status);
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
      auto it = conn_->calls.find(sid_);
      if (it == conn_->calls.end() || it->second.cancel_fired) return 0;
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
    std::string first_msg_;
    bool has_first_msg_ = false;
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
        for (auto& [sid, call] : conn->calls) {
          conn->CancelStreamIo(sid);
          conn->FireCancel(call);
        }
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
