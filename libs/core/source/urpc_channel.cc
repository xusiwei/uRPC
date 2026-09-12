#include "urpc/core/channel.h"

#include <atomic>
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
constexpr uint32_t kH2Cancel = 0x8;

// Debug logging helper (existing repo convention: URPC_WIRE_DEBUG env var).
inline bool WireDebug() { return getenv("URPC_WIRE_DEBUG") != nullptr; }
#define URPC_DBG(...)                          \
  do {                                         \
    if (WireDebug()) {                         \
      std::fprintf(stderr, "[ch] ");           \
      std::fprintf(stderr, __VA_ARGS__);       \
      std::fprintf(stderr, "\n");              \
    }                                          \
  } while (0)

void UvAlloc(uv_handle_t*, size_t suggested, uv_buf_t* buf) {
  buf->base = new char[suggested];
  buf->len = suggested;
}

void UvWriteCb(uv_write_t* req, int) {
  delete[] static_cast<char*>(req->data);
  delete req;
}

StatusCode GrpcStatusFromInt(const std::string& v, StatusCode fallback) {
  try {
    return static_cast<StatusCode>(std::stoi(v));
  } catch (...) {
    return fallback;
  }
}
}  // namespace

struct Channel::Impl : public H2Session::Handler {
  LoopRunner* loop;
  Options options;

  // connection lifecycle — loop thread only
  enum class State { kIdle, kConnecting, kReady, kBroken };
  State state = State::kIdle;
  uv_tcp_t* socket = nullptr;
  std::unique_ptr<H2Session> session;

  struct Call {
    uint64_t id = 0;
    int32_t sid = 0;
    uint64_t timeout_timer = 0;
    std::unique_ptr<FrameDecoder> decoder;
    std::function<void(Status, std::string)> done;
    bool completed = false;
  };

  struct PendingCall {
    std::string path;
    std::string framed;
    Call call;
  };

  std::map<uint64_t, Call> calls;         // call id → call (loop thread)
  std::map<int32_t, uint64_t> by_stream;  // stream id → call id
  std::deque<PendingCall> pending;        // waiting for the connection
  std::atomic<uint64_t> next_call_id{1};

  void OnHeadersComplete(int32_t sid, const H2Session::HeaderMap& headers,
                         bool) override {
    auto it = by_stream.find(sid);
    if (it == by_stream.end()) return;
    auto cit = calls.find(it->second);
    if (cit == calls.end()) return;

    auto gs = headers.find("grpc-status");
    if (gs != headers.end()) {
      // terminal header block (trailers, or trailers-only error response):
      // deliver the decoded response message together with the status
      std::string msg;
      auto gm = headers.find("grpc-message");
      if (gm != headers.end()) msg = gm->second;
      std::string payload;
      if (cit->second.decoder != nullptr &&
          cit->second.decoder->HasMessage()) {
        payload = cit->second.decoder->TakeMessage();
      }
      Complete(cit->second,
               Status(GrpcStatusFromInt(gs->second, StatusCode::kInternal), msg),
               payload);
      return;
    }
    auto st = headers.find(":status");
    if (st == headers.end() || st->second != "200") {
      Complete(cit->second,
               Status(StatusCode::kUnavailable, "unexpected http status"),
               std::string());
    }
  }

  void OnData(int32_t sid, const uint8_t* data, size_t len, bool) override {
    auto it = by_stream.find(sid);
    if (it == by_stream.end()) return;
    auto cit = calls.find(it->second);
    if (cit == calls.end() || cit->second.completed) return;
    if (data != nullptr && len > 0 && cit->second.decoder) {
      Status st = cit->second.decoder->Consume(data, len);
      if (!st.ok()) Complete(cit->second, st, std::string());
    }
  }

  void OnStreamClose(int32_t sid, uint32_t) override {
    auto sit = by_stream.find(sid);
    if (sit == by_stream.end()) return;
    auto cit = calls.find(sit->second);
    if (cit != calls.end() && !cit->second.completed) {
      // stream closed before trailers: connection-level failure
      Complete(cit->second, Status(StatusCode::kUnavailable, "stream closed"),
               std::string());
    }
    by_stream.erase(sit);
  }

  void OnWrite(const uint8_t* data, size_t len) override {
    if (socket == nullptr) return;
    auto* req = new uv_write_t();
    auto* copy = new char[len];
    memcpy(copy, data, len);
    req->data = copy;
    uv_buf_t b = uv_buf_init(copy, static_cast<unsigned>(len));
    uv_write(req, reinterpret_cast<uv_stream_t*>(socket), &b, 1, &UvWriteCb);
  }

  void Complete(Call& call, Status status, std::string payload) {
    if (call.completed) return;
    call.completed = true;
    if (call.timeout_timer != 0) {
      loop->CancelTimer(call.timeout_timer);
      call.timeout_timer = 0;
    }
    if (call.sid != 0) by_stream.erase(call.sid);
    URPC_DBG("complete call=%llu sid=%d status=%d ms=%llu",
             (unsigned long long)call.id, (int)call.sid, (int)status.code(),
             (unsigned long long)loop->NowMs());
    auto done = std::move(call.done);  // move out BEFORE erasing the entry
    calls.erase(call.id);
    if (done) done(status, std::move(payload));
  }

  void CallNow(const std::string& path, const std::string& framed, Call call) {
    URPC_DBG("submit call=%llu path=%s bytes=%zu ms=%llu",
             (unsigned long long)call.id, path.c_str(), framed.size(),
             (unsigned long long)loop->NowMs());
    int32_t sid = session->SubmitRequest(
        {{":method", "POST"},
         {":scheme", "http"},
         {":path", path},
         {":authority", options.address},
         {"content-type", kContentType},
         {"te", "trailers"}},
        framed);
    if (sid < 0) {
      Complete(call, Status(StatusCode::kUnavailable, "submit failed"),
               std::string());
      return;
    }
    call.sid = sid;
    by_stream[sid] = call.id;
    calls[call.id] = std::move(call);
  }

  void FailPending(const Status& st) {
    while (!pending.empty()) {
      PendingCall pc = std::move(pending.front());
      pending.pop_front();
      Complete(pc.call, st, std::string());
    }
  }

  void StartPending() {
    while (!pending.empty()) {
      PendingCall pc = std::move(pending.front());
      pending.pop_front();
      CallNow(pc.path, pc.framed, std::move(pc.call));
    }
  }

  void ConnectNow() {
    state = State::kConnecting;
    URPC_DBG("connecting address=%s", options.address.c_str());
    platform::TcpConnect(
        loop->loop(), options.address,
        [this](Status st, uv_stream_t* stream) {
          if (!st.ok()) {
            state = State::kBroken;
            URPC_DBG("connect failed: %s", st.message().c_str());
            log::Warn(log::LogCategory::kConnection, "channel_connect_failed",
                      "address=" + options.address + " err=" + st.message());
            FailPending(st);
            return;
          }
          URPC_DBG("connected");
          socket = reinterpret_cast<uv_tcp_t*>(stream);
          socket->data = this;
          session = std::make_unique<H2Session>(H2Session::Role::kClient, this);
          session->Flush();  // magic + SETTINGS
          uv_read_start(reinterpret_cast<uv_stream_t*>(socket), &UvAlloc,
                        [](uv_stream_t* stream, ssize_t nread,
                           const uv_buf_t* buf) {
                          auto* impl =
                              static_cast<Channel::Impl*>(stream->data);
                          if (nread > 0 && impl != nullptr) {
                            impl->session->Consume(
                                reinterpret_cast<const uint8_t*>(buf->base),
                                static_cast<size_t>(nread));
                          }
                          delete[] buf->base;
                          if (nread < 0 && impl != nullptr) {
                            uv_read_stop(stream);
                            impl->OnConnectionLost();
                          }
                        });
          state = State::kReady;
          log::Info(log::LogCategory::kConnection, "channel_connected",
                    "address=" + options.address);
          StartPending();
        });
  }

  void OnConnectionLost() {
    if (state == State::kBroken) return;
    state = State::kBroken;
    URPC_DBG("connection lost");
    if (socket != nullptr) {
      uv_close(reinterpret_cast<uv_handle_t*>(socket), nullptr);
      socket = nullptr;
    }
    session.reset();
    auto snapshot = std::move(calls);
    calls.clear();
    by_stream.clear();
    const Status lost(StatusCode::kUnavailable, "connection lost");
    for (auto& [id, call] : snapshot) {
      if (!call.completed) {
        call.completed = true;
        if (call.timeout_timer != 0) loop->CancelTimer(call.timeout_timer);
        if (call.done) call.done(lost, std::string());
      }
    }
    FailPending(lost);
    log::Warn(log::LogCategory::kConnection, "channel_lost",
              "address=" + options.address);
  }
};

Channel::Channel(LoopRunner* loop, Options options) : impl_(new Impl()) {
  impl_->loop = loop;
  impl_->options = std::move(options);
}
Channel::~Channel() { delete impl_; }

uint64_t Channel::Call(const std::string& path,
                       const std::string& framed_request, uint64_t timeout_ms,
                       std::function<void(Status, std::string)> done) {
  Impl::Call call;
  call.id = impl_->next_call_id.fetch_add(1);
  call.done = std::move(done);
  call.decoder =
      std::make_unique<FrameDecoder>(impl_->options.max_receive_size);

  auto call_ptr = std::make_shared<Impl::Call>(std::move(call));
  impl_->loop->Post([this, id = call_ptr->id, path, framed_request, timeout_ms,
                     call_ptr]() mutable {
    Impl::Call& call = *call_ptr;
    if (timeout_ms > 0) {
      call.timeout_timer = impl_->loop->SetTimer(timeout_ms, [this, id] {
        auto it = impl_->calls.find(id);
        if (it == impl_->calls.end()) return;
        Impl::Call& c = it->second;
        if (c.completed) return;
        URPC_DBG("timeout fired call=%llu sid=%d ms=%llu",
                 (unsigned long long)id, (int)c.sid,
                 (unsigned long long)impl_->loop->NowMs());
        c.completed = true;
        if (c.sid != 0 && impl_->session) {
          impl_->session->ResetStream(c.sid, kH2Cancel);
          impl_->by_stream.erase(c.sid);
        }
        auto done_cb = std::move(c.done);
        impl_->calls.erase(it);
        if (done_cb) {
          done_cb(Status(StatusCode::kDeadlineExceeded, "deadline exceeded"),
                  std::string());
        }
      });
    }
    switch (impl_->state) {
      case Impl::State::kReady:
        impl_->CallNow(path, framed_request, std::move(call));
        break;
      case Impl::State::kBroken:
        impl_->Complete(call, Status(StatusCode::kUnavailable, "channel broken"),
                        std::string());
        break;
      default:
        Impl::PendingCall pc;
        pc.path = path;
        pc.framed = framed_request;
        pc.call = std::move(call);
        impl_->pending.push_back(std::move(pc));
        if (impl_->state == Impl::State::kIdle) impl_->ConnectNow();
        break;
    }
  });
  return call.id;
}

void Channel::Cancel(uint64_t call_id) {
  impl_->loop->Post([this, call_id] {
    auto it = impl_->calls.find(call_id);
    if (it == impl_->calls.end()) return;
    Impl::Call& call = it->second;
    if (call.completed) return;
    call.completed = true;
    if (call.timeout_timer != 0) impl_->loop->CancelTimer(call.timeout_timer);
    auto done = std::move(call.done);
    if (call.sid != 0 && impl_->session) {
      impl_->session->ResetStream(call.sid, kH2Cancel);
      impl_->by_stream.erase(call.sid);
    }
    impl_->calls.erase(it);
    if (done) done(Status(StatusCode::kUnavailable, "cancelled"), std::string());
  });
}

void Channel::set_max_receive_size(size_t n) {
  impl_->options.max_receive_size = n;
}

bool Channel::OnLoopThread() const { return impl_->loop->OnLoopThread(); }

}  // namespace core
}  // namespace urpc
