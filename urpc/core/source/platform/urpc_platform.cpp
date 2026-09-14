#include "urpc/core/platform.h"

#ifdef _WIN32
#include <winsock2.h>  // ntohs (sockaddr types already come via uv.h)
#else
#include <arpa/inet.h>  // ntohs
#endif
#include <cstring>
#include <string>

namespace urpc {
namespace core {
namespace platform {

namespace {

struct ConnectCtx {
  uv_connect_t req;
  std::function<void(Status, uv_stream_t*)> cb;
  bool done = false;
};

void OnConnect(uv_connect_t* req, int status) {
  auto* ctx = static_cast<ConnectCtx*>(req->data);
  if (ctx->done) return;
  ctx->done = true;
  if (status < 0) {
    ctx->cb(Status(StatusCode::kUnavailable, uv_strerror(status)), nullptr);
    delete ctx;
    return;
  }
  ctx->cb(Status::Ok(), reinterpret_cast<uv_stream_t*>(req->handle));
  delete ctx;
}

}  // namespace

Status ParseAddress(const std::string& address, sockaddr_storage* out) {
  const std::string proto = "tcp://";
  std::string a = address;
  if (a.rfind(proto, 0) == 0) a = a.substr(proto.size());

  size_t colon = a.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 >= a.size()) {
    return Status(StatusCode::kInternal,
                  "address must be host:port");
  }
  std::string host = a.substr(0, colon);
  std::string port = a.substr(colon + 1);

  int port_no = 0;
  try {
    port_no = std::stoi(port);
  } catch (...) {
    return Status(StatusCode::kInternal, "bad port: " + port);
  }
  if (port_no < 0 || port_no > 65535) {
    return Status(StatusCode::kInternal, "port out of range: " + port);
  }

  // strip IPv6 brackets
  if (!host.empty() && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }

  int rc = 0;
  if (host.find(':') != std::string::npos) {
    rc = uv_ip6_addr(host.c_str(), port_no,
                     reinterpret_cast<sockaddr_in6*>(out));
  } else {
    rc = uv_ip4_addr(host.c_str(), port_no,
                     reinterpret_cast<sockaddr_in*>(out));
  }
  if (rc != 0) {
    return Status(StatusCode::kInternal,
                  std::string("bad address '") + address +
                      "': " + uv_strerror(rc));
  }
  return Status::Ok();
}

void TcpConnect(uv_loop_t* loop, const std::string& address,
                std::function<void(Status, uv_stream_t*)> cb) {
  sockaddr_storage ss;
  Status st = ParseAddress(address, &ss);
  if (!st.ok()) {
    cb(st, nullptr);
    return;
  }
  auto* tcp = new uv_tcp_t();
  uv_tcp_init(loop, tcp);
  auto* ctx = new ConnectCtx{};
  ctx->req.data = ctx;
  ctx->cb = std::move(cb);
  uv_tcp_nodelay(tcp, 1);  // small RPC frames must not wait on Nagle
  int rc = uv_tcp_connect(&ctx->req, tcp, reinterpret_cast<const sockaddr*>(&ss),
                          &OnConnect);
  if (rc != 0) {
    Status err(StatusCode::kUnavailable, uv_strerror(rc));
    uv_close(reinterpret_cast<uv_handle_t*>(tcp), [](uv_handle_t* h) {
      delete reinterpret_cast<uv_tcp_t*>(h);
    });
    delete ctx;
    cb(err, nullptr);
    return;
  }
}

TcpListener::~TcpListener() { Close(); }

Status TcpListener::BindAndListen(uv_loop_t* loop, const std::string& address,
                                  AcceptCallback on_accept, int backlog) {
  sockaddr_storage ss;
  Status st = ParseAddress(address, &ss);
  if (!st.ok()) return st;

  loop_ = loop;
  on_accept_ = std::move(on_accept);
  uv_tcp_init(loop, &handle_);
  handle_.data = this;
  int rc = uv_tcp_bind(&handle_, reinterpret_cast<const sockaddr*>(&ss), 0);
  if (rc != 0) {
    return Status(StatusCode::kUnavailable, uv_strerror(rc));
  }
  rc = uv_listen(reinterpret_cast<uv_stream_t*>(&handle_), backlog,
                 &TcpListener::OnConnection);
  if (rc != 0) {
    return Status(StatusCode::kUnavailable, uv_strerror(rc));
  }
  return Status::Ok();
}

void TcpListener::OnConnection(uv_stream_t* server, int status) {
  auto* self = static_cast<TcpListener*>(server->data);
  if (status < 0 || !self->on_accept_) return;
  auto* client = new uv_tcp_t();
  uv_tcp_init(server->loop, client);
  int rc = uv_accept(server, reinterpret_cast<uv_stream_t*>(client));
  if (rc == 0) {
    uv_tcp_nodelay(client, 1);  // latency over batching for RPC frames
  }
  if (rc != 0) {
    uv_close(reinterpret_cast<uv_handle_t*>(client), [](uv_handle_t* h) {
      delete reinterpret_cast<uv_tcp_t*>(h);
    });
    return;
  }
  self->on_accept_(reinterpret_cast<uv_stream_t*>(client));
}

uint16_t TcpListener::bound_port() const {
  sockaddr_storage ss{};
  int len = sizeof(ss);
  if (uv_tcp_getsockname(&handle_, reinterpret_cast<sockaddr*>(&ss), &len) !=
      0) {
    return 0;
  }
  if (ss.ss_family == AF_INET6) {
    return ntohs(reinterpret_cast<const sockaddr_in6*>(&ss)->sin6_port);
  }
  return ntohs(reinterpret_cast<const sockaddr_in*>(&ss)->sin_port);
}

void TcpListener::OnClosedTramp(uv_handle_t* handle) {
  // Runs on the loop thread after the handle is fully closed.
  auto* self = static_cast<TcpListener*>(handle->data);
  if (self->on_closed_) {
    auto cb = std::move(self->on_closed_);
    self->on_closed_ = nullptr;
    cb();
  }
}

void TcpListener::Close(std::function<void()> on_closed) {
  if (loop_ == nullptr) {
    if (on_closed) on_closed();  // already closed: complete immediately
    return;
  }
  on_closed_ = std::move(on_closed);
  if (!uv_is_closing(reinterpret_cast<uv_handle_t*>(&handle_))) {
    uv_close(reinterpret_cast<uv_handle_t*>(&handle_), &TcpListener::OnClosedTramp);
  }
  loop_ = nullptr;
}

}  // namespace platform
}  // namespace core
}  // namespace urpc
