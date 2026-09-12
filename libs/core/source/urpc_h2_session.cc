#include "urpc/core/h2_session.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <nghttp2/nghttp2.h>

namespace urpc {
namespace core {

// Bodies submitted for sending are kept alive until their stream closes:
// nghttp2 may defer data-provider calls beyond the current Flush (flow
// control), so ownership must be tied to the stream, not the submit call.
struct PendingBody {
  std::string data;
  size_t offset = 0;
};

struct H2Session::Impl {
  Role role = Role::kServer;
  Handler* handler = nullptr;
  nghttp2_session* session = nullptr;
  std::string out;
  std::map<int32_t, HeaderMap> pending_headers;
  std::map<int32_t, std::shared_ptr<PendingBody>> pending_bodies;

  void FlushOut() {
    nghttp2_session_send(session);
    if (getenv("URPC_H2_DEBUG"))
      fprintf(stderr, "[h2][%s] flush out=%zu\n",
              role == Role::kServer ? "server" : "client", out.size());
    if (!out.empty()) {
      handler->OnWrite(reinterpret_cast<const uint8_t*>(out.data()),
                       out.size());
      out.clear();
    }
  }
};

namespace {

nghttp2_ssize SendCb2(nghttp2_session* session, const uint8_t* data,
                      size_t len, int flags, void* user_data) {
  auto* impl = static_cast<H2Session::Impl*>(user_data);
  impl->out.append(reinterpret_cast<const char*>(data), len);
  return static_cast<nghttp2_ssize>(len);
}

int OnHeaderCb(nghttp2_session* session, const nghttp2_frame* frame,
               const uint8_t* name, size_t namelen, const uint8_t* value,
               size_t valuelen, uint8_t flags, void* user_data) {
  auto* impl = static_cast<H2Session::Impl*>(user_data);
  if (frame->hd.type != NGHTTP2_HEADERS) return 0;
  std::string n(reinterpret_cast<const char*>(name), namelen);
  std::string v(reinterpret_cast<const char*>(value), valuelen);
  impl->pending_headers[frame->hd.stream_id][std::move(n)] = std::move(v);
  return 0;
}

int OnFrameRecvCb(nghttp2_session* session, const nghttp2_frame* frame,
                  void* user_data) {
  auto* impl = static_cast<H2Session::Impl*>(user_data);
  const int32_t sid = frame->hd.stream_id;
  const bool end_stream = (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0;

  if (frame->hd.type == NGHTTP2_HEADERS) {
    auto it = impl->pending_headers.find(sid);
    if (it != impl->pending_headers.end()) {
      H2Session::HeaderMap headers = std::move(it->second);
      impl->pending_headers.erase(it);
      impl->handler->OnHeadersComplete(sid, headers, end_stream);
    }
  } else if (frame->hd.type == NGHTTP2_DATA) {
    impl->handler->OnData(sid, nullptr, 0, end_stream);
  }
  return 0;
}

int OnDataChunkCb(nghttp2_session* session, uint8_t flags, int32_t stream_id,
                  const uint8_t* data, size_t len, void* user_data) {
  auto* impl = static_cast<H2Session::Impl*>(user_data);
  impl->handler->OnData(stream_id, data, len, false);
  return 0;
}

int OnStreamCloseCb(nghttp2_session* session, int32_t stream_id,
                    uint32_t error_code, void* user_data) {
  auto* impl = static_cast<H2Session::Impl*>(user_data);
  impl->pending_headers.erase(stream_id);
  impl->pending_bodies.erase(stream_id);
  impl->handler->OnStreamClose(stream_id, error_code);
  return 0;
}

nghttp2_nv MakeNv(const std::string& name, const std::string& value) {
  nghttp2_nv nv;
  nv.name = reinterpret_cast<uint8_t*>(const_cast<char*>(name.data()));
  nv.namelen = name.size();
  nv.value = reinterpret_cast<uint8_t*>(const_cast<char*>(value.data()));
  nv.valuelen = value.size();
  nv.flags = NGHTTP2_NV_FLAG_NONE;
  return nv;
}

nghttp2_ssize BodyRead2(nghttp2_session* session, int32_t stream_id,
                        uint8_t* buf, size_t length, uint32_t* data_flags,
                        nghttp2_data_source* source, void* user_data) {
  auto* body = static_cast<PendingBody*>(source->ptr);
  const size_t remaining = body->data.size() - body->offset;
  if (remaining == 0) {
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    return 0;
  }
  const size_t chunk = remaining < length ? remaining : length;
  memcpy(buf, body->data.data() + body->offset, chunk);
  body->offset += chunk;
  if (body->offset == body->data.size()) {
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
  }
  return static_cast<nghttp2_ssize>(chunk);
}

}  // namespace

H2Session::H2Session(Role role, Handler* handler) : impl_(new Impl()) {
  impl_->role = role;
  impl_->handler = handler;

  nghttp2_session_callbacks* cbs = nullptr;
  nghttp2_session_callbacks_new(&cbs);
  nghttp2_session_callbacks_set_send_callback2(cbs, &SendCb2);
  nghttp2_session_callbacks_set_on_header_callback(cbs, &OnHeaderCb);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, &OnFrameRecvCb);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs,
                                                            &OnDataChunkCb);
  nghttp2_session_callbacks_set_on_stream_close_callback(cbs,
                                                         &OnStreamCloseCb);

  nghttp2_option* opt = nullptr;
  nghttp2_option_new(&opt);
  if (role == Role::kServer) {
    nghttp2_session_server_new2(&impl_->session, cbs, impl_, opt);
  } else {
    nghttp2_session_client_new2(&impl_->session, cbs, impl_, opt);
  }
  nghttp2_option_del(opt);
  nghttp2_session_callbacks_del(cbs);

  // nghttp2 does NOT auto-send the initial SETTINGS frame; submitting it
  // here makes the first Flush() emit the connection preface properly
  // (client: magic + SETTINGS, server: SETTINGS).
  nghttp2_submit_settings(impl_->session, NGHTTP2_FLAG_NONE, nullptr, 0);
}

H2Session::~H2Session() {
  if (impl_->session != nullptr) {
    nghttp2_session_del(impl_->session);
  }
  delete impl_;
}

void H2Session::Consume(const uint8_t* data, size_t len) {
  nghttp2_ssize rc = nghttp2_session_mem_recv2(impl_->session, data, len);
  if (getenv("URPC_H2_DEBUG")) {
    fprintf(stderr, "[h2][%s] consume %zu bytes -> %lld\n",
            impl_->role == Role::kServer ? "server" : "client", len,
            (long long)rc);
  }
  impl_->FlushOut();
}

void H2Session::Flush() { impl_->FlushOut(); }

int32_t H2Session::SubmitRequest(
    const std::vector<std::pair<std::string, std::string>>& headers,
    const std::string& body) {
  std::vector<nghttp2_nv> nvs;
  nvs.reserve(headers.size());
  for (const auto& [n, v] : headers) nvs.push_back(MakeNv(n, v));

  auto pending = std::make_shared<PendingBody>();
  pending->data = body;

  nghttp2_data_provider2 dp2{};
  dp2.source.ptr = pending.get();
  dp2.read_callback = &BodyRead2;

  // submit_request2 returns the new stream id; a NULL provider (empty body)
  // half-closes the stream via END_STREAM on the HEADERS frame.
  int32_t sid = nghttp2_submit_request2(
      impl_->session, nullptr, nvs.data(), nvs.size(),
      body.empty() ? nullptr : &dp2, nullptr);
  if (sid < 0) return -1;
  if (!body.empty()) {
    // The provider may be invoked during the flush below; keep the body
    // alive until the stream closes (flow control may defer delivery).
    impl_->pending_bodies[sid] = pending;
  }
  impl_->FlushOut();
  return sid;
}

void H2Session::SendHeaders(
    int32_t stream_id,
    const std::vector<std::pair<std::string, std::string>>& fields,
    bool end_stream) {
  std::vector<nghttp2_nv> nvs;
  nvs.reserve(fields.size());
  for (const auto& [n, v] : fields) nvs.push_back(MakeNv(n, v));
  int rv = nghttp2_submit_headers(impl_->session,
                         end_stream ? NGHTTP2_FLAG_END_STREAM
                                    : NGHTTP2_FLAG_NONE,
                         stream_id, nullptr, nvs.data(), nvs.size(), nullptr);
  if (getenv("URPC_H2_DEBUG"))
    fprintf(stderr, "[h2] submit_headers sid=%d end=%d rv=%d\n",
            (int)stream_id, end_stream ? 1 : 0, rv);
  impl_->FlushOut();
}

void H2Session::SendData(int32_t stream_id, const std::string& data,
                         bool end_stream) {
  auto pending = std::make_shared<PendingBody>();
  pending->data = data;
  impl_->pending_bodies[stream_id] = pending;
  nghttp2_data_provider2 dp{};
  dp.source.ptr = pending.get();
  dp.read_callback = &BodyRead2;
  nghttp2_submit_data2(impl_->session,
                       end_stream ? NGHTTP2_FLAG_END_STREAM
                                  : NGHTTP2_FLAG_NONE,
                       stream_id, &dp);
  impl_->FlushOut();
}

void H2Session::SendTrailers(int32_t stream_id, const HeaderMap& trailers) {
  std::vector<nghttp2_nv> nvs;
  nvs.reserve(trailers.size());
  for (const auto& [n, v] : trailers) nvs.push_back(MakeNv(n, v));
  nghttp2_submit_trailer(impl_->session, stream_id, nvs.data(), nvs.size());
  impl_->FlushOut();
}

void H2Session::ResetStream(int32_t stream_id, uint32_t error_code) {
  nghttp2_submit_rst_stream(impl_->session, NGHTTP2_FLAG_NONE, stream_id,
                            error_code);
  impl_->FlushOut();
}

}  // namespace core
}  // namespace urpc
