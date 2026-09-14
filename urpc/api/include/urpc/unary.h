#pragma once

// Typed unary API over upb-generated message types.
//
// Usage: declare method traits once per (file, message pair):
//   URPC_UNARY_METHOD(Echo, example__echo, EchoRequest, EchoResponse)
//   server.RegisterUnaryFor<Echo>("example.EchoService", "Echo", handler);
// where `example__echo` is the upb generated symbol prefix
// (<package>__<file>).

#include <functional>
#include <string>

#include <upb/mem/arena.h>
#include <upb/message/message.h>
#include <upb/wire/encode.h>


#include "urpc/core/status.h"
#include "urpc/detail/raw.h"
#include "urpc/server.h"

namespace urpc {

// Public alias: the canonical Status type (urpc::core::Status).
using Status = ::urpc::core::Status;
using StatusCode = ::urpc::core::StatusCode;

#define URPC_UNARY_METHOD(METHOD, TYPE_PREFIX, TABLE_PREFIX, REQ_TYPE,       \
                          RES_TYPE)                                          \
                                          \
  struct METHOD {                                                            \
    using ReqType = TYPE_PREFIX##_##REQ_TYPE;                                \
    using ResType = TYPE_PREFIX##_##RES_TYPE;                                \
    static ReqType* ParseRequest(const char* buf, size_t size,               \
                                 upb_Arena* arena) {                         \
      return TYPE_PREFIX##_##REQ_TYPE##_parse(buf, size, arena);             \
    }                                                                        \
    static ResType* ParseResponse(const char* buf, size_t size,              \
                                  upb_Arena* arena) {                        \
      return TYPE_PREFIX##_##RES_TYPE##_parse(buf, size, arena);             \
    }                                                                        \
    static const upb_MiniTable* ReqTable() {                                 \
      return &TABLE_PREFIX##REQ_TYPE##_msg_init;                         \
    }                                                                        \
    static const upb_MiniTable* ResTable() {                                 \
      return &TABLE_PREFIX##RES_TYPE##_msg_init;                          \
    }                                                                        \
  };

// Server side: typed registration (header-inline; heavy lifting via detail).
template <typename M>
::urpc::Status Server::RegisterUnaryFor(const std::string& service,
                                        const std::string& method,
                                        UnaryHandlerFn<M> handler) {
  auto raw = [handler](ServerContext& ctx, const std::string& bytes,
                       detail::RawDone done) {
    upb_Arena* arena = upb_Arena_New();
    if (arena == nullptr) {
      done(Status(StatusCode::kInternal, "arena alloc failed"), std::string());
      return;
    }
    const typename M::ReqType* req =
        M::ParseRequest(bytes.data(), bytes.size(), arena);
    if (req == nullptr) {
      upb_Arena_Free(arena);
      done(Status(StatusCode::kDataLoss, "request decode failed"),
           std::string());
      return;
    }
    auto typed_done = [done, arena](Status st, const typename M::ResType* res) {
      if (!st.ok() || res == nullptr) {
        done(st, std::string());
        upb_Arena_Free(arena);
        return;
      }
      char* buf = nullptr;
      size_t n = 0;
      upb_EncodeStatus es = upb_Encode(reinterpret_cast<const upb_Message*>(res),
                                       M::ResTable(), 0, arena, &buf, &n);
      if (es != kUpb_EncodeStatus_Ok) {
        done(Status(StatusCode::kInternal, "response encode failed"),
             std::string());
        upb_Arena_Free(arena);
        return;
      }
      done(st, std::string(buf, n));  // copied downstream before arena free
      upb_Arena_Free(arena);
    };
    handler(ctx, req, std::move(typed_done));
  };
  return detail::RegisterUnaryRaw(this, service, method, std::move(raw));
}

// Client side: encode the request and dispatch (implementation in client.h).
// (declared there to keep one include graph per class)

}  // namespace urpc
