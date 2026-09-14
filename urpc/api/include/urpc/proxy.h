#pragma once

// Helpers for generated service proxies (spec 003-typed-service-interface).
//
// Generated proxies hold a weak reference to the Channel: a destroyed or
// closed channel makes every subsequent proxy call fail immediately with
// UNAVAILABLE (FR-009). Typed sync/async calls funnel through the existing
// Channel entry points (zero extra copy layers, FR-007); proxies are
// stateless beyond the channel reference and therefore safe for concurrent
// use (FR-006).
//
// M (the traits argument) is generated per method by protoc-gen-urpc with
// the same concept as the URPC_UNARY_METHOD structs in urpc/unary.h, so
// generated proxies and hand-written call sites share the exact same
// Channel machinery.

#include <functional>
#include <memory>
#include <string>

#include <upb/mem/arena.h>
#include <upb/wire/decode.h>
#include <upb/wire/encode.h>

#include "urpc/client.h"
#include "urpc/core/status.h"
#include "urpc/detail/raw.h"

namespace urpc {
namespace detail {

template <typename M>
uint64_t ProxyCallAsync(const std::weak_ptr<Channel>& channel,
                        const typename M::ReqType* request,
                        uint64_t timeout_ms,
                        std::function<void(Result<typename M::ResType>)> done) {
  auto ch = channel.lock();
  if (ch == nullptr || ch->closed()) {
    if (done) {
      done(Result<typename M::ResType>(
          Status(StatusCode::kUnavailable, "channel closed"), nullptr));
    }
    return 0;
  }
  return ch->template CallAsync<M>(M::service_name(), M::method_name(),
                                   request, timeout_ms, std::move(done));
}

template <typename M>
Result<typename M::ResType> ProxyCall(const std::weak_ptr<Channel>& channel,
                                      const typename M::ReqType* request,
                                      uint64_t timeout_ms) {
  auto ch = channel.lock();
  if (ch == nullptr || ch->closed()) {
    return Result<typename M::ResType>(
        Status(StatusCode::kUnavailable, "channel closed"), nullptr);
  }
  return ch->template Call<M>(M::service_name(), M::method_name(), request,
                              timeout_ms);
}

}  // namespace detail
}  // namespace urpc
