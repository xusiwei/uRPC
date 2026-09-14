#pragma once

// Internal bridge between the header-inline typed API templates and the
// hidden implementation (urpc_api_impl.cpp). Not part of the public surface.

#include <functional>
#include <string>

#include "urpc/core/server.h"  // StreamCallCtx
#include "urpc/core/status.h"
#include "urpc/server.h"
#include "urpc/service.h"  // MethodForm

namespace urpc {

class Channel;

namespace detail {

using RawDone = std::function<void(::urpc::Status, const std::string&)>;

// Server: registers a bytes-level unary handler under "/service/method".
::urpc::Status RegisterUnaryRaw(
    Server* server, const std::string& service, const std::string& method,
    std::function<void(ServerContext&, const std::string& request, RawDone)>
        raw_handler);

// Channel: framed async unary call. done fires exactly once.
uint64_t ChannelCallRaw(Channel* channel, const std::string& path,
                        const std::string& framed_request, uint64_t timeout_ms,
                        std::function<void(::urpc::Status, std::string)> done);

// True when the caller is on a urpc event-loop thread (sync calls must
// fast-fail there — FR-002).
bool ChannelOnLoopThread(Channel* channel);

// ---- streaming bridges (spec 004) -------------------------------------------

// Registers a streaming handler under "/service/method". `body` runs on
// the loop thread once per stream; ServerContext wraps the same core call
// context (cancellation, deadline).
Status RegisterStreamRaw(
    Server* server, const std::string& service, const std::string& method,
    MethodForm form,
    std::function<void(ServerContext&, core::StreamCallCtx&)> body);

// Client streaming primitives over a Channel (spec 004 contracts §4).
// Returns the stream handle (0 on failure to post).
uint64_t ChannelOpenStreamRaw(
    Channel* channel, const std::string& path,
    std::function<void(Status, std::string)> on_message,
    std::function<void(Status)> on_complete, uint64_t timeout_ms);
// Sends one framed message; on_flushed fires on delivery (or failure).
void ChannelStreamSendRaw(Channel* channel, uint64_t stream_id,
                          const std::string& framed_message,
                          std::function<void(Status)> on_flushed, bool close);
// Half-closes the request side.
void ChannelStreamCloseSendRaw(Channel* channel, uint64_t stream_id);
// True when the caller is on the channel's event-loop thread.
bool ChannelOnLoopThreadRaw(Channel* channel);

}  // namespace detail
}  // namespace urpc
