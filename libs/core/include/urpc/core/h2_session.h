#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace urpc {
namespace core {

// Thin wrapper over an nghttp2 session (server or client role). Pure
// in-memory framing layer: bytes in via Consume(), bytes out via the
// WriteCallback. No sockets here (transport binding is the owner's job).
//
// Header pairs are buffered per header-block and delivered on
// OnHeadersComplete (map field-name → value; later duplicates overwrite).
class H2Session {
 public:
  using HeaderMap = std::map<std::string, std::string>;

  // Events fire on the thread that drives Consume()/Flush().
  class Handler {
   public:
    virtual ~Handler() = default;
    virtual void OnHeadersComplete(int32_t stream_id,
                                   const HeaderMap& headers,
                                   bool end_stream) = 0;
    virtual void OnData(int32_t stream_id, const uint8_t* data, size_t len,
                        bool end_stream) = 0;
    virtual void OnStreamClose(int32_t stream_id, uint32_t error_code) = 0;
    virtual void OnWrite(const uint8_t* data, size_t len) = 0;
  };

  enum class Role { kServer, kClient };

  H2Session(Role role, Handler* handler);
  ~H2Session();
  H2Session(const H2Session&) = delete;
  H2Session& operator=(const H2Session&) = delete;

  // Feed transport bytes into the session, then flush pending output.
  void Consume(const uint8_t* data, size_t len);

  // Push any session output to Handler::OnWrite.
  void Flush();

  // --- client side ---------------------------------------------------------
  // Submits a request (headers + optional single DATA with END_STREAM).
  // `pseudo` must contain :method/:path/etc. Returns the stream id or -1.
  int32_t SubmitRequest(const std::vector<std::pair<std::string, std::string>>&
                            pseudo_and_headers,
                        const std::string& body);

  // --- both roles ----------------------------------------------------------
  void SendHeaders(int32_t stream_id,
                   const std::vector<std::pair<std::string, std::string>>&
                       fields,      // response fields (no pseudo path needed)
                   bool end_stream);
  void SendData(int32_t stream_id, const std::string& data, bool end_stream);
  // Trailers close the stream (grpc-status / grpc-message).
  void SendTrailers(int32_t stream_id, const HeaderMap& trailers);
  void ResetStream(int32_t stream_id, uint32_t error_code);

  // --- streaming (spec 004) ------------------------------------------------
  // Client-side: opens a request stream (headers only, no END_STREAM) so
  // message DATA can follow via SendData. Returns the stream id or -1.
  int32_t SubmitRequestOpen(
      const std::vector<std::pair<std::string, std::string>>&
          pseudo_and_headers);
  // SendData with a consumed-callback: fires exactly once after nghttp2
  // has fully read the body into its outbound queue (spec 004
  // backpressure boundary), or never if the stream closes first.
  void SendData(int32_t stream_id, const std::string& data, bool end_stream,
                std::function<void()> on_consumed);

 public:
  // Opaque implementation type (defined in the .cpp; pimpl idiom).
  struct Impl;

 private:
  Impl* impl_;
};

}  // namespace core
}  // namespace urpc
