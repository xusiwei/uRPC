#include <gtest/gtest.h>
#include <cstdlib>
#include <cstdio>

#include <atomic>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#include "urpc/core/codec.h"
#include "urpc/core/h2_session.h"

#include <nghttp2/nghttp2.h>

namespace {

using urpc::core::EncodeFrame;
using urpc::core::FrameDecoder;
using urpc::core::H2Session;

// In-memory duplex wire: delivers each session's output to the peer.
// nghttp2 callbacks are synchronous, so Consume() can re-enter OnWrite
// before returning — bytes are only queued in nested calls and drained
// iteratively at the outermost level (no recursion, no deque mutation
// during iteration).
class Wire {
 public:
  void Attach(H2Session* client, H2Session* server) {
    client_ = client;
    server_ = server;
  }

  void Queue(size_t dir, const uint8_t* d, size_t n) {
    std::vector<uint8_t> v(d, d + n);
    if (dir == 0) {
      a2b_.push_back(std::move(v));
    } else {
      b2a_.push_back(std::move(v));
    }
    // NOTE: never Consume() from inside OnWrite — nghttp2 forbids
    // re-entering a session while inside session_send/mem_recv. The test
    // pumps explicitly at assertion points instead.
  }

  void Pump() {
    while (!a2b_.empty() || !b2a_.empty()) {
      if (!a2b_.empty()) {
        auto v = std::move(a2b_.front());
        a2b_.pop_front();
        server_->Consume(v.data(), v.size());
      } else {
        auto v = std::move(b2a_.front());
        b2a_.pop_front();
        client_->Consume(v.data(), v.size());
      }
    }
  }

 private:
  H2Session* client_ = nullptr;
  H2Session* server_ = nullptr;
  std::deque<std::vector<uint8_t>> a2b_;  // client → server
  std::deque<std::vector<uint8_t>> b2a_;  // server → client
};

class RecordingHandler : public H2Session::Handler {
 public:
  void OnHeadersComplete(int32_t sid, const H2Session::HeaderMap& headers,
                         bool end_stream) override {
    if (getenv("URPC_H2_DEBUG")) fprintf(stderr, "[dbg dir=%zu] headers sid=%d end=%d\n", direction_, sid, (int)end_stream);
    header_blocks_[sid].push_back(headers);
    if (end_stream) end_headers_[sid] = true;
    events_.emplace_back("headers");
  }
  void OnData(int32_t sid, const uint8_t* data, size_t len,
              bool end_stream) override {
    if (data != nullptr && len > 0) {
      data_[sid].append(reinterpret_cast<const char*>(data), len);
    }
    if (end_stream) end_data_[sid] = true;
    events_.emplace_back("data");
  }
  void OnStreamClose(int32_t sid, uint32_t error_code) override {
    closed_[sid] = error_code;
    events_.emplace_back("close");
  }
  void OnWrite(const uint8_t* data, size_t len) override {
    if (getenv("URPC_H2_DEBUG")) {
      fprintf(stderr, "[dbg dir=%zu] write %zu bytes:", direction_, len);
      for (size_t i = 0; i < len && i < 48; i++) fprintf(stderr, " %02x", data[i]);
      fprintf(stderr, "\n");
    }
    if (wire_ != nullptr) wire_->Queue(direction_, data, len);
  }

  void Bind(Wire* wire, size_t dir) {
    wire_ = wire;
    direction_ = dir;
  }

  // First header block (initial HEADERS).
  const H2Session::HeaderMap& InitialHeaders(int32_t sid) const {
    return header_blocks_.at(sid).front();
  }
  // Last header block (trailers when present).
  const H2Session::HeaderMap& LastHeaders(int32_t sid) const {
    return header_blocks_.at(sid).back();
  }

  Wire* wire_ = nullptr;
  size_t direction_ = 0;
  std::map<int32_t, std::vector<H2Session::HeaderMap>> header_blocks_;
  std::map<int32_t, bool> end_headers_;
  std::map<int32_t, std::string> data_;
  std::map<int32_t, bool> end_data_;
  std::map<int32_t, uint32_t> closed_;
  std::vector<std::string> events_;
};

class H2SessionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_unique<H2Session>(H2Session::Role::kClient,
                                           &client_handler_);
    server_ = std::make_unique<H2Session>(H2Session::Role::kServer,
                                           &server_handler_);
    wire_.Attach(client_.get(), server_.get());
    client_handler_.Bind(&wire_, 0);
    server_handler_.Bind(&wire_, 1);
    server_->Flush();
    client_->Flush();
    wire_.Pump();
  }

  Wire wire_;
  RecordingHandler client_handler_;
  RecordingHandler server_handler_;
  std::unique_ptr<H2Session> client_;
  std::unique_ptr<H2Session> server_;
};

TEST_F(H2SessionTest, UnaryRequestResponseRoundTrip) {
  // --- client: request with one framed message ----------------------------
  const std::string msg = "ping";
  std::string framed;
  EncodeFrame(msg, &framed);
  int32_t sid = client_->SubmitRequest(
      {{":method", "POST"},
       {":scheme", "http"},
       {":path", "/example.EchoService/Echo"},
       {":authority", "127.0.0.1:50051"},
       {"content-type", "application/grpc"},
       {"te", "trailers"}},
      framed);
  ASSERT_GT(sid, 0);
  wire_.Pump();

  // --- server: sees request headers + data + end ---------------------------
  ASSERT_EQ(server_handler_.header_blocks_.count(sid), 1u);
  EXPECT_EQ(server_handler_.InitialHeaders(sid).at(":path"),
            "/example.EchoService/Echo");
  EXPECT_EQ(server_handler_.InitialHeaders(sid).at("content-type"),
            "application/grpc");
  EXPECT_FALSE(server_handler_.end_headers_[sid]);
  ASSERT_EQ(server_handler_.data_.count(sid), 1u);
  FrameDecoder dec;
  ASSERT_TRUE(dec.Consume(server_handler_.data_[sid]).ok());
  ASSERT_TRUE(dec.HasMessage());
  EXPECT_EQ(dec.TakeMessage(), "ping");
  EXPECT_TRUE(server_handler_.end_data_[sid]);

  // --- server: respond 200 + one framed message + trailers -----------------
  std::string resp_framed;
  EncodeFrame(std::string("pong"), &resp_framed);
  server_->SendHeaders(sid, {{":status", "200"}, {"content-type", "application/grpc"}}, false);
  server_->SendData(sid, resp_framed, false);
  server_->SendTrailers(sid, {{"grpc-status", "0"}});
  wire_.Pump();

  // --- client: sees response headers, data, trailers, close ----------------
  ASSERT_GE(client_handler_.header_blocks_.count(sid), 1u);
  EXPECT_EQ(client_handler_.InitialHeaders(sid).at(":status"), "200");
  ASSERT_EQ(client_handler_.data_.count(sid), 1u);
  FrameDecoder cdec;
  ASSERT_TRUE(cdec.Consume(client_handler_.data_[sid]).ok());
  ASSERT_TRUE(cdec.HasMessage());
  EXPECT_EQ(cdec.TakeMessage(), "pong");
  // trailers arrive as a trailing HEADERS block with END_STREAM
  EXPECT_EQ(client_handler_.LastHeaders(sid).at("grpc-status"), "0");
  EXPECT_TRUE(client_handler_.end_headers_[sid]);
  ASSERT_EQ(client_handler_.closed_.count(sid), 1u);
  EXPECT_EQ(client_handler_.closed_[sid], 0u);  // clean close
}

TEST_F(H2SessionTest, EmptyBodyRequestEndsStreamInHeaders) {
  int32_t sid = client_->SubmitRequest(
      {{":method", "POST"},
        {":scheme", "http"},
        {":path", "/svc/M"},
        {":authority", "127.0.0.1:1"},
        {"content-type", "application/grpc"},
        {"te", "trailers"}},
      std::string());
  ASSERT_GT(sid, 0);
  wire_.Pump();
  EXPECT_TRUE(server_handler_.end_headers_[sid]);  // END_STREAM on headers
  EXPECT_EQ(server_handler_.data_.count(sid), 0u);
  EXPECT_EQ(server_handler_.header_blocks_[sid].size(), 1u);
}

TEST_F(H2SessionTest, TrailersOnlyErrorResponse) {
  int32_t sid = client_->SubmitRequest(
      {{":method", "POST"},
        {":scheme", "http"},
        {":path", "/svc/Missing"},
        {":authority", "127.0.0.1:1"},
        {"content-type", "application/grpc"},
        {"te", "trailers"}},
      std::string());
  ASSERT_GT(sid, 0);
  wire_.Pump();
  // trailers-only: headers carry grpc-status and END_STREAM
  server_->SendHeaders(sid,
                       {{":status", "200"},
                        {"content-type", "application/grpc"},
                        {"grpc-status", "12"},
                        {"grpc-message", "unknown method"}},
                       true);
  wire_.Pump();
  EXPECT_EQ(client_handler_.InitialHeaders(sid).at("grpc-status"), "12");
  EXPECT_EQ(client_handler_.InitialHeaders(sid).at("grpc-message"),
            "unknown method");
  EXPECT_TRUE(client_handler_.end_headers_[sid]);
  EXPECT_EQ(client_handler_.data_.count(sid), 0u);  // no DATA at all
  ASSERT_EQ(client_handler_.closed_.count(sid), 1u);
}

TEST_F(H2SessionTest, LargeBodySurvivesChunkedDelivery) {
  const std::string big(200 * 1024, 'x');  // beyond one DATA frame
  std::string framed;
  EncodeFrame(big, &framed);
  int32_t sid = client_->SubmitRequest(
      {{":method", "POST"},
        {":scheme", "http"},
        {":path", "/svc/Big"},
        {":authority", "127.0.0.1:1"},
        {"content-type", "application/grpc"},
        {"te", "trailers"}},
      framed);
  ASSERT_GT(sid, 0);
  wire_.Pump();
  FrameDecoder dec;
  ASSERT_TRUE(dec.Consume(server_handler_.data_[sid]).ok());
  ASSERT_TRUE(dec.HasMessage());
  EXPECT_EQ(dec.TakeMessage().size(), big.size());
}

TEST_F(H2SessionTest, ResetStreamReachesPeer) {
  int32_t sid = client_->SubmitRequest(
      {{":method", "POST"},
        {":scheme", "http"},
        {":path", "/svc/M"},
        {":authority", "127.0.0.1:1"},
        {"content-type", "application/grpc"},
        {"te", "trailers"}},
      std::string());
  ASSERT_GT(sid, 0);
  client_->ResetStream(sid, NGHTTP2_CANCEL);
  wire_.Pump();
  ASSERT_EQ(server_handler_.closed_.count(sid), 1u);
  EXPECT_EQ(server_handler_.closed_[sid], NGHTTP2_CANCEL);
}

}  // namespace
