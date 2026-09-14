#include <gtest/gtest.h>

#include <cstring>

#include "urpc/core/codec.h"

namespace {

using urpc::core::EncodeFrame;
using urpc::core::FrameDecoder;
using urpc::core::Status;
using urpc::core::StatusCode;

TEST(Codec, EncodeFramePrefix) {
  std::string out;
  EncodeFrame("abc", &out);
  ASSERT_EQ(out.size(), 3u + 5u);
  EXPECT_EQ(out[0], '\0');  // compressed flag
  EXPECT_EQ(static_cast<unsigned char>(out[1]), 0);
  EXPECT_EQ(static_cast<unsigned char>(out[2]), 0);
  EXPECT_EQ(static_cast<unsigned char>(out[3]), 0);
  EXPECT_EQ(static_cast<unsigned char>(out[4]), 3);
  EXPECT_EQ(out.substr(5), "abc");
}

TEST(Codec, RoundTripSingleMessage) {
  std::string wire;
  EncodeFrame(std::string("hello frame"), &wire);
  FrameDecoder dec;
  ASSERT_TRUE(dec.Consume(wire).ok());
  ASSERT_TRUE(dec.HasMessage());
  EXPECT_EQ(dec.TakeMessage(), "hello frame");
  EXPECT_FALSE(dec.HasMessage());
  EXPECT_EQ(dec.message_count(), 1u);
}

TEST(Codec, EmptyMessageRoundTrip) {
  std::string wire;
  EncodeFrame(std::string(), &wire);
  ASSERT_EQ(wire.size(), 5u);
  FrameDecoder dec;
  ASSERT_TRUE(dec.Consume(wire).ok());
  ASSERT_TRUE(dec.HasMessage());
  EXPECT_TRUE(dec.TakeMessage().empty());
}

TEST(Codec, TwoMessagesInSequence) {
  std::string wire;
  EncodeFrame(std::string("one"), &wire);
  EncodeFrame(std::string("two"), &wire);
  FrameDecoder dec;
  ASSERT_TRUE(dec.Consume(wire).ok());
  ASSERT_TRUE(dec.HasMessage());
  EXPECT_EQ(dec.TakeMessage(), "one");
  ASSERT_TRUE(dec.HasMessage());
  EXPECT_EQ(dec.TakeMessage(), "two");
  EXPECT_EQ(dec.message_count(), 2u);
}

TEST(Codec, IncrementalByteByByte) {
  std::string wire;
  EncodeFrame(std::string("chunked"), &wire);
  FrameDecoder dec;
  for (char c : wire) {
    ASSERT_TRUE(dec.Consume(std::string(1, c)).ok());
  }
  ASSERT_TRUE(dec.HasMessage());
  EXPECT_EQ(dec.TakeMessage(), "chunked");
}

TEST(Codec, SplitAcrossBoundary) {
  std::string wire;
  EncodeFrame(std::string("0123456789"), &wire);
  FrameDecoder dec;
  ASSERT_TRUE(dec.Consume(wire.substr(0, 7)).ok());  // partial header+data
  EXPECT_FALSE(dec.HasMessage());
  ASSERT_TRUE(dec.Consume(wire.substr(7)).ok());
  EXPECT_EQ(dec.TakeMessage(), "0123456789");
}

TEST(Codec, DeclaredLengthOverLimitRejected) {
  std::string wire;
  EncodeFrame(std::string(100, 'x'), &wire);
  FrameDecoder dec(50);
  Status st = dec.Consume(wire);
  ASSERT_FALSE(st.ok());
  EXPECT_EQ(st.code(), StatusCode::kResourceExhausted);
}

TEST(Codec, CompressedFlagRejected) {
  std::string wire;
  EncodeFrame(std::string("zz"), &wire);
  wire[0] = '\x01';  // pretend compression
  FrameDecoder dec;
  Status st = dec.Consume(wire);
  ASSERT_FALSE(st.ok());
  EXPECT_EQ(st.code(), StatusCode::kInternal);
}

TEST(Codec, MaxMessageSizeAccessor) {
  FrameDecoder default_dec;
  EXPECT_EQ(default_dec.max_message_size(), 4u * 1024 * 1024);
  FrameDecoder custom_dec(123);
  EXPECT_EQ(custom_dec.max_message_size(), 123u);
}

TEST(Codec, EmptyConsumeIsNoop) {
  FrameDecoder dec;
  ASSERT_TRUE(dec.Consume(std::string()).ok());
  EXPECT_FALSE(dec.HasMessage());
  EXPECT_EQ(dec.message_count(), 0u);
}

}  // namespace
