#include <gtest/gtest.h>

#include "urpc/core/status.h"

namespace {

using urpc::core::Status;
using urpc::core::StatusCode;

TEST(Status, EnumMatchesGrpcWireCodes) {
  EXPECT_EQ(static_cast<int>(StatusCode::kOk), 0);
  EXPECT_EQ(static_cast<int>(StatusCode::kDeadlineExceeded), 4);
  EXPECT_EQ(static_cast<int>(StatusCode::kResourceExhausted), 8);
  EXPECT_EQ(static_cast<int>(StatusCode::kUnimplemented), 12);
  EXPECT_EQ(static_cast<int>(StatusCode::kInternal), 13);
  EXPECT_EQ(static_cast<int>(StatusCode::kUnavailable), 14);
  EXPECT_EQ(static_cast<int>(StatusCode::kDataLoss), 15);
}

TEST(Status, NamesAreGrpcCanonical) {
  EXPECT_STREQ(urpc::core::StatusCodeName(StatusCode::kOk), "OK");
  EXPECT_STREQ(urpc::core::StatusCodeName(StatusCode::kDeadlineExceeded),
               "DEADLINE_EXCEEDED");
  EXPECT_STREQ(urpc::core::StatusCodeName(StatusCode::kResourceExhausted),
               "RESOURCE_EXHAUSTED");
  EXPECT_STREQ(urpc::core::StatusCodeName(StatusCode::kUnimplemented),
               "UNIMPLEMENTED");
  EXPECT_STREQ(urpc::core::StatusCodeName(StatusCode::kInternal), "INTERNAL");
  EXPECT_STREQ(urpc::core::StatusCodeName(StatusCode::kUnavailable),
               "UNAVAILABLE");
  EXPECT_STREQ(urpc::core::StatusCodeName(StatusCode::kDataLoss), "DATA_LOSS");
}

TEST(Status, DefaultIsOk) {
  Status s;
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(s.code(), StatusCode::kOk);
  EXPECT_TRUE(s.message().empty());
}

TEST(Status, OkFactory) {
  EXPECT_TRUE(Status::Ok().ok());
}

TEST(Status, ErrorCarriesCodeAndMessage) {
  Status s(StatusCode::kUnavailable, "dial failed");
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.code(), StatusCode::kUnavailable);
  EXPECT_EQ(s.message(), "dial failed");
  EXPECT_EQ(s.ToString(), "UNAVAILABLE: dial failed");
}

TEST(Status, ToStringWithoutMessageIsNameOnly) {
  Status s(StatusCode::kInternal, "");
  EXPECT_EQ(s.ToString(), "INTERNAL");
}

TEST(Status, CopyAndAssign) {
  Status a(StatusCode::kDataLoss, "x");
  Status b = a;
  EXPECT_EQ(b.code(), StatusCode::kDataLoss);
  EXPECT_EQ(b.message(), "x");
  Status c;
  c = a;
  EXPECT_FALSE(c.ok());
  // mutating a afterwards must not affect c
  a = Status::Ok();
  EXPECT_FALSE(c.ok());
}

}  // namespace
