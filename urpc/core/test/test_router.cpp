#include <gtest/gtest.h>

#include <atomic>
#include <cstring>

#include "urpc/core/router.h"

namespace {

using urpc::core::Router;
using urpc::core::ServerCallCtx;
using urpc::core::Status;
using urpc::core::StatusCode;
using urpc::core::UnaryHandler;

// Minimal stub context for handler capture.
class StubCtx : public ServerCallCtx {
 public:
  Status Respond(Status s, const std::string&) override { return s; }
  bool IsCancelled() const override { return false; }
  bool OnCancel(std::function<void()>) override { return false; }
  uint64_t TimeRemainingMs() const override { return 0; }
  const std::string& path() const override { return path_; }
  std::string path_;
};

UnaryHandler Counter(std::atomic<int>* hits) {
  return [hits](ServerCallCtx&, const std::string&) {
    hits->fetch_add(1);
  };
}

TEST(Router, RegisterAndFind) {
  Router r;
  std::atomic<int> hits{0};
  ASSERT_TRUE(r.RegisterUnary("/svc/Method", Counter(&hits)).ok());
  auto h = r.Find("/svc/Method");
  ASSERT_TRUE(h.has_value());
  StubCtx ctx;
  h->unary(ctx, "");
  EXPECT_EQ(hits.load(), 1);
  EXPECT_EQ(r.size(), 1u);
}

TEST(Router, FindUnknownPathMisses) {
  Router r;
  EXPECT_FALSE(r.Find("/nope/Nope").has_value());
  EXPECT_EQ(r.size(), 0u);
}

TEST(Router, DuplicatePathRejected) {
  Router r;
  std::atomic<int> hits{0};
  ASSERT_TRUE(r.RegisterUnary("/a/b", Counter(&hits)).ok());
  auto st = r.RegisterUnary("/a/b", Counter(&hits));
  ASSERT_FALSE(st.ok());
  EXPECT_EQ(st.code(), StatusCode::kInternal);
  // original handler still works
  auto h = r.Find("/a/b");
  ASSERT_TRUE(h.has_value());
  StubCtx ctx;
  h->unary(ctx, "");
  EXPECT_EQ(hits.load(), 1);
  EXPECT_EQ(r.size(), 1u);
}

TEST(Router, InvalidRegistrationsRejected) {
  Router r;
  std::atomic<int> hits{0};
  EXPECT_FALSE(r.RegisterUnary("", Counter(&hits)).ok());
  EXPECT_FALSE(r.RegisterUnary("svc/method", Counter(&hits)).ok());  // no '/'
  EXPECT_FALSE(r.RegisterUnary("/svc/method", nullptr).ok());        // null
  EXPECT_EQ(r.size(), 0u);
}

TEST(Router, DynamicRegistrationVisibleWithoutRestart) {
  Router r;
  std::atomic<int> hits{0};
  ASSERT_TRUE(r.RegisterUnary("/svc/A", Counter(&hits)).ok());
  // snapshot taken before the new registration
  auto before = r.Find("/svc/B");
  EXPECT_FALSE(before.has_value());
  // register later (dynamic registration, US1 scenario 2)
  ASSERT_TRUE(r.RegisterUnary("/svc/B", Counter(&hits)).ok());
  auto after = r.Find("/svc/B");
  ASSERT_TRUE(after.has_value());
  StubCtx ctx;
  after->unary(ctx, "");
  EXPECT_EQ(hits.load(), 1);
  EXPECT_EQ(r.size(), 2u);
}

TEST(Router, HeldSnapshotStaysStableAcrossWrites) {
  Router r;
  std::atomic<int> hits{0};
  ASSERT_TRUE(r.RegisterUnary("/svc/Keep", Counter(&hits)).ok());
  auto old_handler = r.Find("/svc/Keep");
  ASSERT_TRUE(old_handler.has_value());
  ASSERT_TRUE(r.RegisterUnary("/svc/New", Counter(&hits)).ok());
  // the previously returned handler remains valid (shared_ptr snapshot)
  StubCtx ctx;
  old_handler->unary(ctx, "");
  EXPECT_EQ(hits.load(), 1);
}

}  // namespace
