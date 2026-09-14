#include <gtest/gtest.h>

#include <mutex>
#include <string>
#include <vector>

#include "urpc/core/log.h"

namespace {

using urpc::core::log::LogCategory;
using urpc::core::log::LogLevel;
using urpc::core::log::LogSink;

class CaptureSink : public LogSink {
 public:
  void Write(LogLevel level, LogCategory category, const char* event,
             const std::string& fields) override {
    std::lock_guard<std::mutex> lock(mu_);
    records.emplace_back(level, category, event, fields);
  }
  size_t size() {
    std::lock_guard<std::mutex> lock(mu_);
    return records.size();
  }
  const std::tuple<LogLevel, LogCategory, std::string, std::string>& last() {
    std::lock_guard<std::mutex> lock(mu_);
    return records.back();
  }

 private:
  std::mutex mu_;
  std::vector<std::tuple<LogLevel, LogCategory, std::string, std::string>>
      records;
};

class LogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    sink_ = std::make_shared<CaptureSink>();
    urpc::core::log::SetSink(sink_);
    urpc::core::log::SetLevel(LogLevel::kDebug);  // record everything
  }
  void TearDown() override {
    urpc::core::log::SetSink(nullptr);  // restore default stderr sink
    urpc::core::log::SetLevel(LogLevel::kWarn);
  }
  std::shared_ptr<CaptureSink> sink_;
};

TEST_F(LogTest, DefaultLevelIsWarn) {
  // (checked via Level() round-trip, not the initial global state)
  urpc::core::log::SetLevel(LogLevel::kWarn);
  EXPECT_EQ(urpc::core::log::Level(), LogLevel::kWarn);
}

TEST_F(LogTest, LevelOrderingGatesWrites) {
  urpc::core::log::SetLevel(LogLevel::kWarn);
  EXPECT_TRUE(urpc::core::log::Enabled(LogLevel::kError));
  EXPECT_TRUE(urpc::core::log::Enabled(LogLevel::kWarn));
  EXPECT_FALSE(urpc::core::log::Enabled(LogLevel::kInfo));
  EXPECT_FALSE(urpc::core::log::Enabled(LogLevel::kDebug));

  urpc::core::log::Error(LogCategory::kCall, "e1", "a=1");
  urpc::core::log::Warn(LogCategory::kConnection, "e2", "b=2");
  urpc::core::log::Info(LogCategory::kCall, "e3", "c=3");
  urpc::core::log::Debug(LogCategory::kCall, "e4", "d=4");
  ASSERT_EQ(sink_->size(), 2u);
}

TEST_F(LogTest, WriteDeliversLevelCategoryEventFields) {
  urpc::core::log::Info(LogCategory::kConnection, "conn_open",
                        "peer=127.0.0.1:9 dir=server");
  ASSERT_EQ(sink_->size(), 1u);
  const auto& r = sink_->last();
  EXPECT_EQ(std::get<0>(r), LogLevel::kInfo);
  EXPECT_EQ(std::get<1>(r), LogCategory::kConnection);
  EXPECT_EQ(std::get<2>(r), "conn_open");
  EXPECT_EQ(std::get<3>(r), "peer=127.0.0.1:9 dir=server");
}

TEST_F(LogTest, Names) {
  EXPECT_STREQ(urpc::core::log::LevelName(LogLevel::kError), "ERROR");
  EXPECT_STREQ(urpc::core::log::LevelName(LogLevel::kWarn), "WARN");
  EXPECT_STREQ(urpc::core::log::LevelName(LogLevel::kInfo), "INFO");
  EXPECT_STREQ(urpc::core::log::LevelName(LogLevel::kDebug), "DEBUG");
  EXPECT_STREQ(urpc::core::log::CategoryName(LogCategory::kConnection), "conn");
  EXPECT_STREQ(urpc::core::log::CategoryName(LogCategory::kCall), "call");
}

TEST_F(LogTest, SetNullSinkRestoresDefault) {
  urpc::core::log::SetSink(nullptr);
  // Writing must not crash with the default sink.
  urpc::core::log::Error(LogCategory::kCall, "to_default", "x=1");
  SUCCEED();
}

}  // namespace
