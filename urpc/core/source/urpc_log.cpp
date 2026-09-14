#include "urpc/core/log.h"

#include <cstdio>
#include <mutex>

namespace urpc {
namespace core {
namespace log {

namespace {

std::mutex g_mu;
LogLevel g_level = LogLevel::kWarn;  // production-friendly default
std::shared_ptr<LogSink> g_sink = std::make_shared<DefaultStderrSink>();

const char* kLevelNames[] = {"ERROR", "WARN", "INFO", "DEBUG"};
const char* kCategoryNames[] = {"conn", "call"};

}  // namespace

const char* LevelName(LogLevel level) {
  return kLevelNames[static_cast<int>(level)];
}

const char* CategoryName(LogCategory category) {
  return kCategoryNames[static_cast<int>(category)];
}

void DefaultStderrSink::Write(LogLevel level, LogCategory category,
                              const char* event, const std::string& fields) {
  static std::mutex write_mu;
  std::lock_guard<std::mutex> lock(write_mu);
  std::fprintf(stderr, "%s %s %s %s\n", LevelName(level),
               CategoryName(category), event, fields.c_str());
}

void SetLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(g_mu);
  g_level = level;
}

LogLevel Level() {
  std::lock_guard<std::mutex> lock(g_mu);
  return g_level;
}

void SetSink(std::shared_ptr<LogSink> sink) {
  std::lock_guard<std::mutex> lock(g_mu);
  g_sink = sink ? std::move(sink) : std::make_shared<DefaultStderrSink>();
}

bool Enabled(LogLevel level) {
  return static_cast<int>(level) <= static_cast<int>(Level());
}

void Write(LogLevel level, LogCategory category, const char* event,
           const std::string& fields) {
  if (!Enabled(level)) return;
  std::shared_ptr<LogSink> sink;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    sink = g_sink;
  }
  sink->Write(level, category, event, fields);
}

}  // namespace log
}  // namespace core
}  // namespace urpc
