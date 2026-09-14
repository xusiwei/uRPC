#pragma once

#include <memory>
#include <string>

namespace urpc {
namespace core {
namespace log {

enum class LogLevel { kError = 0, kWarn = 1, kInfo = 2, kDebug = 3 };

enum class LogCategory { kConnection, kCall };

// Sink receives single-line structured records:
//   "<LEVEL> <category> <event> k=v k=v"
class LogSink {
 public:
  virtual ~LogSink() = default;
  virtual void Write(LogLevel level, LogCategory category, const char* event,
                      const std::string& fields) = 0;
};

class DefaultStderrSink : public LogSink {
 public:
  void Write(LogLevel level, LogCategory category, const char* event,
             const std::string& fields) override;
};

// Configuration is global per process (FR-011: level MUST be controllable).
// Production default is warn.
void SetLevel(LogLevel level);
LogLevel Level();
void SetSink(std::shared_ptr<LogSink> sink);

bool Enabled(LogLevel level);
void Write(LogLevel level, LogCategory category, const char* event,
           const std::string& fields);

const char* LevelName(LogLevel level);
const char* CategoryName(LogCategory category);

inline void Error(LogCategory c, const char* e, const std::string& f) {
  Write(LogLevel::kError, c, e, f);
}
inline void Warn(LogCategory c, const char* e, const std::string& f) {
  Write(LogLevel::kWarn, c, e, f);
}
inline void Info(LogCategory c, const char* e, const std::string& f) {
  Write(LogLevel::kInfo, c, e, f);
}
inline void Debug(LogCategory c, const char* e, const std::string& f) {
  Write(LogLevel::kDebug, c, e, f);
}

}  // namespace log
}  // namespace core
}  // namespace urpc
