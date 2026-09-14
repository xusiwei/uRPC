#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <thread>

#include <uv.h>

namespace urpc {
namespace core {

// LoopRunner owns a dedicated libuv event-loop thread (research.md #3).
// External threads submit work via Post(); uv_async wakes the loop.
class LoopRunner {
 public:
  using Task = std::function<void()>;

  LoopRunner() = default;
  ~LoopRunner();

  LoopRunner(const LoopRunner&) = delete;
  LoopRunner& operator=(const LoopRunner&) = delete;

  // Starts the loop thread. Returns false when already running or on
  // uv initialization failure.
  bool Start();

  // Posts the quit task, waits for all pending tasks to drain, and joins
  // the loop thread. Idempotent.
  void Stop();

  bool running() const { return running_.load(std::memory_order_acquire); }
  uv_loop_t* loop() { return &loop_; }
  bool OnLoopThread() const { return std::this_thread::get_id() == thread_id_; }

  // Thread-safe: run fn on the loop thread as soon as possible.
  void Post(Task fn);

  // One-shot timer; returns an id (0 when not running).
  // Must be called from the loop thread OR via Post() by external threads.
  uint64_t SetTimer(uint64_t delay_ms, Task fn);
  void CancelTimer(uint64_t id);

  uint64_t NowMs() const;

 private:
  struct TimerCtx {
    LoopRunner* self;
    uint64_t id;
    Task task;
    uv_timer_t handle;
  };

  void RunLoop();
  void DrainPending();
  void CloseAllHandlesOnLoop();

  // libuv trampolines (C callbacks) — need access to private state.
  static void OnWakeTramp(uv_async_t* handle);
  static void OnTimerTramp(uv_timer_t* handle);
  static void OnTimerClosedTramp(uv_handle_t* handle);

  uv_loop_t loop_{};
  uv_async_t wake_{};
  std::thread thread_;
  std::thread::id thread_id_{};

  std::mutex mu_;
  std::deque<Task> pending_;
  std::map<uint64_t, TimerCtx*> timers_;  // loop-thread only
  uint64_t next_timer_id_ = 1;

  std::atomic<bool> running_{false};
  std::atomic<bool> quit_requested_{false};
};

}  // namespace core
}  // namespace urpc
