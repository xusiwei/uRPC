#include "urpc/core/loop.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstdint>

#include "urpc/core/log.h"

namespace urpc {
namespace core {

void LoopRunner::OnWakeTramp(uv_async_t* handle) {
  auto* self = static_cast<LoopRunner*>(handle->data);
  self->DrainPending();
}

void LoopRunner::OnTimerTramp(uv_timer_t* handle) {
  auto* ctx = static_cast<TimerCtx*>(handle->data);
  ctx->self->CancelTimer(ctx->id);
  ctx->task();
  // ctx freed by CancelTimer's uv_close callback
}

void LoopRunner::OnTimerClosedTramp(uv_handle_t* handle) {
  delete static_cast<TimerCtx*>(handle->data);
}

LoopRunner::~LoopRunner() { Stop(); }

bool LoopRunner::Start() {
  if (running_.load(std::memory_order_acquire)) return false;

  if (uv_loop_init(&loop_) != 0) return false;
  wake_.data = this;
  if (uv_async_init(&loop_, &wake_, &LoopRunner::OnWakeTramp) != 0) {
    uv_loop_close(&loop_);
    return false;
  }

  running_.store(true, std::memory_order_release);
  quit_requested_.store(false, std::memory_order_release);
  thread_ = std::thread([this] { RunLoop(); });

  // Wait until the loop thread has recorded its identity.
  while (thread_id_ == std::thread::id()) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  return true;
}

void LoopRunner::RunLoop() {
  thread_id_ = std::this_thread::get_id();
  // wake_ keeps the default run alive; uv_run returns only once every
  // handle (async + timers) has been closed by the quit task.
  uv_run(&loop_, UV_RUN_DEFAULT);
}

void LoopRunner::DrainPending() {
  std::deque<Task> ready;
  {
    std::lock_guard<std::mutex> lock(mu_);
    ready.swap(pending_);
  }
  for (auto& task : ready) {
    task();
  }
}

void LoopRunner::Post(Task fn) {
  if (!fn) return;
  {
    std::lock_guard<std::mutex> lock(mu_);
    pending_.push_back(std::move(fn));
  }
  if (running_.load(std::memory_order_acquire)) {
    uv_async_send(&wake_);
  }
}

void LoopRunner::CloseAllHandlesOnLoop() {
  if (getenv("URPC_WIRE_DEBUG"))
    std::fprintf(stderr, "[dbg] CloseAllHandlesOnLoop: timers=%zu\n",
                 timers_.size());
  for (auto& [id, ctx] : timers_) {
    uv_timer_stop(&ctx->handle);
    uv_close(reinterpret_cast<uv_handle_t*>(&ctx->handle),
             &LoopRunner::OnTimerClosedTramp);
  }
  timers_.clear();
  if (!uv_is_closing(reinterpret_cast<uv_handle_t*>(&wake_))) {
    uv_close(reinterpret_cast<uv_handle_t*>(&wake_),
             [](uv_handle_t* h) { (void)h; });
  }
  // Force-close any handles still alive (e.g. sockets owned by callers that
  // forgot to close): without this uv_run would never return.
  uv_walk(&loop_,
          [](uv_handle_t* h, void*) {
            if (getenv("URPC_WIRE_DEBUG"))
              std::fprintf(stderr,
                           "[dbg] CloseAll: walk type=%s active=%d "
                           "closing=%d\n",
                           uv_handle_type_name(uv_handle_get_type(h)),
                           (int)uv_is_active(h), (int)uv_is_closing(h));
            if (!uv_is_closing(h)) uv_close(h, nullptr);
          },
          nullptr);
  if (getenv("URPC_WIRE_DEBUG"))
    std::fprintf(stderr, "[dbg] CloseAllHandlesOnLoop: done\n");
}

void LoopRunner::Stop() {
  if (getenv("URPC_WIRE_DEBUG"))
    std::fprintf(stderr, "[dbg] LoopRunner::Stop begin on_loop=%d\n",
                 (int)OnLoopThread());
  if (!running_.exchange(false, std::memory_order_acq_rel)) return;

  quit_requested_.store(true, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lock(mu_);
    pending_.clear();
    pending_.push_back([this] { CloseAllHandlesOnLoop(); });
  }
  if (wake_.data != nullptr &&
      !uv_is_closing(reinterpret_cast<uv_handle_t*>(&wake_))) {
    uv_async_send(&wake_);
  }
  if (getenv("URPC_WIRE_DEBUG"))
    std::fprintf(stderr, "[dbg] Stop: about to join/detach\n");
  if (thread_.joinable()) {
    if (OnLoopThread()) {
      if (getenv("URPC_WIRE_DEBUG"))
        std::fprintf(stderr, "[dbg] Stop: detaching (on loop thread)\n");
      // Stop() invoked from a task on the loop thread: joining ourselves
      // would deadlock; detach and let the loop finish on its own.
      thread_.detach();
    } else {
      thread_.join();
      if (getenv("URPC_WIRE_DEBUG"))
        std::fprintf(stderr, "[dbg] Stop: joined\n");
    }
  }
  if (!OnLoopThread()) {
    uv_loop_close(&loop_);
    thread_id_ = std::thread::id();
  }
}

uint64_t LoopRunner::SetTimer(uint64_t delay_ms, Task fn) {
  if (!running_.load(std::memory_order_acquire)) return 0;
  auto* ctx = new TimerCtx{this, next_timer_id_++, std::move(fn), {}};
  ctx->handle.data = ctx;
  if (uv_timer_init(&loop_, &ctx->handle) != 0) {
    delete ctx;
    return 0;
  }
  timers_[ctx->id] = ctx;
  uv_timer_start(&ctx->handle, &LoopRunner::OnTimerTramp, delay_ms, 0);
  return ctx->id;
}

void LoopRunner::CancelTimer(uint64_t id) {
  auto it = timers_.find(id);
  if (it == timers_.end()) return;
  TimerCtx* ctx = it->second;
  timers_.erase(it);
  uv_timer_stop(&ctx->handle);
  uv_close(reinterpret_cast<uv_handle_t*>(&ctx->handle),
                  &LoopRunner::OnTimerClosedTramp);
}

uint64_t LoopRunner::NowMs() const {
  // Steady wall clock: uv_now() only advances on loop iterations, which is
  // useless for deadline math from other threads.
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

}  // namespace core
}  // namespace urpc
