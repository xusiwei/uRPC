#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "urpc/core/loop.h"

namespace {

using urpc::core::LoopRunner;

// Runs the loop thread's body inline; asserts it matches OnLoopThread().
TEST(LoopTest, StartStopLifecycle) {
  LoopRunner runner;
  EXPECT_FALSE(runner.running());
  ASSERT_TRUE(runner.Start());
  EXPECT_TRUE(runner.running());
  EXPECT_FALSE(runner.Start());  // second start is rejected
  runner.Stop();
  EXPECT_FALSE(runner.running());
  runner.Stop();  // idempotent
}

TEST(LoopTest, OnLoopThreadDistinguishesThreads) {
  LoopRunner runner;
  ASSERT_TRUE(runner.Start());
  std::atomic<bool> on_loop_thread_seen{false};
  std::mutex mu;
  std::condition_variable cv;
  bool done = false;
  runner.Post([&] {
    on_loop_thread_seen = runner.OnLoopThread();
    {
      std::lock_guard<std::mutex> lock(mu);
      done = true;
    }
    cv.notify_one();
  });
  {
    std::unique_lock<std::mutex> lock(mu);
    cv.wait_for(lock, std::chrono::seconds(2), [&] { return done; });
  }
  EXPECT_TRUE(on_loop_thread_seen.load());
  EXPECT_FALSE(runner.OnLoopThread());  // test main thread is not the loop
  runner.Stop();
}

TEST(LoopTest, PostFromExternalThreadExecutes) {
  LoopRunner runner;
  ASSERT_TRUE(runner.Start());
  std::atomic<int> counter{0};
  constexpr int kTasks = 64;
  std::thread poster([&] {
    for (int i = 0; i < kTasks; i++) {
      runner.Post([&] { counter.fetch_add(1); });
    }
  });
  poster.join();
  // wait for all tasks to drain
  for (int i = 0; i < 200 && counter.load() < kTasks; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_EQ(counter.load(), kTasks);
  runner.Stop();
}

TEST(LoopTest, TimerFiresOnce) {
  LoopRunner runner;
  ASSERT_TRUE(runner.Start());
  std::atomic<bool> fired{false};
  std::mutex mu;
  std::condition_variable cv;
  runner.Post([&] {
    runner.SetTimer(20, [&] {
      fired = true;
      cv.notify_one();
    });
  });
  std::unique_lock<std::mutex> lock(mu);
  cv.wait_for(lock, std::chrono::seconds(2), [&] { return fired.load(); });
  EXPECT_TRUE(fired.load());
  runner.Stop();
}

TEST(LoopTest, CancelledTimerDoesNotFire) {
  LoopRunner runner;
  ASSERT_TRUE(runner.Start());
  std::atomic<int> fires{0};
  std::atomic<uint64_t> timer_id{0};
  runner.Post([&] {
    timer_id = runner.SetTimer(60, [&] { fires.fetch_add(1); });
  });
  // wait until the timer id is assigned, then cancel it
  for (int i = 0; i < 100 && timer_id.load() == 0; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ASSERT_NE(timer_id.load(), 0u);
  runner.Post([&] { runner.CancelTimer(timer_id.load()); });
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  EXPECT_EQ(fires.load(), 0);
  runner.Stop();
}

TEST(LoopTest, NowMsIsMonotonicish) {
  LoopRunner runner;
  ASSERT_TRUE(runner.Start());
  const uint64_t a = runner.NowMs();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  const uint64_t b = runner.NowMs();
  EXPECT_GE(b - a, 20u);
  runner.Stop();
}

}  // namespace
