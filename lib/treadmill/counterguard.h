#pragma once

#include <atomic>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>

namespace treadmill {

class Counter {
public:
  Counter() : counter_(0), done_(nullptr), force_exit_(false) {}
  auto value() const { return counter_.load(); };
  void inc() { ++counter_; }
  void dec() {
    long prev = counter_.fetch_sub(1);
    if (done_ && prev == 1) {
      std::promise<void> *done = done_;
      done_ = nullptr;
      done->set_value();
    }
  }

  bool wait(const std::function<void()> &f) {
    std::promise<void> done;
    done_ = &done;
    f();
    done.get_future().wait();
    return force_exit_ ? false : true;
  }

  auto make_guard(bool force_exit) {
    if (force_exit) {
      if (done_) {
        std::promise<void> *done = done_;
        done_ = nullptr;
        done->set_value();
      }
      force_exit_ = true;
    }
    return std::shared_ptr<void *>(nullptr, [this](void *) { dec(); });
  }

private:
  std::atomic_long counter_;
  std::promise<void> *done_;
  std::atomic_bool force_exit_;
};

// When fully migrated to c++20, replace with counting_semaphore.
class Semaphore {
public:
  Semaphore() : counter_(0), ready_(false) {}
  ~Semaphore() {}
  void inc() {
    std::unique_lock<std::mutex> lck(mx_);
    ++counter_;
  }
  void dec() {
    std::unique_lock<std::mutex> lck(mx_);
    --counter_;
    if (counter_ == 0) {
      ready_ = true;
      cv_.notify_all();
    }
  }
  void wait() {
    std::unique_lock<std::mutex> lck(mx_);
    while (!ready_)
      cv_.wait(lck);
  }

private:
  unsigned long counter_;
  bool ready_;
  std::mutex mx_;
  std::condition_variable cv_;
};

} // namespace treadmill
