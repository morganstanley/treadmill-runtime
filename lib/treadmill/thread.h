#pragma once

#include <chrono>
#include <thread>

namespace treadmill {

class Event {
public:
  Event() {}

  void notify() {
    std::unique_lock<std::mutex> lck(mx_);
    ev_.notify_all();
  }

  void wait() {
    std::unique_lock<std::mutex> lck(mx_);
    ev_.wait(lck);
  }

private:
  std::mutex mx_;
  std::condition_variable ev_;
};

void sleep_forever() {
  using namespace std::chrono_literals;
  while (true) {
    std::this_thread::sleep_for(3600s);
  }
}

} // namespace treadmill
