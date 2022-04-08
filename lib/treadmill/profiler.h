#pragma once

#include <chrono>

namespace treadmill {

struct Timer {
  Timer() : t1(std::chrono::high_resolution_clock::now()) {}
  ~Timer() {
    time_span += std::chrono::duration_cast<std::chrono::duration<double>>(
        std::chrono::high_resolution_clock::now() - t1);
  }

  static std::chrono::duration<double> time_span;
  std::chrono::high_resolution_clock::time_point t1;
};

} // namespace treadmill
