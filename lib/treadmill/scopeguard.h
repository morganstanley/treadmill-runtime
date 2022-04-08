#pragma once

#include <functional>
#include <memory>

namespace treadmill {

inline auto make_scopeguard(const std::function<void(void *)> &cleanup) {
  return std::shared_ptr<void *>(nullptr, cleanup);
}

} // namespace treadmill
