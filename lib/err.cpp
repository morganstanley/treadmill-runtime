#include <treadmill/err.h>

#include <errno.h>
#include <string.h>

#include <fmt/format.h>

namespace treadmill {

std::string errno_msg() {
  return fmt::format(" [ {} - {} ] ", errno, strerror(errno));
}

} // namespace treadmill
