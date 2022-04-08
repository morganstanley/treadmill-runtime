#pragma once

#include <optional>

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <spdlog/spdlog.h>

namespace treadmill {

void init_logging(int argc, char **argv);
void print_stacktrace();

#ifndef PCHECK
#define PCHECK(condition)                                                      \
  if (!(condition)) {                                                          \
    SPDLOG_CRITICAL("Check failed: [{}] {}", #condition, errno);               \
    ::treadmill::print_stacktrace();                                           \
    throw std::logic_error(#condition);                                        \
  }
#endif

#ifndef PCHECK_LOG
#define PCHECK_LOG(condition, ...)                                             \
  if (!(condition)) {                                                          \
    std::string err = fmt::format(__VA_ARGS__);                                \
    SPDLOG_CRITICAL("Check failed: [{}] {} - {} - {}", #condition, errno,      \
                    strerror(errno), err);                                     \
    ::treadmill::print_stacktrace();                                           \
    throw std::logic_error(#condition);                                        \
  }
#endif

#ifndef CHECK
#define CHECK(condition)                                                       \
  if (!(condition)) {                                                          \
    SPDLOG_CRITICAL("Check failed: [{}]", #condition);                         \
    spdlog::default_logger_raw()->flush();                                     \
    ::treadmill::print_stacktrace();                                           \
    throw std::logic_error(#condition);                                        \
  }
#endif

#ifndef CHECK_LOG
#define CHECK_LOG(condition, ...)                                              \
  if (!(condition)) {                                                          \
    std::string err = fmt::format(__VA_ARGS__);                                \
    SPDLOG_CRITICAL("Check failed: [{}] {}", #condition, err);                 \
    ::treadmill::print_stacktrace();                                           \
    throw std::logic_error(#condition);                                        \
  }
#endif

} // namespace treadmill
