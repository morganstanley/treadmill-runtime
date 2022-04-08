#ifndef TREADMILL_CONVERT_H
#define TREADMILL_CONVERT_H

#include <chrono>
#include <string>

namespace treadmill {

inline auto bytes2number(const std::string &value) {
  long multiplier = 1;
  switch (toupper(value[value.size() - 1])) {
  case 'P':
    multiplier <<= 10;
  case 'T':
    multiplier <<= 10;
  case 'G':
    multiplier <<= 10;
  case 'M':
    multiplier <<= 10;
  case 'K':
    multiplier <<= 10;
    break;
  default:
    // TODO: need to handle invalid format.
    break;
  }
  return std::stol(value) * multiplier;
}

inline auto cpu2number(const std::string &value) {
  // TODO: need to handle invalid format.
  return std::stol(value);
}

inline auto str2duration(const std::string &value) {
  long multiplier = 1;
  switch (toupper(value[value.size() - 1])) {
  case 'D':
    multiplier *= 24;
  case 'H':
    multiplier *= 60;
  case 'M':
    multiplier *= 60;
  case 'S':
    break;
  default:
    // TODO: need to handle invalid format.
    break;
  }

  return std::chrono::seconds(std::stol(value) * multiplier);
}

} // namespace treadmill

#endif // TREADMILL_CONVERT_H
