#pragma once

#include <sstream>
#include <string>
#include <vector>

namespace treadmill {

static std::vector<std::string> split(const std::string &s, char sep) {
  std::stringstream ss(s);
  std::string item;
  std::vector<std::string> results;
  while (getline(ss, item, sep)) {
    results.push_back(item);
  }
  return results;
}

} // namespace treadmill
