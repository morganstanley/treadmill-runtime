#ifndef TREADMILL_SCHEDULER_COMMON_H
#define TREADMILL_SCHEDULER_COMMON_H

#include <algorithm>
#include <atomic>
#include <bitset>
#include <chrono>
#include <iterator>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <valarray>
#include <vector>

#include <bitmask/bitmask.hpp>

namespace treadmill {
namespace scheduler {

typedef int identity_t;
typedef int priority_t;
typedef int rank_t;
typedef std::chrono::time_point<std::chrono::system_clock> time_point_t;
typedef std::chrono::duration<long> duration_t;
typedef std::bitset<sizeof(size_t) * 8> traits_t;
typedef std::bitset<sizeof(size_t) * 8> labels_t;
typedef std::valarray<long> volume_t;
typedef std::valarray<long> affinity_limits_t;
typedef std::optional<identity_t> Identity;
typedef float utilization_t;
typedef uint32_t object_id_t;

template <typename T> using set_t = std::unordered_set<T>;
typedef set_t<std::string> string_set_t;

template <typename T> using sorted_set_t = std::set<T>;

template <typename K, typename V> using map_t = std::unordered_map<K, V>;
template <typename V> using string_map_t = std::unordered_map<std::string, V>;
template <typename K, typename V> using sorted_map_t = std::map<K, V>;
template <typename V> using sorted_string_map_t = std::map<std::string, V>;

static_assert(sizeof(size_t) == sizeof(labels_t));
static_assert(sizeof(size_t) == sizeof(traits_t));

inline bool all(const volume_t &lhs, const volume_t &rhs,
                const std::function<bool(volume_t::value_type,
                                         volume_t::value_type)> &pred) {
  CHECK(lhs.size() == rhs.size());
  auto li = std::begin(lhs);
  auto ri = std::begin(rhs);

  for (; li != std::end(lhs); ++li, ++ri) {
    if (!pred((*li), (*ri))) {
      return false;
    }
  }
  return true;
}

inline bool any(const volume_t &lhs, const volume_t &rhs,
                const std::function<bool(volume_t::value_type,
                                         volume_t::value_type)> &pred) {
  CHECK(lhs.size() == rhs.size());
  auto li = std::begin(lhs);
  auto ri = std::begin(rhs);

  for (; li != std::end(lhs); ++li, ++ri) {
    if (pred((*li), (*ri)))
      return true;
  }
  return false;
}

inline bool all_eq(const volume_t &lhs, const volume_t &rhs) {
  return all(lhs, rhs, [](auto const &l, auto r) { return l == r; });
}

inline bool all_lt(const volume_t &lhs, const volume_t &rhs) {
  return all(lhs, rhs, std::less<>());
}

inline bool all_le(const volume_t &lhs, const volume_t &rhs) {
  return all(lhs, rhs, std::less_equal<long>());
}

inline bool all_gt(const volume_t &lhs, const volume_t &rhs) {
  return all(lhs, rhs, std::greater<>());
}

inline bool all_ge(const volume_t &lhs, const volume_t &rhs) {
  return all(lhs, rhs, std::greater_equal<>());
}

inline bool any_lt(const volume_t &lhs, const volume_t &rhs) {
  return any(lhs, rhs, std::less<>());
}

inline bool any_le(const volume_t &lhs, const volume_t &rhs) {
  return any(lhs, rhs, std::less_equal<>());
}

inline bool any_gt(const volume_t &lhs, const volume_t &rhs) {
  return any(lhs, rhs, std::greater<>());
}

inline bool any_ge(const volume_t &lhs, const volume_t &rhs) {
  return any(lhs, rhs, std::greater_equal<>());
}

inline volume_t diff(const volume_t &lhs, const volume_t &rhs) {
  return (lhs - rhs).apply([](auto const &v) { return v > 0 ? v : 0; });
}

template <typename C> void erase_all_expired(C &collection) {
  collection.erase(std::remove_if(collection.begin(), collection.end(),
                                  [](auto &ptr) { return ptr.expired(); }),
                   collection.end());
}

template <class InputIt, class OutputIt>
void lock_all(InputIt first, InputIt last, OutputIt d_first) {
  while (first != last) {
    if (auto locked = (*first++).lock()) {
      *d_first++ = locked;
    }
  }
}

template <class K, class T> class Factory {
public:
  template <typename... Args>
  std::shared_ptr<T> get(const K &name, Args &&... args) {

    auto found = ptrs_.find(name);
    if (found != ptrs_.end()) {
      if (auto locked = (*found).second.lock()) {
        return locked;
      } else {
        ptrs_.erase((*found).first);
      }
    }

    CHECK(ptrs_.find(name) == ptrs_.end());
    auto ptr = std::make_shared<T>(std::forward<Args>(args)...);
    ptrs_.emplace(name, ptr);
    return ptr;
  }

  void erase_expired() {
    for (auto it = ptrs_.begin(); it != ptrs_.end();) {
      if ((*it).second.expired()) {
        it = ptrs_.erase(it);
      } else {
        ++it;
      }
    }
  }

private:
  std::unordered_map<K, std::weak_ptr<T>> ptrs_;
};

inline object_id_t make_id() {
  static std::atomic<object_id_t> id;
  return ++id;
}

struct Trace {
  std::string action;
  std::string data;
};

class Event {
public:
  virtual ~Event() = default;
  virtual void flush() {}
  virtual std::optional<Trace> trace() const { return std::nullopt; }

  static std::shared_ptr<spdlog::logger> logger;
};

typedef std::function<void(const std::string &name, const Trace &trace)>
    trace_clbk_t;

namespace defaults {

utilization_t constexpr max_util = std::numeric_limits<utilization_t>::max();
utilization_t constexpr min_util = -max_util;
utilization_t constexpr eps = std::numeric_limits<utilization_t>::epsilon();
rank_t constexpr default_rank = 100;
rank_t constexpr unplaced_rank = std::numeric_limits<rank_t>::max();
labels_t constexpr default_label = 1;
traits_t constexpr invalid_trait = 1;
affinity_limits_t::value_type constexpr default_affinity_limit =
    std::numeric_limits<affinity_limits_t::value_type>::max();
char constexpr default_allocation[] = "/_default";
char constexpr default_partition[] = "_default";
} // namespace defaults

} // namespace scheduler
} // namespace treadmill

#endif // TREADMILL_SCHEDULER_COMMON_H
