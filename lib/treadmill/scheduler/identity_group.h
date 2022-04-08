#ifndef TREADMILL_SCHEDULER_IDENTITY_GROUP_H
#define TREADMILL_SCHEDULER_IDENTITY_GROUP_H

#include <mutex>
#include <treadmill/scheduler/common.h>

namespace treadmill {
namespace scheduler {

class IdentityGroup {
public:
  IdentityGroup(const std::string &name) : name_(name) {}
  IdentityGroup(const std::string &name, identity_t count)
      : name_(name), identities_(count) {
    SPDLOG_DEBUG("igroup:ctor {} {} {}", (void *)this, name_, count);
  }

  auto name() const { return name_; }

  identity_t count() const { return identities_.size(); }

  // Acquire first available identity.
  Identity acquire() {
    SPDLOG_DEBUG("igroup:acquire {} {} {}", (void *)this, name_,
                 identities_.size());
    std::unique_lock<std::mutex> lck(mx_);
    for (size_t i = 0; i < identities_.size(); ++i) {
      if (!identities_[i]) {
        identities_[i] = true;
        return std::make_optional(i);
      }
    }
    return std::nullopt;
  }

  // Acquire specific identity if it is free, otherwise, return nullopt.
  Identity acquire(const Identity &ident) {
    std::unique_lock<std::mutex> lck(mx_);
    if (!ident) {
      return ident;
    }

    SPDLOG_DEBUG("igroup:acquire(specific) {} {} {} {}", (void *)this, name_,
                 identities_.size(), ident.value());
    auto idx = ident.value();
    if (idx >= identities_.size()) {
      return std::nullopt;
    }

    if (!identities_[idx]) {
      identities_[idx] = true;
      return ident;
    }

    return std::nullopt;
  }

  void release(const Identity &ident) {
    std::unique_lock<std::mutex> lck(mx_);
    if (ident) {
      SPDLOG_DEBUG("igroup:release {} {} {} {}", (void *)this, name_,
                   identities_.size(), ident.value());
      if (ident.value() < identities_.size()) {
        identities_[ident.value()] = false;
      }
    }
  }

  void adjust(identity_t new_count) {
    std::unique_lock<std::mutex> lck(mx_);
    SPDLOG_DEBUG("igroup:adjust {} {} {} {}", (void *)this, name_,
                 identities_.size(), new_count);
    identities_.resize(new_count, false);
  }

private:
  std::string name_;
  std::mutex mx_;
  std::vector<bool> identities_;
};

} // namespace scheduler
} // namespace treadmill

#endif // TREADMILL_SCHEDULER_IDENTITY_GROUP_H
