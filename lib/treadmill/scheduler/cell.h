#pragma once

#include <regex>
#include <treadmill/scheduler/node.h>

namespace treadmill {
namespace scheduler {

inline std::string assignment_key(const std::string &pattern) {
  // Construct assignment key based on app pattern/name.
  auto key = pattern.substr(0, pattern.find('.'));
  auto pos = key.find('@');
  if (pos != std::string::npos) {
    key = key.substr(pos + 1);
  }
  return key;
}

class Application;
class IdentiyGroup;
class Allocation;

class Partition {
public:
  Partition(const std::string &name);
  ~Partition() {}

private:
  std::string name_;
};

class Assignment {
public:
  Assignment() : re_(std::regex(".*")), priority_(1) {}

  Assignment(const std::regex &re, priority_t priority,
             const std::shared_ptr<Allocation> alloc)
      : re_(re), priority_(priority), alloc_(alloc) {}

  bool match(const std::string &name, const std::shared_ptr<Application> app,
             bool check_traits) {
    if (!std::regex_match(name, re_)) {
      return false;
    }
    if (check_traits && alloc_ &&
        ((alloc_->traits() & app->traits()) != app->traits())) {
      return false;
    }

    if (app->priority() <= 0) {
      app->priority(priority_);
    }
    if (alloc_) {
      alloc_->add(app);
    }
    return true;
  }

private:
  std::regex re_;
  priority_t priority_;
  std::shared_ptr<Allocation> alloc_;
};

struct Placement {
  std::string app;
  std::optional<std::string> old_server;
  std::optional<time_point_t> old_expires_at;
  std::optional<std::string> new_server;
  std::optional<time_point_t> new_expires_at;
  Identity identity;
  Identity identity_count;
  bool schedule_once;
  std::string assigned_alloc;
};

class Cell : public Bucket {
public:
  typedef sorted_string_map_t<std::shared_ptr<Partition>> partitions_t;
  typedef std::vector<std::shared_ptr<Allocation>> allocations_t;
  typedef string_map_t<std::vector<Assignment>> assignments_t;
  typedef std::vector<std::shared_ptr<Application>> placement_t;

  Cell(std::string name, int ndim)
      : Bucket(name, volume_t(ndim)), next_event_at_{
                                          std::chrono::system_clock::now()} {}
  ~Cell() {}

  placement_t schedule();
  placement_t schedule_queue(const std::vector<AllocationEntry> &queue);
  placement_t current_placement();

  void assignments(assignments_t assignments) {
    assignments_ = std::move(assignments);
  }

  void default_assignment(const Assignment &assignment) {
    default_assignment_ = assignment;
  }

  void allocations(allocations_t allocs) {
    std::unique_lock<std::mutex> lock(allocs_mx_);
    intended_allocs_ = std::move(allocs);
  }
  auto allocations() const { return allocs_; }

  void apply_allocations() {
    std::unique_lock<std::mutex> lock(allocs_mx_);
    if (!intended_allocs_.empty()) {
      allocs_ = std::move(intended_allocs_);
      intended_allocs_.clear();
    }
  }

  void assign(const std::shared_ptr<Application> app) {
    auto key = assignment_key(app->name());
    auto it = assignments_.find(key);

    if (it != assignments_.end()) {
      for (auto &assignment : it->second) {
        if (assignment.match(app->name(), app, true)) {
          return;
        }
      }
    }

    default_assignment_.match(app->name(), app, false);
  }

private:
  placement_t schedule_alloc(std::shared_ptr<Allocation> alloc);
  void record_schedule_event(Application &app, const why_t &why);

  time_point_t next_event_at_;
  partitions_t partitions_;
  std::mutex allocs_mx_;
  allocations_t allocs_;
  allocations_t intended_allocs_;
  assignments_t assignments_;
  Assignment default_assignment_;
};

struct SchedulerEvent {
  SchedulerEvent(const std::string &name) {}
};

} // namespace scheduler
} // namespace treadmill
