#ifndef TREADMILL_SCHEDULER_ALLOCATION_H
#define TREADMILL_SCHEDULER_ALLOCATION_H

#include <treadmill/scheduler/common.h>

namespace treadmill {
namespace scheduler {

class Application;

struct AllocationEntry {
  rank_t rank;
  utilization_t before;
  utilization_t after;
  bool pending;
  std::shared_ptr<Application> app;

  bool operator<(const AllocationEntry &rhs) const {
    return std::tie(rank, before, after) <
           std::tie(rhs.rank, rhs.before, rhs.after);
  }
};

enum class AllocationGroupPolicy { priority_ordering, fair_sharing };

inline std::optional<std::string> policy2str(AllocationGroupPolicy policy) {
  switch (policy) {
  case AllocationGroupPolicy::priority_ordering:
    return "priority-ordering";
  case AllocationGroupPolicy::fair_sharing:
    return "fair-sharing";
  default:
    CHECK(false);
    return std::nullopt;
  }
}

inline std::optional<AllocationGroupPolicy> str2policy(const std::string &str) {
  if (str == "priority-ordering") {
    return AllocationGroupPolicy::priority_ordering;
  } else if (str == "fair-sharing") {
    return AllocationGroupPolicy::fair_sharing;
  } else {
    return std::nullopt;
  }
}

class Allocation {
public:
  Allocation(const std::string &name, const volume_t &reserved,
             const labels_t labels = 0,
             const rank_t &rank = defaults::default_rank,
             const rank_t &rank_adjustment = 0, const traits_t &traits = 0,
             const utilization_t &max_utilization = defaults::max_util,
             const std::optional<AllocationGroupPolicy>
                 allocation_group_policy = std::nullopt)
      : name_(name), labels_(labels), reserved_(reserved),
        empty_(volume_t::value_type(0), reserved.size()), rank_(rank),
        rank_adjustment_(rank_adjustment), traits_(traits),
        max_utilization_(max_utilization),
        allocation_group_policy_(allocation_group_policy) {}

  virtual ~Allocation() {}

  std::string name() const { return name_; }

  void add_app(const std::shared_ptr<Application> app);

  void add_sub_alloc(const std::shared_ptr<Allocation> alloc);

  std::vector<std::shared_ptr<Allocation>> sub_allocs() const;

  virtual void add(const std::shared_ptr<Application> app);

  std::vector<AllocationEntry> queue(const volume_t &free_capacity);

  labels_t labels() const { return labels_; }
  void labels(labels_t labels) { labels_ = labels; }

  traits_t traits() const { return traits_; }
  void traits(traits_t traits) { traits_ = traits; }

  auto reserved() const { return reserved_; }

  auto rank() const { return rank_; }

  auto rank_adjustment() const { return rank_adjustment_; }

  auto max_utilization() const { return max_utilization_; }

  auto allocation_group_policy() const { return allocation_group_policy_; }

#ifdef UNIT_TEST
public:
#else
protected:
#endif

  volume_t total_reserved() const;

  std::vector<AllocationEntry> priv_queue();

  std::shared_ptr<Allocation> get_or_add_sub_alloc(const std::string &name);

  std::string name_;
  labels_t labels_;
  volume_t reserved_;
  volume_t empty_;
  rank_t rank_;
  rank_t rank_adjustment_;
  traits_t traits_;
  utilization_t max_utilization_;
  std::optional<AllocationGroupPolicy> allocation_group_policy_;

  std::mutex apps_mx_;
  mutable std::mutex sub_allocs_mx_;
  std::vector<std::weak_ptr<Application>> apps_;
  std::vector<std::shared_ptr<Allocation>> sub_allocs_;
  std::unordered_map<std::string, std::shared_ptr<Allocation>>
      sub_allocs_by_name_;
};

class DefaultAllocation : public Allocation {
public:
  DefaultAllocation(const volume_t &reserved)
      : Allocation(defaults::default_allocation, reserved,
                   defaults::default_label) {}

  virtual void add(const std::shared_ptr<Application> app) override;
};

} // namespace scheduler
} // namespace treadmill

#endif // TREADMILL_SCHEDULER_ALLOCATION_H
