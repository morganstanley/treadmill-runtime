#include <treadmill/scheduler.h>

#include <mutex>
#include <numeric>
#include <tuple>

namespace treadmill {
namespace scheduler {

static float utilization(const volume_t &demand, const volume_t &allocated,
                         const volume_t &available) {
  CHECK(demand.size() == allocated.size());
  CHECK(demand.size() == available.size());

  auto util = defaults::min_util;

  auto idemand = std::begin(demand);
  auto iallocated = std::begin(allocated);
  auto iavailable = std::begin(available);
  auto end = std::end(demand);
  while (idemand != end) {
    util = fmax(util,
                (*idemand++ - *iallocated++) / (*iavailable++ + defaults::eps));
  }
  return util;
}

std::vector<AllocationEntry> Allocation::priv_queue() {
  std::vector<std::shared_ptr<Application>> apps;
  {
    std::unique_lock<std::mutex> lock(apps_mx_);
    erase_all_expired(apps_);
    lock_all(apps_.begin(), apps_.end(), std::back_inserter(apps));
  }

  for (auto &app : apps) {
    app->assigned_alloc(name());
    app->labels(labels());
    app->lock_priority();
  }

  std::sort(apps.begin(), apps.end(), [](auto const &lhs, auto const &rhs) {
    return std::tuple(-lhs->locked_prio(), !lhs->is_scheduled(),
                      lhs->instance_id()) < std::tuple(-rhs->locked_prio(),
                                                       !rhs->is_scheduled(),
                                                       rhs->instance_id());
  });

  std::vector<AllocationEntry> q;

  auto util_before = defaults::min_util;
  auto util_after = defaults::min_util;

  volume_t acc_demand(volume_t::value_type(0), reserved_.size());
  for (auto &app : apps) {

    acc_demand += app->demand();
    if (app->priority() == 0) {
      util_before = defaults::max_util;
      util_after = defaults::max_util;
    } else {
      util_after = utilization(acc_demand, reserved_, reserved_);
    }

    if (util_after <= (max_utilization_ - 1)) {
      auto rank = rank_;
      if (util_after <= 0) {
        rank -= rank_adjustment_;
      }
      q.push_back({rank, util_before, util_after, app->is_pending(), app});
    } else {
      app->release_identity();
      app->release_placement();
    }

    util_before = util_after;
  }

  return q;
}

volume_t Allocation::total_reserved() const {
  auto total = reserved_;
  for (auto sub_a : sub_allocs()) {
    total += sub_a->total_reserved();
  }
  return total;
}

std::vector<AllocationEntry> Allocation::queue(const volume_t &free_capacity) {
  volume_t total_r = total_reserved();
  std::vector<AllocationEntry> q = priv_queue();
  for (auto &sub_alloc : sub_allocs()) {
    auto size = q.size();
    auto child_q = sub_alloc->queue(free_capacity);
    std::copy(child_q.begin(), child_q.end(), std::back_inserter(q));
    std::inplace_merge(q.begin(), q.begin() + size, q.end());
  }

  volume_t acc_demand(volume_t::value_type(0), reserved_.size());
  volume_t available = total_r + free_capacity;

  auto util_before = utilization(acc_demand, total_r, available);

  for (auto &entry : q) {

    acc_demand += entry.app->demand();
    if (entry.app->priority() == 0) {
      entry.before = defaults::max_util;
      entry.after = defaults::max_util;
    } else {
      entry.before = util_before;
      entry.after = utilization(acc_demand, total_r, available);
    }

    util_before = entry.after;
  }

  return q;
}

void Allocation::add_app(const std::shared_ptr<Application> app) {
  std::unique_lock<std::mutex> lock(apps_mx_);
  apps_.push_back(app);
}

void Allocation::add_sub_alloc(const std::shared_ptr<Allocation> alloc) {
  std::unique_lock<std::mutex> lock(sub_allocs_mx_);
  sub_allocs_by_name_.emplace(alloc->name(), alloc);
  sub_allocs_.push_back(alloc);
}

std::vector<std::shared_ptr<Allocation>> Allocation::sub_allocs() const {
  std::unique_lock<std::mutex> lock(sub_allocs_mx_);
  return sub_allocs_;
}

std::shared_ptr<Allocation>
Allocation::get_or_add_sub_alloc(const std::string &name) {
  auto it = sub_allocs_by_name_.find(name);
  if (it != sub_allocs_by_name_.end()) {
    return it->second;
  }

  auto sub_alloc = std::make_shared<Allocation>(name, empty_, labels_, rank_,
                                                rank_adjustment_, traits_);
  add_sub_alloc(sub_alloc);
  return sub_alloc;
}

void Allocation::add(const std::shared_ptr<Application> app) {
  std::shared_ptr<Allocation> sub_alloc(nullptr);

  if (allocation_group_policy_ &&
      allocation_group_policy_.value() == AllocationGroupPolicy::fair_sharing) {
    auto pos = app->name().find('@');
    if (pos != std::string::npos) {
      auto user = app->name().substr(0, pos);
      sub_alloc = get_or_add_sub_alloc(name_ + "/" + user);
    }
  }

  if (sub_alloc) {
    sub_alloc->add_app(app);
  } else {
    add_app(app);
  }
}

void DefaultAllocation::add(const std::shared_ptr<Application> app) {
  auto app_prefix = app->name().substr(0, app->name().find('.'));
  auto pos = app_prefix.find('@');
  if (pos != std::string::npos) {
    app_prefix = app_prefix.substr(0, pos);
  }
  auto sub_alloc = get_or_add_sub_alloc(name_ + name_ + "/" + app_prefix);
  sub_alloc->add_app(app);
}

inline bool check_capacity(const Node *node, const Application *app) {
  return all_le(app->demand(), node->capacity());
}

inline bool check_lease(const Node *node, const Application *app,
                        const time_point_t now) {
  return app->lease_ok(node->valid_until(), now);
}

inline bool check_labels(const Node *node, const Application *app) {
  return node->has_labels(app->labels());
}

inline bool check_traits(const Node *node, const Application *app) {
  return node->has_traits(app->traits());
}

inline bool check_required_traits(const Node *node, const Application *app) {
  return app->has_traits(node->required_traits());
}

inline bool check_affinity(const Node *node, const Application *app) {
  return node->affinity(app->affinity()) < app->affinity_limit(node->level());
}

Node::Node(const std::string &name, const ServerState state)
    : name_(name), level_(-1),
      required_traits_(Accumulated<traits_t, traits_and_t>(traits_t().set())),
      current_state_(state), intended_state_(state), id_(make_id()) {
  logger->info("created: {:016x} {}", id(), name_);
}

Node::Node(const std::string &name, const labels_t labels,
           const traits_t &traits, const traits_t &required_traits,
           const time_point_t valid_until, const volume_t &capacity,
           const ServerState state, const time_point_t state_since)
    : name_(name), level_(-1), labels_(labels), traits_(traits),
      required_traits_(required_traits), valid_until_(valid_until),
      capacity_(capacity), current_state_(state), intended_state_(state),
      state_since_(state_since), id_(make_id()) {
  logger->info("created: {:016x} {}", id(), name_);
}

Node::~Node() { logger->info("deleted: {:016x} {}", id(), name_); }

PlacementConstraints
Node::check_app_constraints(const std::shared_ptr<Application> app,
                            const time_point_t now) const {
  CHECK(app);
  const Application *raw = app.get();

  if (!check_lease(this, raw, now))
    return PlacementConstraints::lease;
  if (!check_labels(this, raw))
    return PlacementConstraints::labels;
  if (!check_traits(this, raw))
    return PlacementConstraints::traits;
  if (!check_required_traits(this, raw))
    return PlacementConstraints::required_traits;
  if (!check_affinity(this, raw))
    return PlacementConstraints::affinity;
  if (!check_capacity(this, raw))
    return PlacementConstraints::capacity;
  return PlacementConstraints::success;
}

bool Bucket::put(std::shared_ptr<Application> app, const time_point_t now) {
  auto placement_constraint = check_app_constraints(app, now);
  if (placement_constraint != PlacementConstraints::success) {
    app->record<PlacementConstraintEvent>(id(), placement_constraint);
    return false;
  }

  auto strategy = affinity_strategy(app->affinity());
  auto node_count = count();
  auto node = strategy->suggested();
  for (size_t i = 0; i < node_count; ++i) {
    if (node == nullptr) {
      node = strategy->next();
      continue;
    }

    if (node->state() == ServerState::up && node->put(app, now)) {
      return true;
    }
    node = strategy->next();
  }

  return false;
}

bool Server::put(std::shared_ptr<Application> app, const time_point_t now) {
  if (state() != ServerState::up) {
    app->record<NodeNotUpEvent>(id(), state());
    return false;
  }

  auto placement_constraint = check_app_constraints(app, now);
  if (placement_constraint != PlacementConstraints::success) {
    app->record<PlacementConstraintEvent>(id(), placement_constraint);
    return false;
  }

  app->server(std::dynamic_pointer_cast<Server>(shared_from_this()));
  return true;
}

std::shared_ptr<spdlog::logger> Event::logger;
std::shared_ptr<spdlog::logger> Node::logger;
std::shared_ptr<spdlog::logger> Application::logger;

void PendingEvent::flush() { logger->info(".. pending"); }
void AssignedEvent::flush() { logger->info(".. assigned to {}", alloc_); }
void ScheduledEvent::flush() { logger->info(".. scheduled on {}", server_); }
void BlacklistedEvent::flush() { logger->info(".. blacklisted"); }
void OfferPlacementEvent::flush() {
  logger->info(".. offer to: {:016x}", app_id_);
}
void EvictedEvent::flush() { logger->info(".. evicted"); }
void NeedPlacementEvent::flush() { logger->info(".. need placement"); }
void ServerStateEvent::flush() {
  auto state = [this]() {
    switch (state_) {
    case ServerState::up:
      return "up";
    case ServerState::down:
      return "down";
    case ServerState::frozen:
      return "frozen";
    case ServerState::deleted:
      return "deleted";
    default:
      CHECK(false);
    }
  };
  logger->info(".. {}", state());
}
void ServerBlackoutEvent::flush() { logger->info(".. blackout"); }
void ServerBlackoutClearedEvent::flush() {
  logger->info(".. blackout cleared");
}
void PlacementConstraintEvent::flush() {
  auto constraint = [this]() {
    switch (constraint_) {
    case PlacementConstraints::lease:
      return "lease";
    case PlacementConstraints::traits:
      return "traits";
    case PlacementConstraints::required_traits:
      return "required_traits";
    case PlacementConstraints::affinity:
      return "affinity";
    case PlacementConstraints::labels:
      return "labels";
    case PlacementConstraints::capacity:
      return "capacity";
    case PlacementConstraints::success:
      return "success";
    default:
      CHECK(false);
    }
  };
  logger->info(".. {:016x}: {}", node_id_, constraint());
}
void NodeNotUpEvent::flush() {
  auto state = [this]() {
    switch (state_) {
    case ServerState::up:
      return "up";
    case ServerState::down:
      return "down";
    case ServerState::frozen:
      return "frozen";
    case ServerState::deleted:
      return "deleted";
    default:
      CHECK(false);
    }
  };
  logger->info(".. node {}: {}", node_id_, state());
}
void NeedIdentityEvent::flush() { logger->info(".. identity"); }

Application::Application(const std::string &name, const priority_t priority,
                         const volume_t &demand, const Affinity &affinity,
                         const duration_t lease,
                         const duration_t data_retention_timeout,
                         const Identity identity,
                         const std::optional<std::string> &identity_group_name,
                         const traits_t traits, labels_t labels,
                         bool schedule_once, bool renew, bool blacklisted)
    : name_(name), priority_(priority), demand_(demand), affinity_(affinity),
      lease_(lease), data_retention_timeout_(data_retention_timeout),
      identity_(identity), identity_group_name_(identity_group_name),
      traits_(traits), labels_(labels), state_(AppState::pending),
      schedule_once_(schedule_once), unschedule_(false), renew_(renew),
      blacklisted_(blacklisted), id_(make_id()) {
  logger->info("created: {:016x} {}", id(), name_);
  instance_id_ = std::stol(name.substr(name.find("#") + 1));
}

Application::~Application() {
  logger->info("deleted: {:016x} {}", id(), name_);
  release_placement();
  release_identity();
}

std::optional<why_t> Application::need_placement(const time_point_t now) {
  if (blacklisted()) {
    record<BlacklistedEvent>();
    release_placement();
    release_identity();
    return std::nullopt;
  }

  if (identity_group_) {
    auto identity_group = identity_group_.value().lock();
    if (identity_group) {
      if (!identity_) {
        release_placement();
        return why_t{Why::identity};
      } else if (identity_.value() >= identity_group->count()) {
        release_placement();
        release_identity();
        return why_t{Why::identity};
      }
    } else {
      // Identity group was deleted.
      identity_group_ = std::nullopt;
    }
  }

  auto server = server_.lock();

  if (identity_group_name_ && !identity_group_) {
    record<NeedIdentityEvent>();
    if (server) {
      // Since app had placement, create an extra event that it's now pending.
      record<PendingEvent>("identity");
    }
    release_placement();
    release_identity();
    return std::nullopt;
  }

  if (!server) {
    release_placement();
    if (is_evicted()) {
      // Don't release identity yet.
      // App that was temporary evicted should first try to restore placement.
      return why_t{Why::evicted};
    } else {
      release_identity();
      return why_t{Why::pending};
    }
  }

  auto state = server->state();
  auto since = server->since();
  if (state == ServerState::down) {
    auto data_expires_at = data_retention_expires_at(since);
    if (now >= data_expires_at) {
      release_placement();
      release_identity();
      return why_t{Why::server_down, server->name()};
    }
  }

  if (state == ServerState::frozen) {
    if (unschedule()) {
      release_placement();
      release_identity();
      return why_t{Why::server_frozen, server->name()};
    }
  }

  if (renew_) {
    if (lease_ok(server->valid_until(), now)) {
      record_placement_time(true);
      return std::nullopt;
    } else {
      return why_t{Why::renew};
    }
  }

  return std::nullopt;
}

Partition::Partition(const std::string &name) : name_(name) {}

Cell::placement_t Cell::current_placement() {
  Cell::placement_t placement;
  for (auto &alloc : allocs_) {
    auto queue = alloc->queue(size(alloc->labels()));
    for (auto entry = queue.begin(); entry != queue.end(); ++entry) {
      auto &app = (*entry).app;
      if (!app) {
        continue;
      }

      placement.push_back(app);
    }
  }

  return placement;
}

Cell::placement_t Cell::schedule() {

  apply_allocations();

  // apply pending state for each node.
  apply_state();

  Cell::placement_t placement;
  for (auto &alloc : allocs_) {
    auto p = schedule_alloc(alloc);
    std::copy(p.begin(), p.end(), std::back_inserter(placement));
  }

  return placement;
}

Cell::placement_t Cell::schedule_alloc(std::shared_ptr<Allocation> alloc) {
  CHECK(alloc);
  const auto &queue = alloc->queue(size(alloc->labels()));
  Event::logger->info("Schedule alloc: {} queue: {}", alloc->name(),
                      queue.size());
  return schedule_queue(queue);
}

Cell::placement_t
Cell::schedule_queue(const std::vector<AllocationEntry> &queue) {

  Cell::placement_t placement;

  auto now = std::chrono::system_clock::now();
  PlacementFeasibilityTracker tracker;

  for (auto entry = queue.begin(); entry != queue.end(); ++entry) {
    auto &app = entry->app;
    if (!app) {
      continue;
    }

    SPDLOG_INFO("Scheduling: {}", app->name());

    placement.push_back(app);

    auto why = app->need_placement(now);
    if (!why) {
      continue;
    }

    // Apps that were temporary evicted in the earlier cycles, try to restore
    // them on the server they were running on.
    if (app->restore_placement()) {
      continue;
    } else {
      if (!app->renew()) {
        app->release_identity();
      }
    }

    if (app->is_evicted() && app->schedule_once()) {
      record_schedule_event(*app, why.value());
      continue;
    }

    if (!app->acquire_identity()) {
      app->record<NeedIdentityEvent>();
      continue;
    }

    if (!tracker.is_feasible(*app)) {
      SPDLOG_INFO("Placement not feasible: {}", app->name());
      record_schedule_event(*app, why.value());
      continue;
    }

    // Try to find new placement, if succeeded, nothing to be done.
    if (put(app, now)) {
      record_schedule_event(*app, why.value());
      continue;
    }

    // Placement not found, evict apps from the right of the queue.
    bool placement_failed = true;
    for (auto rentry = queue.rbegin(); rentry != queue.rend(); ++rentry) {
      auto &eviction_candidate = rentry->app;
      // We reached the app we can't place.
      if (eviction_candidate == app) {
        break;
      }
      // We reached an app with the same rank, within reservation.
      if ((rentry->rank == entry->rank) && (rentry->after <= 0)) {
        break;
      }
      auto server = eviction_candidate->offer_placement();
      if (server) {
        eviction_candidate->record<OfferPlacementEvent>(app->id());
        if (server->put(app, now)) {
          placement_failed = false;
          break;
        }
      }
    }

    if (placement_failed) {
      SPDLOG_INFO("Placement not found: {}", app->name());
      app->release_identity();
      tracker.record_failed(*app);
    }

    record_schedule_event(*app, why.value());
  }

  return placement;
}

void Cell::record_schedule_event(Application &app, const why_t &why) {
  std::optional<std::string> reason;

  if (why.reason == Why::evicted) {
    reason = "evicted";
  } else if (why.reason == Why::server_down) {
    CHECK(why.data);
    reason = fmt::format("{}:down", why.data.value());
  } else if (why.reason == Why::server_frozen) {
    CHECK(why.data);
    reason = fmt::format("{}:frozen", why.data.value());
  }

  auto server = app.server().get();
  if (server) {
    // App needed placement, and it's now scheduled.
    app.record<ScheduledEvent>(server->name(), reason);
  } else {
    // App needed placement, but it's pending.
    // In this case only publish event if it was caused by any of the above.
    // We don't want to keep publishing this event if app just stays pending.
    if (reason) {
      app.record<PendingEvent>(reason);
    }
  }
}

bool PlacementFeasibilityTracker::record_failed(const Application &app) {

  duration_t lease = app.lease() ? app.lease().value() : duration_t{0};
  auto demand = app.demand();
  auto limits = app.affinity_limits();
  std::string affinity = app.affinity();

  bool recorded = false;

  auto traits_tracker = failed_placements_.find(app.traits());
  if (traits_tracker == failed_placements_.cend()) {
    // new traits.
    failed_placements_.emplace(
        std::make_pair<traits_t, PlacementFeasibilityTracker::AffinityTracker>(
            app.traits(), {{affinity, PlacementFeasibilityTracker::Shape{
                                          demand, limits, lease}}}));
    recorded = true;
  } else {
    auto affinity_tracker = traits_tracker->second.find(affinity);
    if (affinity_tracker == traits_tracker->second.end()) {
      // new
      traits_tracker->second.emplace(std::make_pair(
          affinity, PlacementFeasibilityTracker::Shape{demand, limits, lease}));
      recorded = true;
    } else {
      auto &shape = affinity_tracker->second;
      // replace if failed placement strictly less-equal than current.
      if (lease <= shape.lease && all_le(demand, shape.demand) &&
          all_ge(limits, shape.affinity_limits)) {
        shape.lease = lease;
        shape.demand = demand;
        shape.affinity_limits = limits;
        recorded = true;
      }
    }
  }

  return recorded;
}

bool is_placement_feasible(const Application &app,
                           const PlacementFeasibilityTracker::Shape &shape) {
  // For placement to be not-feasible, application requirements need to be
  // strictly greater-equal than last failed placement.
  //
  // Demand must be >= recorded failed demans.
  // Affinity limits must be <= failed limits.
  // Lease >= than recorded lease.
  duration_t lease = app.lease() ? app.lease().value() : duration_t{0};
  if (lease >= shape.lease && all_ge(app.demand(), shape.demand) &&
      all_le(app.affinity_limits(), shape.affinity_limits)) {
    return false;
  }

  return true;
}

bool PlacementFeasibilityTracker::is_feasible(const Application &app) const {
  // if application is "new", placement is feasible.
  auto const traits_tracker = failed_placements_.find(app.traits());
  if (traits_tracker == failed_placements_.cend()) {
    return true;
  }

  auto const failed = (*traits_tracker).second.find(app.affinity());
  if (failed == (*traits_tracker).second.cend()) {
    return true;
  }

  auto &shape = failed->second;
  return is_placement_feasible(app, shape);
}

} // namespace scheduler
} // namespace treadmill
