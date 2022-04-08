#ifndef TREADMILL_SCHEDULER_APPLICATION_H
#define TREADMILL_SCHEDULER_APPLICATION_H

#include <variant>

#include <treadmill/convert.h>
#include <treadmill/scheduler/common.h>
#include <treadmill/scheduler/identity_group.h>
#include <treadmill/scheduler/node.h>

namespace treadmill {
namespace scheduler {

class Server;

struct Affinity {
  Affinity() = default;
  Affinity(const std::string &name, const affinity_limits_t &limits = {})
      : name_(name), limits_(limits) {}

  affinity_limits_t::value_type get(size_t level) const {
    if (level >= limits_.size()) {
      return defaults::default_affinity_limit;
    }
    return limits_[level];
  }

  const std::string name() const { return name_; }

  std::string name_;
  affinity_limits_t limits_;
};

class PendingEvent : public Event {
public:
  PendingEvent(std::optional<std::string> reason)
      : reason_(std::move(reason)) {}

  void flush() override;
  std::optional<Trace> trace() const override {
    if (reason_) {
      return std::make_optional<Trace>(Trace{"pending", reason_.value()});
    } else {
      return std::make_optional<Trace>(Trace{"pending", ""});
    }
  }

private:
  std::optional<std::string> reason_;
};

class AssignedEvent : public Event {
public:
  AssignedEvent(const std::string &alloc) {
    alloc_ = alloc.substr(1);
    std::replace(alloc_.begin(), alloc_.end(), '/', ':');
  }
  void flush() override;
  std::optional<Trace> trace() const override {
    return std::make_optional<Trace>(Trace{"assigned", alloc_});
  }

private:
  std::string alloc_;
};

class ScheduledEvent : public Event {
public:
  ScheduledEvent(std::string server) : server_(std::move(server)) {}
  ScheduledEvent(std::string server, std::optional<std::string> reason)
      : server_(std::move(server)), reason_(std::move(reason)) {}

  void flush() override;
  std::optional<Trace> trace() const override {
    if (reason_) {
      return std::make_optional<Trace>(
          Trace{"scheduled", fmt::format("{}:{}", server_, reason_.value())});
    } else {
      return std::make_optional<Trace>(Trace{"scheduled", server_});
    }
  }

private:
  std::string server_;
  std::optional<std::string> reason_;
};

class BlacklistedEvent : public Event {
public:
  void flush() override;
  std::optional<Trace> trace() const override {
    return std::make_optional<Trace>(Trace{"blacklisted", ""});
  }
};

class OfferPlacementEvent : public Event {
public:
  OfferPlacementEvent(object_id_t app_id) : app_id_(app_id) {}
  void flush() override;

private:
  object_id_t app_id_;
};

class EvictedEvent : public Event {
public:
  EvictedEvent() {}
  void flush() override;
};

class NeedPlacementEvent : public Event {
public:
  NeedPlacementEvent() {}
  void flush() override;
};

class PlacementConstraintEvent : public Event {
public:
  PlacementConstraintEvent(object_id_t node_id, PlacementConstraints constraint)
      : node_id_(node_id), constraint_(constraint) {}
  void flush() override;

private:
  object_id_t node_id_;
  PlacementConstraints constraint_;
};

class NodeNotUpEvent : public Event {
public:
  NodeNotUpEvent(object_id_t node_id, ServerState state)
      : node_id_(node_id), state_(state) {}
  void flush() override;

private:
  object_id_t node_id_;
  ServerState state_;
};

class NeedIdentityEvent : public Event {
public:
  NeedIdentityEvent() {}
  void flush() override;
};

enum class AppState {
  pending = 0x1,
  scheduled = 0x2,
  evicted = 0x4,
};

BITMASK_DEFINE_VALUE_MASK(AppState, -1);

enum class Why {
  pending,
  evicted,
  identity,
  server_down,
  server_frozen,
  renew
};

struct why_t {
  Why reason;
  std::optional<std::string> data;
};

class Application : public std::enable_shared_from_this<Application> {
public:
  static std::shared_ptr<spdlog::logger> logger;

  Application(
      const std::string &name, const priority_t priority,
      const volume_t &demand, const Affinity &affinity,
      const duration_t lease = duration_t(0),
      const duration_t data_retention_timeout = duration_t(0),
      const Identity identity = std::nullopt,
      const std::optional<std::string> &identity_group_name = std::nullopt,
      const traits_t traits = {0}, labels_t labels = {0},
      bool schedule_once = false, bool renew = false, bool blacklisted = false);

  ~Application();

  auto id() const { return id_; }

  std::shared_ptr<IdentityGroup> identity_group() {
    if (!identity_group_) {
      return nullptr;
    }
    return identity_group_.value().lock();
  }

  void identity_group(const std::shared_ptr<IdentityGroup> identity_group) {
    identity_group_ = identity_group;
    if (identity_) {
      identity_ = identity_group->acquire(identity_);
    }
  }

  bool acquire_identity() {
    if (!identity_group_) {
      return true;
    }

    if (identity_) {
      return true;
    }

    auto igroup = identity_group_.value().lock();
    if (igroup) {
      identity_ = igroup->acquire();
    }

    return (bool)identity_;
  }

  bool acquire_identity(const Identity &identity) {
    CHECK(identity);

    if (!identity_group_) {
      return false;
    }

    if (auto igroup = identity_group_.value().lock()) {
      if (igroup->acquire(identity)) {
        identity_ = identity;
        return true;
      }
    }

    return false;
  }

  void release_identity() {
    if (identity_ && identity_group_) {
      if (auto igroup = identity_group_.value().lock()) {
        igroup->release(identity_);
      }
    }
    identity_ = std::nullopt;
  }

  void identity(const Identity &identity) { identity_ = identity; }
  Identity identity() const { return identity_; }

  bool has_identity() const { return (bool)identity_; }

  Identity identity_count() const {
    if (identity_group_) {
      if (auto igroup = identity_group_.value().lock()) {
        return igroup->count();
      }
    }
    return std::nullopt;
  }

  priority_t priority() const { return priority_; }
  void priority(priority_t priority) { priority_ = priority; }

  priority_t locked_prio() const { return locked_priority_; }
  void lock_priority() { locked_priority_ = priority_; }

  std::string name() const { return name_; }

  long instance_id() const { return instance_id_; }

  auto assigned_alloc() const { return assigned_alloc_; }
  void assigned_alloc(std::optional<std::string> assigned_alloc) {
    if (assigned_alloc && (!assigned_alloc_ ||
                           assigned_alloc.value() != assigned_alloc_.value())) {
      record<AssignedEvent>(assigned_alloc.value());
    }
    assigned_alloc_ = assigned_alloc;
  }

  bool is_scheduled() const {
    bool rc = false;
    if (auto s = server_.lock()) {
      rc = s->state() == ServerState::up || s->state() == ServerState::frozen;
    }
    return rc;
  }
  bool is_pending() const {
    return (state_ & AppState::pending) == AppState::pending;
  }
  bool is_evicted() const {
    return (state_ & AppState::evicted) == AppState::evicted;
  }
  void clear_evicted() { state_ &= ~AppState::evicted; }

  bool schedule_once() const { return schedule_once_; }

  const volume_t &demand() const { return demand_; }

  auto affinity_limit(size_t level) const { return affinity_.get(level); }

  const std::string affinity() const { return affinity_.name(); }
  const affinity_limits_t affinity_limits() const { return affinity_.limits_; }

  traits_t traits() const { return traits_; }
  bool has_traits(const traits_t &traits) const {
    return (traits_ & traits) == traits;
  }

  labels_t labels() const { return labels_; }
  void labels(const labels_t &labels) { labels_ = labels; }

  bool renew() const { return renew_; }
  void renew(bool renew) { renew_ = renew; }

  const auto lease() const { return lease_; };
  const auto data_retention_timeout() const { return data_retention_timeout_; };

  void record_placement_time(bool renew = false) {
    if (renew || !expires_at_) {
      auto now = std::chrono::system_clock::now();
      expires_at_ = (lease_) ? now + lease_.value() : now;
    }
  }

  bool lease_ok(const time_point_t &until, const time_point_t now) const {
    if (!lease_ || lease_.value() == duration_t::zero()) {
      return true;
    }
    if (until.time_since_epoch() == time_point_t::duration::zero()) {
      return false;
    }
    return lease_.value() < until - now;
  }

  std::shared_ptr<Server> server() { return server_.lock(); }
  void server(const std::shared_ptr<Server> server) {
    release_placement();
    record_placement(server);
  }

  void release_placement() {
    if (auto server = server_.lock()) {
      server->inc_capacity(demand());
      server->dec_affinity(affinity());
    }

    server_.reset();

    if (state_ & AppState::scheduled) {
      state_ = AppState::pending | AppState::evicted;
    }

    unschedule_ = false;
  }

  std::shared_ptr<Server> offer_placement() {
    auto locked = server();
    if (locked && locked->state() == ServerState::up) {
      CHECK(expires_at_);
      saved_placement_ = saved_placement_t{locked, expires_at_.value()};
      release_placement();
    } else {
      locked.reset();
    }

    return locked;
  }

  bool unschedule() const { return unschedule_; }
  void unschedule(bool unschedule) { unschedule_ = unschedule; }

  auto data_retention_expires_at(const time_point_t since) const {
    return !data_retention_timeout_ ? since
                                    : since + data_retention_timeout_.value();
  }

  bool blacklisted() const { return blacklisted_; }
  void blacklisted(bool blacklisted) { blacklisted_ = blacklisted; }

  std::optional<why_t> need_placement(const time_point_t now);
  bool restore_placement() {
    if (!saved_placement_) {
      return false;
    }

    auto server = saved_placement_.value().server;
    auto expires_at = saved_placement_.value().expires_at;
    saved_placement_ = std::nullopt;

    if (server->affinity(affinity()) >= affinity_limit(server->level())) {
      record<EvictedEvent>();
      return false;
    }

    if (!all_le(demand(), server->capacity())) {
      record<EvictedEvent>();
      return false;
    }

    record_placement(server);
    expires_at_ = expires_at;
    return true;
  }

  void expires_at(time_point_t expires_at) { expires_at_ = expires_at; }
  auto expires_at() const { return expires_at_; }

  template <typename Event, typename... Args> void record(Args &&... args) {
    app_events_.push_back(std::make_unique<Event>(std::forward<Args>(args)...));
  }

  void
  flush_events(const std::optional<trace_clbk_t> trace_clbk = std::nullopt) {
    if (!app_events_.empty()) {
      Event::logger->info("{}", name());
      std::for_each(app_events_.begin(), app_events_.end(),
                    [](auto &ev) { ev->flush(); });
      if (trace_clbk) {
        std::for_each(app_events_.begin(), app_events_.end(),
                      [this, trace_clbk](auto &ev) {
                        auto trace = ev->trace();
                        if (trace) {
                          trace_clbk.value()(name(), trace.value());
                        }
                      });
      }
      app_events_.clear();
    }
  }

private:
  void record_placement(const std::shared_ptr<Server> server) {
    CHECK(server);
    record_placement_time();
    server->dec_capacity(demand());
    server->inc_affinity(affinity());
    server_ = server;
    state_ = AppState::scheduled;
    saved_placement_ = std::nullopt;
  }

  std::string name_;
  long instance_id_;
  priority_t priority_;
  priority_t locked_priority_;
  volume_t demand_;
  Affinity affinity_;
  std::optional<duration_t> lease_;
  std::optional<duration_t> data_retention_timeout_;
  std::optional<time_point_t> expires_at_;

  std::weak_ptr<Server> server_;

  Identity identity_;
  std::optional<std::string> identity_group_name_;
  std::optional<std::weak_ptr<IdentityGroup>> identity_group_;
  traits_t traits_;
  labels_t labels_;

  bitmask::bitmask<AppState> state_;

  bool schedule_once_;
  bool unschedule_;
  bool renew_;
  bool blacklisted_;

  std::optional<std::string> assigned_alloc_;

  struct saved_placement_t {
    std::shared_ptr<Server> server;
    time_point_t expires_at;
  };

  std::optional<saved_placement_t> saved_placement_;

  void save_placement(const std::shared_ptr<Server> &server) {
    CHECK(expires_at_);
    CHECK(server);
    saved_placement_ = saved_placement_t{server, expires_at_.value()};
    release_placement();
  }

  object_id_t id_;
  std::vector<std::unique_ptr<Event>> app_events_;
};

class PlacementFeasibilityTracker {
public:
  bool record_failed(const Application &app);
  bool is_feasible(const Application &app) const;

  struct Shape {
    volume_t demand;
    affinity_limits_t affinity_limits;
    duration_t lease;
  };

private:
  typedef std::unordered_map<std::string, Shape> AffinityTracker;
  std::unordered_map<traits_t, AffinityTracker> failed_placements_;
};

} // namespace scheduler
} // namespace treadmill

#endif // TREADMILL_SCHEDULER_APPLICATION_H
