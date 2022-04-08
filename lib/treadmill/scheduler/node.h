#pragma once

#include <deque>
#include <numeric>
#include <treadmill/scheduler/common.h>

namespace treadmill {
namespace scheduler {

enum class ServerState { up, down, frozen, deleted };

inline std::optional<std::string> state2str(ServerState state) {
  switch (state) {
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
    return std::nullopt;
  }
}

inline std::optional<ServerState> str2state(const std::string &str) {
  if (str == "up") {
    return ServerState::up;
  } else if (str == "down") {
    return ServerState::down;
  } else if (str == "frozen") {
    return ServerState::frozen;
  } else if (str == "deleted") {
    return ServerState::deleted;
  } else {
    return std::nullopt;
  }
}

struct affinity_inc_t;

class AffinityCounter {
public:
  typedef affinity_limits_t::value_type value_t;

  value_t get(const std::string &name) const {
    auto it = counters_.find(name);
    return it != counters_.end() ? (*it).second : 0;
  }

  void inc(const std::string &name, value_t value = 1) {
    // clang-tidy bug, does not process structured assignment.
    //
    // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
    auto [it, inserted] = counters_.emplace(name, value);
    if (!inserted) {
      ((*it).second) += value;
    }
  }

  void dec(const std::string &name, value_t value = 1) {
    auto it = counters_.find(name);
    CHECK(it != counters_.end());
    CHECK((*it).second >= value);

    (*it).second -= value;
    if ((*it).second == 0) {
      counters_.erase(it);
    }
  }

  void dec_all(const AffinityCounter &counter) {
    for (auto &[k, v] : counter.counters_) {
      auto it = counters_.find(k);
      CHECK(it != counters_.end());
      CHECK((*it).second >= v);
      (*it).second -= v;
      if ((*it).second == 0) {
        counters_.erase(it);
      }
    }
  }

  auto log() const {
    std::vector<char> summary;
    for (auto &[k, v] : counters_) {
      fmt::format_to(std::back_inserter(summary), " {}:{}", k, v);
    }
    summary.push_back(0);
    return summary;
  }

private:
  auto &counters() const { return counters_; };

  string_map_t<value_t> counters_;
  friend affinity_inc_t;
};

template <typename T> struct or_t {
  void operator()(T &self, const T &child) { self |= child; }
};

struct traits_and_t {
  void operator()(traits_t &self, const traits_t &child) { self &= child; }
};

struct time_max_t {
  void operator()(time_point_t &self, const time_point_t &child) {
    self = max(self, child);
  }
};

struct volume_max_t {
  void operator()(volume_t &self, const volume_t &child) {
    CHECK(self.size() == child.size());
    // clang-tidy bug, does not process structured assignment.
    //
    // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
    for (auto [si, ci] = std::make_pair(std::begin(self), std::cbegin(child));
         si != std::end(self); ++si, ++ci) {
      *si = std::max(*si, *ci);
    }
  }
};

struct affinity_inc_t {
  void operator()(AffinityCounter &self, const AffinityCounter &child) {
    for (auto &[k, v] : child.counters()) {
      self.inc(k, v);
    }
  }
};

enum class PlacementConstraints {
  success = 0,
  lease = 1 << 0,
  labels = 1 << 1,
  traits = 1 << 2,
  affinity = 1 << 3,
  capacity = 1 << 4,
  required_traits = 1 << 5
};

BITMASK_DEFINE_VALUE_MASK(PlacementConstraints, -1);

class Node : public std::enable_shared_from_this<Node> {
public:
  static constexpr time_point_t time_zero =
      std::chrono::time_point<std::chrono::system_clock>::min();

  static std::shared_ptr<spdlog::logger> logger;

  Node(const std::string &name, const ServerState state = ServerState::up);
  Node(const std::string &name, const labels_t labels, const traits_t &traits,
       const traits_t &required_traits, const time_point_t valid_until,
       const volume_t &capacity, const ServerState state = ServerState::up,
       const time_point_t state_since = std::chrono::system_clock::now());
  virtual ~Node();

  auto id() const { return id_; }
  auto name() const { return name_; }

  auto traits() const { return traits_.get(); }
  void traits(const traits_t &traits) {
    CHECK(traits_t{0} == traits_.get());
    traits_ = traits;
  }
  bool has_traits(const traits_t &traits) const {
    return (traits_.get() & traits) == traits;
  }

  auto required_traits() const { return required_traits_.get(); }
  void required_traits(const traits_t &required_traits) {
    required_traits_ = required_traits;
  }

  labels_t labels() const { return labels_; }
  void labels(const labels_t &labels) { labels_ = labels; }
  bool has_labels(const labels_t &labels) const {
    return (labels_.get() & labels) == labels;
  }

  auto valid_until() const { return valid_until_.get(); }
  void valid_until(const time_point_t &valid_until) {
    valid_until_ = valid_until;
  }

  auto capacity() const { return capacity_.get(); }
  void capacity(const volume_t &capacity) { capacity_ = capacity; }

  bool has_capacity(const volume_t &capacity) const {
    return all_le(capacity, capacity_);
  }

  void inc_capacity(const volume_t &delta) {
    capacity_.inc(delta);
    if (current_state_ != ServerState::up) {
      // If state is not up, topology tree starting at parent has already been
      // adjusted and recalculated to exclude this node as part of apply_state.
      SPDLOG_INFO("inc_capacity - state not up", name());
      return;
    }
    if (auto parent = parent_.lock()) {
      if (any_gt(capacity_, parent->capacity_)) {
        parent->inc_capacity(diff(capacity_, parent->capacity_));
      }
    }
  }

  void dec_capacity(const volume_t &delta) {
    auto parent = parent_.lock();
    if (!parent || all_lt(capacity_, parent->capacity_)) {
      capacity_.dec(delta);
    } else {
      capacity_.dec(delta);
      parent->adjust(Update::FreeCapacity);
    }
  }

  volume_t size(const labels_t labels = 0) const {
    volume_t total = capacity_.original;
    if (children_) {
      for (auto node : children_.value()) {
        if (auto locked = node.lock()) {
          if (locked->has_labels(labels)) {
            total += locked->size(labels);
          }
        }
      }
    }
    return total;
  }

  bool empty() const { return !children_ || children_.value().empty(); }

  auto count() const { return children_ ? children_.value().size() : 0; }

  auto parent() const { return parent_.lock(); }

  int level() const {
    if (level_ < 0) {
      if (auto parent = parent_.lock()) {
        level_ = parent->level() + 1;
      } else {
        level_ = 0;
      }
    }
    return level_;
  }

  std::string topology() const {
    if (auto parent = parent_.lock()) {
      auto parent_topology = parent->topology();
      if (parent_topology.empty()) {
        return parent->name();
      }
      return parent_topology + "/" + parent->name();
    }
    return "";
  }

  ServerState state() const { return current_state_; }
  void state(const ServerState &state) {
    this->state(state, std::chrono::system_clock::now());
  }

  void state(const ServerState &state, const time_point_t &since) {
    if (intended_state_ != state) {
      state_since_ = since;
      intended_state_ = state;
    }
  }

  time_point_t since() const { return state_since_; }

  void inc_affinity(const std::string &name) {
    // Changing affinity is allowed only on leaf nodes.
    CHECK(!children_);
    inc_affinity_r(name);
  }

  void dec_affinity(const std::string &name) {
    // Changing affinity is allowed only on leaf nodes.
    CHECK(!children_);
    dec_affinity_r(name);
  }

  int affinity(const std::string &name) const {
    return affinity_cntr_.get().get(name);
  }

  void add_node(const std::shared_ptr<Node> node) {
    SPDLOG_INFO("adding node : {} => {}", name(), node->name());
    // TODO: need to initialize children_ in ctor based on node type.
    if (!children_) {
      children_ = std::make_optional<children_t>();
    }
    children_.value().push_back(node);
  }

  void erase_expired() {
    if (children_) {
      erase_all_expired(children_.value());
      for (auto &child : children_.value()) {
        if (auto ptr = child.lock()) {
          ptr->erase_expired();
        }
      }
      children_.value().shrink_to_fit();
    }
  }

  std::shared_ptr<Node> nth_child(size_t idx) const {
    CHECK(children_);
    CHECK(idx < children_.value().size());
    return children_.value()[idx].lock();
  }

  PlacementConstraints
  check_app_constraints(const std::shared_ptr<Application> app,
                        const time_point_t now) const;
  virtual bool put(std::shared_ptr<Application> app, const time_point_t now) {
    return false;
  }

  template <typename F> void visit_each(const F &f) const {
    f(this);
    if (children_) {
      for (auto &child : children_.value()) {
        if (auto ptr = child.lock()) {
          ptr->visit_each(f);
        }
      }
    }
  }

  void apply_state() {

    // TODO: rethink how adjustment is done. In the first loop, we check if the
    //       node is down, and if it is, parent is adjusted to discard this
    //       node capacity until it is up.
    //
    //       Second loop applies state recursively on each child. If the child
    //       can't be locked (desroyed), we need to adjust this node state, and
    //       it will recursively propagate up.
    //
    //       Seems like there is room for non-recursive versions of update &
    //       adjust.
    if (current_state_ != intended_state_) {
      current_state_ = intended_state_;
      if (state() == ServerState::up) {
        propagate(Update::All);
      } else {
        if (auto parent = parent_.lock()) {
          parent->adjust(Update::All);
        }
      }
    }
    bool need_adjustment = false;
    if (children_) {
      for (auto &child : children_.value()) {
        if (auto ptr = child.lock()) {
          ptr->apply_state();
          // If the parent is not set, it means that this is newly added node.
          if (!ptr->parent_.lock()) {
            SPDLOG_INFO("attach: {} {}", name_, ptr->name_);
            ptr->parent_ = weak_from_this();
            ptr->propagate(Update::All);
            ptr->add_affinity_counters();
          }
        } else {
          need_adjustment = true;
        }
      }
    }

    if (need_adjustment) {
      SPDLOG_INFO("Expired children found, erasing and adjusting.");
      erase_all_expired(children_.value());
      children_.value().shrink_to_fit();

      adjust(Update::All);
      adjust_affinity_counters();
    }
  }

  void log() const {
    SPDLOG_INFO("node: {}", name_);
    std::vector<char> capacity;
    for (auto &c : capacity_.get()) {
      fmt::format_to(std::back_inserter(capacity), " {}", c);
    }
    capacity.push_back(0);
    SPDLOG_INFO("  capacity  : {}", &capacity[0]);

    std::vector<char> affinities = affinity_cntr_.get().log();
    SPDLOG_INFO("  affinity  : {}", &affinities[0]);
    SPDLOG_INFO("  level     : {}", level());
    SPDLOG_INFO("  traits    : {}", traits().to_string());
  }

  void log_r() {
    if (children_) {
      for (auto &child : children_.value()) {
        if (auto ptr = child.lock()) {
          ptr->log_r();
        } else {
          SPDLOG_INFO("{} - expired child found", name_);
        }
      }
    }
    log();
  }

#ifdef UNIT_TEST
public:
#else
protected:
#endif

  enum Update {
    Nothing = 0,
    Traits = 1 << 0,
    ValidUntil = 1 << 1,
    FreeCapacity = 1 << 2,
    Labels = 1 << 3,
    RequiredTraits = 1 << 4,
    All = -1
  };

  template <typename T, typename F> struct Accumulated {
    Accumulated() = default;
    Accumulated(const T &value) : original(value), combined(value){};
    Accumulated &operator=(const T &value) {
      original = value;
      reset();
      return *this;
    }

    void reset() { combined = original; }
    T &get() { return combined; }
    const T &get() const { return combined; }
    operator const T &() const { return get(); }
    operator T &() { return get(); }

    Accumulated &inc(const T &value) {
      combined += value;
      return *this;
    }

    Accumulated &dec(const T &value) {
      combined -= value;
      return *this;
    }

    Accumulated &combine(const T &value) {
      combine_(combined, value);
      return *this;
    }

    T original;
    T combined;
    F combine_;
  };

  template <typename T, T Node::*M>
  static void recalculate(Node *parent, bool all_nodes = false) {
    (parent->*M).reset();
    if (parent->children_) {
      std::vector<std::shared_ptr<Node>> children;
      lock_all(parent->children_.value().begin(),
               parent->children_.value().end(), std::back_inserter(children));
      for (const auto &node : children) {
        if (node->state() == ServerState::up || all_nodes) {
          (parent->*M).combine((node.get()->*M).get());
        }
      }
    }
  }

  template <typename T, T Node::*M>
  static void add(Node *parent, const Node *node) {
    (parent->*M).combine((node->*M).get());
  }

  void inc_affinity_r(const std::string &name) {
    affinity_cntr_.get().inc(name);
    if (auto parent = parent_.lock()) {
      parent->inc_affinity_r(name);
    }
  }

  void dec_affinity_r(const std::string &name,
                      AffinityCounter::value_t value = 1) {
    affinity_cntr_.get().dec(name, value);
    if (auto parent = parent_.lock()) {
      parent->dec_affinity_r(name, value);
    }
  }

  void add_affinity_counters() {
    if (auto parent = parent_.lock()) {
      add<decltype(affinity_cntr_), &Node::affinity_cntr_>(parent.get(), this);
    }
  }

  void propagate(int what) {
    if (current_state_ != ServerState::up) {
      SPDLOG_INFO("propagate - state not up", name());
      return;
    }
    if (auto parent = parent_.lock()) {
      SPDLOG_INFO("propagating {} {} to parent {}", what, name(),
                  parent->name());
      if (what & Traits)
        add<decltype(traits_), &Node::traits_>(parent.get(), this);
      if (what & RequiredTraits)
        add<decltype(required_traits_), &Node::required_traits_>(parent.get(),
                                                                 this);
      if (what & ValidUntil)
        add<decltype(valid_until_), &Node::valid_until_>(parent.get(), this);
      if (what & FreeCapacity)
        add<decltype(capacity_), &Node::capacity_>(parent.get(), this);
      if (what & Labels)
        add<decltype(labels_), &Node::labels_>(parent.get(), this);
      parent->propagate(what);
    }
  }

  void adjust(int what) {

    if (what & Traits) {
      recalculate<decltype(traits_), &Node::traits_>(this);
    }

    if (what & RequiredTraits) {
      recalculate<decltype(required_traits_), &Node::required_traits_>(this);
    }

    if (what & ValidUntil) {
      recalculate<decltype(valid_until_), &Node::valid_until_>(this);
    }

    if (what & FreeCapacity) {
      recalculate<decltype(capacity_), &Node::capacity_>(this);
    }

    if (what & Labels) {
      recalculate<decltype(labels_), &Node::labels_>(this);
    }

    if (auto parent = parent_.lock()) {
      parent->adjust(what);
    }
  }

  void adjust_affinity_counters() {
    recalculate<decltype(affinity_cntr_), &Node::affinity_cntr_>(this, true);
    if (auto parent = parent_.lock()) {
      parent->adjust_affinity_counters();
    }
  }

  void adjust_capacity_up(const volume_t &new_capacity) {
    capacity_.combine(new_capacity);
    if (auto parent = parent_.lock()) {
      parent->adjust_capacity_up(capacity_);
    }
  }

  std::weak_ptr<Node> parent_;

  std::string name_;
  mutable int level_;

  typedef std::deque<std::weak_ptr<Node>> children_t;
  std::optional<children_t> children_;

  Accumulated<labels_t, or_t<labels_t>> labels_;
  Accumulated<traits_t, or_t<traits_t>> traits_;
  Accumulated<traits_t, traits_and_t> required_traits_;
  Accumulated<time_point_t, time_max_t> valid_until_;
  Accumulated<volume_t, volume_max_t> capacity_;
  Accumulated<AffinityCounter, affinity_inc_t> affinity_cntr_;

  ServerState current_state_;
  ServerState intended_state_;

  time_point_t state_since_;

  object_id_t id_;
};

class Strategy {
public:
  virtual ~Strategy() {}
  virtual std::shared_ptr<Node> suggested() = 0;
  virtual std::shared_ptr<Node> next() = 0;
};

class SpreadStrategy : public Strategy {
public:
  SpreadStrategy(const Node &node) : current_(0), node_(node) {}

  virtual std::shared_ptr<Node> suggested() override {
    CHECK(node_.count() > 0);
    current_ %= node_.count();
    return node_.nth_child(current_++);
  }

  virtual std::shared_ptr<Node> next() override { return suggested(); }

private:
  int current_;
  const Node &node_;
};

class PackStrategy : public Strategy {
public:
  PackStrategy(const Node &node) : current_(0), node_(node) {}

  virtual std::shared_ptr<Node> suggested() override {
    CHECK(node_.count() > 0);
    current_ %= node_.count();
    return node_.nth_child(current_);
  }
  virtual std::shared_ptr<Node> next() override {
    ++current_;
    return suggested();
  }

private:
  int current_;
  const Node &node_;
};

class Bucket : public Node {
public:
  Bucket(const std::string &name, const volume_t &capacity)
      : Node(name, 0, 0, traits_t().set(), time_zero, capacity) {}
  ~Bucket() override {}

  // TODO: For now, affinity is hardcoded to spread. There is no option to set
  //       custom strategy in current Python implementation, so there is no
  //       regression. That said, need to rethink how to support custom
  //       strategies. It seems that application need to own the strategy
  //       factory, and it should be shared between all apps with same
  //       affinity.

  Strategy *affinity_strategy(const std::string &affinity) {
    auto found = affinity_strategies_.find(affinity);
    if (found != affinity_strategies_.end()) {
      return found->second.get();
    }

    auto [inserted, success] = affinity_strategies_.emplace(
        affinity, std::make_unique<SpreadStrategy>(*this));
    CHECK(success);
    return inserted->second.get();
  }

  virtual bool put(std::shared_ptr<Application> app,
                   const time_point_t now) override;

private:
  string_map_t<std::unique_ptr<Strategy>> affinity_strategies_;
};

class Server : public Node {
public:
  Server(const std::string &name, const std::string &parent_name)
      : Node(name, ServerState::down), parent_name_(parent_name) {
    required_traits_ = Accumulated<traits_t, traits_and_t>();
  }

  virtual bool put(std::shared_ptr<Application> app,
                   const time_point_t now) override;

  auto parent_name() const { return parent_name_; }

  auto partition() const { return partition_; }
  void partition(std::optional<std::string> partition) {
    partition_ = partition;
  }

  template <typename Event, typename... Args> void record(Args &&... args) {
    srv_events_.push_back(std::make_unique<Event>(std::forward<Args>(args)...));
  }

  void
  flush_events(const std::optional<trace_clbk_t> trace_clbk = std::nullopt) {
    if (!srv_events_.empty()) {
      Event::logger->info("{}", name());
      std::for_each(srv_events_.begin(), srv_events_.end(),
                    [](auto &ev) { ev->flush(); });
      if (trace_clbk) {
        std::for_each(srv_events_.begin(), srv_events_.end(),
                      [this, trace_clbk](auto &ev) {
                        auto trace = ev->trace();
                        if (trace) {
                          trace_clbk.value()(name(), trace.value());
                        }
                      });
      }
      srv_events_.clear();
    }
  }

private:
  std::string parent_name_;
  std::optional<std::string> partition_;
  std::vector<std::unique_ptr<Event>> srv_events_;
};

class ServerStateEvent : public Event {
public:
  ServerStateEvent(ServerState state) : state_(state) {}
  void flush() override;
  std::optional<Trace> trace() const override {
    switch (state_) {
    case ServerState::up:
      return std::make_optional<Trace>(Trace{"server_state", "up"});
    case ServerState::down:
      return std::make_optional<Trace>(Trace{"server_state", "down"});
    case ServerState::frozen:
      return std::make_optional<Trace>(Trace{"server_state", "frozen"});
    case ServerState::deleted:
      return std::make_optional<Trace>(Trace{"server_state", "deleted"});
    default:
      CHECK(false);
      return std::nullopt;
    }
  }

private:
  ServerState state_;
};

class ServerBlackoutEvent : public Event {
public:
  ServerBlackoutEvent() {}
  void flush() override;
  std::optional<Trace> trace() const override {
    return std::make_optional<Trace>(Trace{"server_blackout", ""});
  }
};

class ServerBlackoutClearedEvent : public Event {
public:
  ServerBlackoutClearedEvent() {}
  void flush() override;
  std::optional<Trace> trace() const override {
    return std::make_optional<Trace>(Trace{"server_blackout_cleared", ""});
  }
};

} // namespace scheduler
} // namespace treadmill
