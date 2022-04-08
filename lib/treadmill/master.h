#pragma once

#include <treadmill/counterguard.h>
#include <treadmill/scheduler.h>
#include <treadmill/zkclient.h>

#include <filesystem>
#include <mutex>
#include <queue>

namespace treadmill {
namespace scheduler {

template <typename T> class Collection {
public:
  typedef std::shared_ptr<T> ptr_t;
  typedef std::optional<ptr_t> opt_t;
  typedef std::function<void(const ptr_t &)> completion_t;
  typedef std::function<void()> cleanup_t;

  ~Collection() { entries_.erase(entries_.begin(), entries_.end()); }

  struct entry_t {
    entry_t() : element(std::nullopt) {}
    explicit entry_t(const opt_t &e) : element(e) {}
    ~entry_t() {}

    opt_t element;
    std::vector<completion_t> completions;
  };

  bool exists(const std::string &name) const {
    return entries_.find(name) != entries_.end();
  }

  bool ready(const std::string &name) const {
    auto found = entries_.find(name);
    return found != entries_.end() && found->second.element;
  }

  void add(const std::string &name, std::shared_ptr<T> ptr) {
    auto found = entries_.find(name);
    if (found != entries_.end()) {
      found->second.element = ptr;
      for (auto &f : found->second.completions) {
        f(ptr);
      }
      found->second.completions.erase(found->second.completions.begin(),
                                      found->second.completions.end());
    }
  }

  void add_async(const std::string &name, const std::function<void()> &make) {
    CHECK(entries_.find(name) == entries_.end());
    entries_.emplace(name, std::nullopt);
    make();
  }

  void when_ready(const std::string &name, const completion_t &completion) {
    auto found = entries_.find(name);
    if (found == entries_.end()) {
      auto inserted = entries_.emplace(name, std::nullopt);
      found = inserted.first;
    }

    auto &entry = found->second;
    if (found->second.element && found->second.element.value()) {
      completion(entry.element.value());
    } else {
      entry.completions.push_back(completion);
    }
  }

  void when_ready(const std::string &name, const completion_t &completion,
                  const cleanup_t &cleanup) {
    auto found = entries_.find(name);
    if (found == entries_.end()) {
      auto inserted = entries_.emplace(name, std::nullopt);
      found = inserted.first;
    }

    auto &entry = found->second;
    if (found->second.element && found->second.element.value()) {
      completion(entry.element.value());
    } else {
      struct WithCleanup {
        WithCleanup(const completion_t &completion, const cleanup_t &cleanup)
            : completion_(completion), cleanup_(cleanup), called_(false) {}
        WithCleanup(const WithCleanup &) = delete;
        WithCleanup(WithCleanup &&) = delete;
        WithCleanup &operator=(const WithCleanup &) = delete;
        WithCleanup &operator=(WithCleanup &&) = delete;
        ~WithCleanup() {
          if (!called_)
            cleanup_();
        }
        void operator()(const ptr_t &ptr) {
          called_ = true;
          completion_(ptr);
        };

        completion_t completion_;
        cleanup_t cleanup_;
        bool called_;
      };

      auto c = std::make_shared<WithCleanup>(completion, cleanup);
      entry.completions.emplace_back([c](auto &p) { (*c)(p); });
    }
  }

  auto find(const std::string &name) const { return entries_.find(name); }

  auto begin() const { return entries_.begin(); }

  auto end() const { return entries_.end(); }

  auto size() const { return entries_.size(); }

  template <typename F> void for_each(const F &f) {
    for (auto &entry : entries_) {
      if (entry.second.element) {
        f(entry.second.element.value());
      }
    }
  }

  template <typename F> void for_each(const F &f) const {
    for (auto &entry : entries_) {
      if (entry.second.element) {
        f(entry.second.element.value());
      }
    }
  }

  void remove(const std::string &name) { entries_.erase(name); }

  template <typename TheirIt, typename CreateF, typename DeleteF>
  auto reconcile(TheirIt first1, TheirIt last1, CreateF create_f,
                 DeleteF delete_f) {

    auto first2 = std::begin(entries_);
    auto last2 = std::end(entries_);

    std::vector<std::string> created;
    auto created_first = std::back_inserter(created);

    std::vector<std::string> deleted;
    auto deleted_first = std::back_inserter(deleted);

    while (true) {
      if (first2 == last2) {
        std::copy(first1, last1, created_first);
        break;
      }
      if (first1 == last1) {
        for (auto it = first2; it != last2; ++it) {
          *deleted_first++ = it->first;
        }
        break;
      }

      if (*first1 < first2->first) {
        *created_first++ = *first1++;
      } else if (first2->first < *first1) {
        *deleted_first++ = first2->first;
        ++first2;
      } else {
        ++first1;
        ++first2;
      }
    }

    for (auto &name : created) {
      entries_.emplace(name, std::nullopt);
      create_f(name);
    }

    for (auto &name : deleted) {
      delete_f(name);
      entries_.erase(name);
    }
  }

  template <typename TheirIt, typename F>
  auto reconcile(TheirIt first1, TheirIt last1, F create_f) {
    reconcile(first1, last1, create_f, [](auto &) {});
  }

  void print_refcount() const {
    for (auto &e : entries_) {
      SPDLOG_DEBUG("{} {}", e.first,
                   e.second.element ? e.second.element.value().use_count() : 0);
    }
  }

private:
  sorted_string_map_t<entry_t> entries_;
};

struct Topology {
  size_t n_dimensions;
  string_map_t<size_t> levels;
};

class ServerPresence {
public:
  ServerPresence(const std::string &name, std::shared_ptr<Server> server,
                 std::optional<time_point_t> valid_until, time_point_t ctime)
      : name_(name), server_(std::move(server)), valid_until_(valid_until),
        ctime_(ctime) {
    if (auto server = server_.lock()) {
      auto now = std::chrono::system_clock::now();
      if (valid_until) {
        server->valid_until(valid_until.value());
      }
      server->state(ServerState::up, now);
      server->record<ServerStateEvent>(ServerState::up);
      Node::logger->info("{:016x} {} is up", server->id(), server->name());
    }
  }

  ~ServerPresence() {
    if (auto server = server_.lock()) {
      auto now = std::chrono::system_clock::now();
      server->state(ServerState::down, now);
      server->record<ServerStateEvent>(ServerState::down);
      Node::logger->info("{:016x} {} is down", server->id(), server->name());
    }
  }

  time_point_t ctime() const { return ctime_; }

private:
  std::string name_;
  std::weak_ptr<Server> server_;
  std::optional<time_point_t> valid_until_;
  time_point_t ctime_;
};

typedef std::tuple<std::string, std::string, time_point_t, Identity, Identity,
                   bool, std::string>
    placement_t;

enum Message : uint64_t {
  end = 0,
  load_servers = 1UL << 0,
  load_servers_presence = 1UL << 1,
  load_blackedout_servers = 1UL << 2,
  load_allocations = 1UL << 3,
  load_igroups = 1UL << 4,
  load_events = 1UL << 5,
  load_placements = 1UL << 6,
  load_buckets = 1UL << 7,
  load_apps = 1UL << 8,
  load_app_placements = 1UL << 9,
  create_namespace = 1UL << 10,
  start = 1UL << (sizeof(Message) * 8 - 1)
};

// BITMASK_DEFINE_VALUE_MASK(Message, -1);

class Master {
public:
  // TODO: make it private.
  Master(const Topology &topology, std::string cell, std::string state_dir,
         std::string appev_dir, std::string srvev_dir, bool readonly);
  Master(const Master &) = delete;
  ~Master();

  void use(ZkClient *zkclient) { zkclient_ = zkclient; }

  void start();
  void init();
  void run();

  labels_t label(const std::string &name);

  traits_t trait(const std::string &name);

private:
  void create_namespace();

  void load_all();

  void load_placement();

  bool load_server(int rc, const ZkClient::Path &path,
                   const ZkClient::Data &data, const ZkClient::Stat &stat);

  bool load_servers(int rc, const ZkClient::Path &path,
                    const ZkClient::List &nodes);

  bool load_blackedout_servers(int rc, const ZkClient::Path &path,
                               const ZkClient::List &nodes);

  bool load_server_presence(int rc, const ZkClient::Path &path,
                            const ZkClient::Data &data,
                            const ZkClient::Stat &stat);

  bool load_servers_presence(int rc, const ZkClient::Path &path,
                             const ZkClient::List &nodes);

  bool load_bucket(int rc, const ZkClient::Path &path,
                   const ZkClient::Data &data, const ZkClient::Stat &stat);

  bool load_buckets(int rc, const ZkClient::Path &path,
                    const ZkClient::List &nodes);

  bool load_allocation(int rc, const ZkClient::Path &path,
                       const ZkClient::Data &data, const ZkClient::Stat &stat);

  bool load_igroup(int rc, const ZkClient::Path &path,
                   const ZkClient::Data &data, const ZkClient::Stat &stat);

  bool load_igroups(int rc, const ZkClient::Path &path,
                    const ZkClient::List &nodes);

  bool load_app(int rc, const ZkClient::Path &path, const ZkClient::Data &data,
                const ZkClient::Stat &stat);

  bool load_apps(int rc, const ZkClient::Path &path,
                 const ZkClient::List &nodes);

  bool load_event(int rc, const ZkClient::Path &path,
                  const ZkClient::Data &data, const ZkClient::Stat &stat);

  bool load_events(int rc, const ZkClient::Path &path,
                   const ZkClient::List &nodes);

  bool restore_placements(int rc, const ZkClient::Path &path,
                          const ZkClient::List &nodes);

  bool restore_placement(int rc, const ZkClient::Path &path,
                         const ZkClient::Data &data,
                         const ZkClient::Stat &stat);

  bool restore_app_placements(int rc, const ZkClient::Path &path,
                              const ZkClient::List &nodes);

  bool restore_app_placement(int rc, const ZkClient::Path &path,
                             const ZkClient::Data &data,
                             const ZkClient::Stat &stat);

  bool terminate_unscheduled_app(int rc, const ZkClient::Path &path,
                                 const ZkClient::Data &data,
                                 const ZkClient::Stat &stat);

  bool terminate_schedule_once_app(int rc, const ZkClient::Path &path,
                                   const ZkClient::Data &data,
                                   const ZkClient::Stat &stat);

  void terminate_app(const std::string &placement_path,
                     std::optional<std::string> unique_id,
                     std::optional<std::string> reason);

  void create_finished_node(std::string app_name,
                            std::optional<std::string> server_name,
                            std::optional<std::string> unique_id,
                            std::optional<std::string> reason);

  void on_delete(int rc);
  void on_set(int rc, const ZkClient::Stat &stat);
  void on_create(int rc, const ZkClient::Data &path);
  void on_ensure_exists(int rc, const ZkClient::Data &path);

  void del(const std::string &path);
  void set(const std::string &path, const std::string &data) {
    set(path, data.c_str(), data.size());
  }
  void set(const std::string &path, const char *data, size_t len);
  void create(const std::string &path, const std::string &data) {
    create(path, data.c_str(), data.size());
  }
  void create(const std::string &path, const char *data, size_t len);

  void ensure_exists(const std::string &path) { ensure_exists(path, ""); }
  void ensure_exists(const std::string &path, const std::string &data) {
    ensure_exists(path, data.c_str(), data.size());
  }
  void ensure_exists(const std::string &path, const char *data, size_t len);

  void save_placement(const std::set<placement_t> &before,
                      const std::set<placement_t> &after);

  void save_server_placement(const std::string &server, ServerState state);

  template <typename... Args> void watch_children(Args &&... args) {
    return zkclient_->watch_children(std::forward<Args>(args)...);
  }

  template <typename... Args> void watch_data(Args &&... args) {
    return zkclient_->watch_data(std::forward<Args>(args)...);
  }

  void schedule(const std::string &app, const std::string &server,
                time_point_t expires_at);
  void unschedule(const std::string &app, const std::string &server);

  void apply(const Placement &placement);

  void update_task(const std::string &appname,
                   const std::optional<std::string> &server,
                   const std::optional<std::string> &why);

  void trace_event(const std::string &name, const Trace &trace,
                   const std::filesystem::path &ev_dir);

  void app_trace(const std::string &app_name, const Trace &trace);

  void srv_trace(const std::string &srv_name, const Trace &trace);

  void write_state() const;
  void write_labels_state() const;
  void write_traits_state() const;
  void write_apps_state() const;
  void write_servers_state() const;
  void write_allocations_state() const;

  template <typename T>
  void write_volume(T &obj, const char *key, const volume_t &volume) const {
    auto volume_arr = obj.arr(key);
    static std::vector<std::string> types{"mem", "cpu", "disk", "gpu"};
    for (size_t i = 0; i < types.size(); ++i) {
      auto type_obj = volume_arr.obj();
      type_obj("type", types[i]);
      type_obj("value", volume[i]);
    }
  }

  template <typename T>
  std::vector<std::string> decode_bitset(const T &bitset,
                                         const string_map_t<T> &codes) const {
    std::vector<std::string> names;
    for (auto &code : codes) {
      if ((bitset & code.second).any()) {
        names.push_back(code.first);
      }
    }
    return names;
  }

  std::optional<std::string> alloc_partition(const std::string &name) const {
    auto pos = name.find('/', 1);
    if (pos == std::string::npos) {
      return std::nullopt;
    }
    return name.substr(1, pos - 1);
  }

  std::optional<std::string> alloc_name(const std::string &name) const {
    auto pos = name.find('/', 1);
    if (pos == std::string::npos) {
      return std::nullopt;
    }
    return name.substr(pos + 1);
  }

  ZkClient *zkclient_;
  Topology topology_;
  std::filesystem::path state_dir_;
  std::filesystem::path appev_dir_;
  std::filesystem::path srvev_dir_;

  std::shared_ptr<Cell> cell_;
  const volume_t empty_;

  ZkClient::DataWatch server_watch_;
  ZkClient::ChildrenWatch servers_watch_;

  ZkClient::ChildrenWatch blackedout_servers_watch_;

  ZkClient::DataWatch server_presence_watch_;
  ZkClient::ChildrenWatch servers_presence_watch_;

  ZkClient::DataWatch bucket_watch_;
  ZkClient::ChildrenWatch buckets_watch_;

  ZkClient::DataWatch igroup_watch_;
  ZkClient::ChildrenWatch igroups_watch_;

  ZkClient::DataWatch app_watch_;
  ZkClient::ChildrenWatch apps_watch_;

  ZkClient::DataWatch allocation_watch_;

  ZkClient::DataWatch placement_watch_;
  ZkClient::ChildrenWatch placements_watch_;

  ZkClient::DataWatch app_placement_watch_;
  ZkClient::ChildrenWatch app_placements_watch_;

  ZkClient::DataWatch unscheduled_app_placement_watch_;
  ZkClient::DataWatch schedule_once_app_placement_watch_;

  ZkClient::DataWatch event_watch_;
  ZkClient::ChildrenWatch events_watch_;

  ZkClient::CreateClbk create_clbk_;
  ZkClient::CreateClbk ensure_exists_clbk_;
  ZkClient::SetClbk set_clbk_;
  ZkClient::DeleteClbk delete_clbk_;

  Collection<Server> servers_;
  Collection<ServerPresence> server_presence_;
  Collection<Bucket> buckets_;
  Collection<Application> apps_;
  Collection<IdentityGroup> igroups_;
  Collection<SchedulerEvent> events_;

  string_map_t<traits_t> traits_;
  string_map_t<labels_t> labels_;

  string_map_t<std::optional<std::string>> restored_allocs_;

  std::set<std::string> blackedout_servers_;

  bool ready_;
  bool readonly_;
  bool uptodate_;
  Semaphore *updates_counter_;

  ZkClient::ExistsWatch msg_complete_watch_;

  void msg_enqueue(Message msg);
  bool msg_complete(int rc, const ZkClient::Path &path,
                    const ZkClient::Stat &stat);
  bool msg_wait(int timeout_ms);

  std::mutex msgs_mx_;
  std::condition_variable msgs_ev_;
  std::queue<Message> pending_msgs_;
  std::queue<Message> msgs_;

  std::thread::id zk_clbk_thread_id;
  std::thread::id scheduler_thread_id;
};

} // namespace scheduler
} // namespace treadmill
