#include <treadmill/master.h>

#include <treadmill/convert.h>
#include <treadmill/diff.h>
#include <treadmill/fs.h>
#include <treadmill/json.h>
#include <treadmill/profiler.h>
#include <treadmill/scheduler.h>
#include <treadmill/strings.h>
#include <treadmill/yaml.h>
#include <treadmill/zknamespace.h>

#include <rapidjson/filewritestream.h>
#include <rapidjson/writer.h>
#include <zlib.h>

#include <chrono>
#include <iomanip>
#include <regex>
#include <sstream>
#include <system_error>

namespace treadmill {
namespace scheduler {

static std::string path_allocations = "/allocations";
static std::string path_appgroups = "/app-groups";
static std::string path_appgroup_lookups = "/appgroup-lookups";
static std::string path_monitors = "/app-monitors";
static std::string path_blackedout_apps = "/blackedout.apps";
static std::string path_blackedout_servers = "/blackedout.servers";
static std::string path_buckets = "/buckets";
static std::string path_cell = "/cell";
static std::string path_cron_jobs = "/cron-jobs";
static std::string path_cron_trace = "/cron-trace";
static std::string path_cron_trace_history = "/cron-trace.history";
static std::string path_data_records = "/data-records";
static std::string path_discovery = "/discovery";
static std::string path_discovery_state = "/discovery.state";
static std::string path_election = "/election";
static std::string path_endpoints = "/endpoints";
static std::string path_events = "/events";
static std::string path_finished = "/finished";
static std::string path_finished_history = "/finished.history";
static std::string path_globals = "/globals";
static std::string path_igroups = "/identity-groups";
static std::string path_keytab_locker = "/keytab-locker";
static std::string path_partitions = "/partitions";
static std::string path_placement = "/placement";
static std::string path_reboots = "/reboots";
static std::string path_running = "/running";
static std::string path_scheduled = "/scheduled";
static std::string path_scheduled_stats = "/scheduled-stats";
static std::string path_scheduler = "/scheduler";
static std::string path_servers = "/servers";
static std::string path_presence = "/server.presence";
static std::string path_server_trace = "/server-trace";
static std::string path_server_trace_history = "/server-trace.history";
static std::string path_reports = "/reports";
static std::string path_strategies = "/strategies";
static std::string path_ticket_locker = "/ticket-locker";
static std::string path_tickets = "/tickets";
static std::string path_trace = "/trace";
static std::string path_trace_history = "/trace.history";
static std::string path_traits = "/traits";
static std::string path_treadmill = "/treadmill";
static std::string path_triggers = "/triggers";
static std::string path_trigger_trace = "/trigger-trace";
static std::string path_trigger_trace_history = "/trigger-trace.history";
static std::string path_version = "/version";
static std::string path_version_history = "/version.history";
static std::string path_zookeeper = "/zookeeper";
static std::string path_scheduler_lock = "/scheduler-lock";

inline bool connectionloss(int rc) {
  return false; /* return rc == ZCONNECTIONLOSS; */
}

inline bool to_bool(const char *value) {
  return strcmp(value, "true") == 0 || strcmp(value, "True") == 0 ||
         strcmp(value, "1") == 0;
}

inline long to_number(const char *value) { return strtol(value, 0, 0); }

inline time_point_t sec_to_time_point(long value) {
  return time_point_t(std::chrono::duration<long>(value));
}

inline time_point_t sec_to_time_point(const std::string &value) {
  return time_point_t(std::chrono::duration<long>(std::stol(value)));
}

inline time_point_t sec_to_time_point(const char *value) {
  return time_point_t(std::chrono::duration<long>(to_number(value)));
}

inline time_point_t msec_to_time_point(long value) {
  return time_point_t(std::chrono::milliseconds(value));
}

inline auto sec_from_epoch(time_point_t t) {
  auto epoch = t.time_since_epoch();
  auto value = std::chrono::duration_cast<std::chrono::seconds>(epoch);
  return value.count();
}

inline auto msec_from_epoch(time_point_t t) {
  auto epoch = t.time_since_epoch();
  auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
  return value.count();
}

inline auto microsec_from_epoch(time_point_t t) {
  auto epoch = t.time_since_epoch();
  auto value = std::chrono::duration_cast<std::chrono::microseconds>(epoch);
  return value.count();
}

inline auto timestamp_str(time_point_t t) {
  auto msec = microsec_from_epoch(t);
  return fmt::format("{}.{:06d}", msec / 1000000, msec % 1000000);
}

inline auto optional_sec_from_epoch(std::optional<time_point_t> t) {
  return t ? std::make_optional(sec_from_epoch(t.value())) : std::nullopt;
}

inline void replace(std::string &s, const char from, const char to) {
  std::for_each(std::begin(s), std::end(s), [from, to](auto &i) {
    if (i == from)
      i = to;
  });
}

inline std::string regex_escape(const std::string &input) {
  static std::regex special_chars{R"([-[\]{}()*+?.,\^$|#\s])"};
  return std::regex_replace(input, special_chars, R"(\$&)");
}

inline std::string glob2regex(const std::string &glob) {
  auto re = "^" + regex_escape(glob);
  re = std::regex_replace(re, std::regex("\\\\\\*"), ".*");
  re = std::regex_replace(re, std::regex("\\\\\\?"), ".");
  return re;
}

inline std::string format_unique_id(const ZkClient::Stat &app_placement_stat) {
  std::stringstream ss;
  ss << std::setw(13) << std::setfill('0') << std::hex
     << app_placement_stat->czxid;
  return ss.str();
}

static std::string placement_data(const Placement &placement) {
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  {
    namespace js = treadmill::js;
    js::Object obj(&writer);

    obj("identity", placement.identity);
    obj("identity_count", placement.identity_count);
    if (placement.new_expires_at) {
      obj("expires", sec_from_epoch(placement.new_expires_at.value()));
    } else {
      obj("expires", std::nullopt);
    }
    obj("allocation", placement.assigned_alloc);
  }
  return std::string(sb.GetString());
}

static std::string server_placement_data(const std::string &state) {
  auto now = std::chrono::high_resolution_clock::now();
  auto timestamp = sec_from_epoch(now);

  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  {
    namespace js = treadmill::js;
    js::Object obj(&writer);

    obj("state", state);
    obj("since", timestamp);
  }
  return std::string(sb.GetString());
}

static void log_node_count(const std::shared_ptr<Node> node) {
  node->visit_each([](auto const &n) {
    SPDLOG_INFO("{} - level: {}, count: {}", n->name(), n->level(), n->count());
  });
}

static void log_allocs(const std::shared_ptr<Allocation> alloc) {
  // TODO: why is this commented out?
  /*
  SPDLOG_INFO("{} - sub-allocs: {}, apps: {}", alloc->name(),
  alloc->sub_allocs_.size(), alloc->apps_.size()); for (auto &a :
  alloc->sub_allocs_) { log_allocs(a);
  }*/
}

#define _bind_memfn(f)                                                         \
  [this](auto &&... args) -> decltype(auto) {                                  \
    return f(std::forward<decltype(args)>(args)...);                           \
  }

Master::Master(const Topology &topology, std::string cell,
               std::string state_dir, std::string appev_dir,
               std::string srvev_dir, bool readonly)
    : zkclient_(nullptr), topology_(topology), state_dir_(std::move(state_dir)),
      appev_dir_(std::move(appev_dir)), srvev_dir_(std::move(srvev_dir)),
      cell_(std::make_shared<Cell>(cell, topology_.n_dimensions)),
      empty_(volume_t::value_type(0), topology_.n_dimensions),
      server_watch_(_bind_memfn(load_server)),
      servers_watch_(_bind_memfn(load_servers)),
      blackedout_servers_watch_(_bind_memfn(load_blackedout_servers)),
      server_presence_watch_(_bind_memfn(load_server_presence)),
      servers_presence_watch_(_bind_memfn(load_servers_presence)),
      bucket_watch_(_bind_memfn(load_bucket)),
      buckets_watch_(_bind_memfn(load_buckets)),
      igroup_watch_(_bind_memfn(load_igroup)),
      igroups_watch_(_bind_memfn(load_igroups)),
      app_watch_(_bind_memfn(load_app)), apps_watch_(_bind_memfn(load_apps)),
      allocation_watch_(_bind_memfn(load_allocation)),
      placement_watch_(_bind_memfn(restore_placement)),
      placements_watch_(_bind_memfn(restore_placements)),
      app_placement_watch_(_bind_memfn(restore_app_placement)),
      app_placements_watch_(_bind_memfn(restore_app_placements)),
      unscheduled_app_placement_watch_(_bind_memfn(terminate_unscheduled_app)),
      schedule_once_app_placement_watch_(
          _bind_memfn(terminate_schedule_once_app)),
      event_watch_(_bind_memfn(load_event)),
      events_watch_(_bind_memfn(load_events)),
      create_clbk_(_bind_memfn(on_create)),
      ensure_exists_clbk_(_bind_memfn(on_ensure_exists)),
      set_clbk_(_bind_memfn(on_set)), delete_clbk_(_bind_memfn(on_delete)),
      traits_({}),
      labels_({{defaults::default_partition, defaults::default_label}}),
      ready_(false), readonly_(readonly), uptodate_(false),
      updates_counter_(nullptr), msg_complete_watch_(_bind_memfn(msg_complete)),
      zk_clbk_thread_id(0), scheduler_thread_id(0) {}

Master::~Master() {
  // Do not invoke any Zk operations at destruction.
  SPDLOG_DEBUG("Master::dtor");
  readonly_ = true;
}

void Master::del(const std::string &path) {
  if (!readonly_) {
    if (updates_counter_)
      updates_counter_->inc();
    zkclient_->del(path, &delete_clbk_);
  }
}

void Master::set(const std::string &path, const char *data, size_t len) {
  if (!readonly_) {
    if (updates_counter_)
      updates_counter_->inc();
    zkclient_->set(path, data, len, &set_clbk_);
  }
}

void Master::create(const std::string &path, const char *data, size_t len) {
  if (!readonly_) {
    if (updates_counter_)
      updates_counter_->inc();
    zkclient_->create(path, data, len, 0, &create_clbk_);
  }
}

void Master::ensure_exists(const std::string &path, const char *data,
                           size_t len) {
  if (!readonly_) {
    if (updates_counter_)
      updates_counter_->inc();
    zkclient_->create(path, data, len, 0, &ensure_exists_clbk_);
  }
}

void Master::create_namespace() {
  CHECK(zkclient_);
  auto zknamespace = {path_allocations,
                      path_appgroups,
                      path_appgroup_lookups,
                      path_monitors,
                      path_blackedout_apps,
                      path_blackedout_servers,
                      path_buckets,
                      path_cell,
                      path_cron_jobs,
                      path_cron_trace,
                      path_cron_trace_history,
                      path_data_records,
                      path_discovery,
                      path_discovery_state,
                      path_election,
                      path_endpoints,
                      path_events,
                      path_finished,
                      path_finished_history,
                      path_globals,
                      path_igroups,
                      path_keytab_locker,
                      path_partitions,
                      path_placement,
                      path_reboots,
                      path_running,
                      path_scheduled,
                      path_scheduled_stats,
                      path_scheduler,
                      path_servers,
                      path_presence,
                      path_server_trace,
                      path_server_trace_history,
                      path_reports,
                      path_strategies,
                      path_ticket_locker,
                      path_tickets,
                      path_trace,
                      path_trace_history,
                      path_traits,
                      path_treadmill,
                      path_triggers,
                      path_trigger_trace,
                      path_trigger_trace_history,
                      path_version,
                      path_version_history,
                      path_zookeeper,
                      path_scheduler_lock};

  namespace z = treadmill::zknamespace::constants;

  for (auto node : zknamespace) {
    SPDLOG_INFO("Create namespace: {}", node);
    ensure_exists(node);
  }

  SPDLOG_INFO("Create trace shards");
  for (int i = 0; i < z::num_trace_shards; ++i) {
    auto shard = fmt::format("{}/{:04X}", z::trace, i);
    ensure_exists(shard);
  }

  SPDLOG_INFO("Create cron trace shards");
  for (int i = 0; i < z::num_cron_trace_shards; ++i) {
    auto shard = fmt::format("{}/{:04X}", z::cron_trace, i);
    ensure_exists(shard);
  }

  SPDLOG_INFO("Create server trace shards");
  for (int i = 0; i < z::num_server_trace_shards; ++i) {
    auto shard = fmt::format("{}/{:04X}", z::server_trace, i);
    ensure_exists(shard);
  }

  msg_enqueue(Message::create_namespace);
  SPDLOG_INFO("Zk namespace initialized.");
}

void Master::msg_enqueue(Message msg) {
  std::unique_lock<std::mutex> lck(msgs_mx_);
  pending_msgs_.push(msg);
  zkclient_->watch_exists("/", &msg_complete_watch_, false);
}

bool Master::msg_complete(int rc, const ZkClient::Path &path,
                          const ZkClient::Stat &stat) {
  if (rc != ZOK) {
    return false;
  }

  std::unique_lock<std::mutex> lck(msgs_mx_);
  CHECK(!pending_msgs_.empty());
  msgs_.push(pending_msgs_.front());
  pending_msgs_.pop();
  msgs_ev_.notify_all();
  return false;
}

bool Master::msg_wait(int timeout_ms) {
  using namespace std::chrono_literals;
  std::unique_lock<std::mutex> lock(msgs_mx_);
  // TODO: does the test function run with lock held? It accesses msgs_ which
  //       is protected by the same mutex (msgs_mx_) as the condition variable.
  return msgs_ev_.wait_for(lock, timeout_ms * 1ms,
                           [this] { return !msgs_.empty(); });
}

void Master::start() {
  CHECK(zk_clbk_thread_id == (std::thread::id)0);
  zk_clbk_thread_id = std::this_thread::get_id();
  msg_enqueue(Message::start);
}

void Master::init() {
  CHECK(scheduler_thread_id == (std::thread::id)0);
  scheduler_thread_id = std::this_thread::get_id();

  uint64_t state = 0;
  auto loaded = Message::load_buckets | Message::load_servers |
                Message::load_servers_presence |
                Message::load_blackedout_servers | Message::load_allocations |
                Message::load_igroups | Message::load_apps |
                Message::load_events;
  auto ready = (loaded | Message::create_namespace | Message::load_placements |
                Message::load_app_placements);

  bool loading_placement = false;
  size_t app_placements_count = 0;

  Semaphore updates_counter;
  updates_counter_ = &updates_counter;

  while (!ready_) {

    if (msg_wait(1000)) {
      std::queue<Message> local;
      {
        std::unique_lock<std::mutex> lock(msgs_mx_);
        local.swap(msgs_);
      }

      // Process messages without lock.
      while (!local.empty()) {
        auto msg = local.front();

        local.pop();
        state |= msg;

        if (msg == Message::load_app_placements) {
          SPDLOG_DEBUG("Got app_placements.");
          ++app_placements_count;
          if (app_placements_count == servers_.size()) {
            SPDLOG_INFO("App placement loaded.");
          } else {
            // Not ready yet, clear state.
            state &= ~(msg);
          }
        } else if (loading_placement && servers_.size() == 0) {
          // There won't be app placements msg for empty cell.
          SPDLOG_INFO("App placement loaded (no servers).");
          state |= Message::load_app_placements;
        } else if (msg == Message::start) {
          create_namespace();
          load_all();
        }

        SPDLOG_INFO("current state : {:0>64b}", state);
        SPDLOG_INFO("ready state   : {:0>64b}", ready);

        if ((state & ready) == ready) {
          ready_ = true;
          break;
        }

        if ((state & loaded) == loaded && !loading_placement) {
          SPDLOG_INFO("Model loaded, loading placement.");
          cell_->apply_allocations();
          cell_->apply_state();
          cell_->visit_each([](auto *n) { n->log(); });
          load_placement();
          loading_placement = true;
        }
      }
    }
  }

  updates_counter.wait();
  updates_counter_ = nullptr;

  log_node_count(cell_);
  for (auto &a : cell_->allocations()) {
    log_allocs(a);
  }
  SPDLOG_INFO("Finished loading SOW.");

  Event::logger->flush();
  Application::logger->flush();
  Node::logger->flush();

  write_state();
}

void Master::run() {
  CHECK(ready_);
  CHECK(scheduler_thread_id == std::this_thread::get_id());

  auto app_trace_clbk = _bind_memfn(app_trace);
  auto srv_trace_clbk = _bind_memfn(srv_trace);
  std::set<placement_t> before;

  load_all();

  {
    auto apps = cell_->current_placement();

    // Flush all the events related to loading SOW.
    std::for_each(apps.begin(), apps.end(),
                  [](auto &app) { app->flush_events(); });

    for (auto &app : apps) {
      // App is assigned to an allocation, compare it to the one we restored.
      CHECK(app->assigned_alloc());
      auto found = restored_allocs_.find(app->name());
      if (found != restored_allocs_.end()) {
        auto restored_alloc = found->second;
        if (restored_alloc &&
            restored_alloc.value() != app->assigned_alloc().value()) {
          // App was scheduled and allocation changed, create assigned event.
          // If restored placement had no allocation (old format), do nothing.
          app->record<AssignedEvent>(app->assigned_alloc().value());
        }
      } else {
        // App is new or previously pending, create assigned event.
        app->record<AssignedEvent>(app->assigned_alloc().value());
      }

      if (auto server = app->server()) {
        CHECK(app->expires_at());
        before.emplace(std::make_tuple(
            app->name(), server->name(), app->expires_at().value(),
            app->identity(), app->identity_count(), app->schedule_once(),
            app->assigned_alloc().value()));
      }
    }

    restored_allocs_.clear();
  }

  auto scheduled_at = std::chrono::high_resolution_clock::now();
  while (true) {

    if (msg_wait(250)) {
      std::unique_lock<std::mutex> lock(msgs_mx_);
      std::queue<Message> local;
      local.swap(msgs_);
      uptodate_ = false;
    }

    if (uptodate_) {
      continue;
    }

    auto now = std::chrono::high_resolution_clock::now();

    using namespace std::chrono_literals;
    if ((now - scheduled_at) < 200ms) {
      continue;
    }

    auto apps = cell_->schedule();

    std::set<placement_t> after;
    for (auto &app : apps) {
      if (auto server = app->server()) {
        CHECK(app->expires_at());
        CHECK(app->assigned_alloc());
        after.emplace(std::make_tuple(
            app->name(), server->name(), app->expires_at().value(),
            app->identity(), app->identity_count(), app->schedule_once(),
            app->assigned_alloc().value()));
      }
    }

    // Calculate placement changes.
    // Note: before and after only contains apps with placement (old or new).
    std::vector<Placement> diffs;
    for_each_diff(
        before.begin(), before.end(), after.begin(), after.end(),
        [&diffs](const auto &deleted) {
          // App had placement before, but it doesn't have placement
          // now. Deleted or evicted app, new server is empty.
          diffs.emplace_back(Placement{
              std::get<0>(deleted), std::get<1>(deleted), std::get<2>(deleted),
              std::nullopt, std::nullopt, std::nullopt, std::nullopt,
              std::get<5>(deleted), std::get<6>(deleted)});
        },
        [&diffs](const auto &created) {
          // App didn't have placement before, but it has placement
          // now. New or previously pending app, old server is
          // empty.
          diffs.emplace_back(
              Placement{std::get<0>(created), std::nullopt, std::nullopt,
                        std::get<1>(created), std::get<2>(created),
                        std::get<3>(created), std::get<4>(created),
                        std::get<5>(created), std::get<6>(created)});
        },
        [&diffs](const auto &old, const auto &changed) {
          if (old != changed) {
            // Placement changed/renewed.
            diffs.emplace_back(
                Placement{std::get<0>(old), std::get<1>(old), std::get<2>(old),
                          std::get<1>(changed), std::get<2>(changed),
                          std::get<3>(changed), std::get<4>(changed),
                          std::get<5>(changed), std::get<6>(changed)});
          }
        },
        [](const auto &l, const auto &r) {
          return std::get<0>(l) < std::get<0>(r);
        });

    std::for_each(apps.begin(), apps.end(), [app_trace_clbk](auto &app) {
      app->flush_events(app_trace_clbk);
    });

    std::for_each(diffs.begin(), diffs.end(),
                  [this](auto const &p) { apply(p); });

    save_placement(before, after);

    // Delete evicted and schedule once apps.
    std::for_each(apps.begin(), apps.end(), [this](auto &app) {
      if (app->is_evicted() && app->schedule_once()) {
        del(path_scheduled + "/" + app->name());
      }
    });

    before = std::move(after);

    servers_.for_each([srv_trace_clbk](auto &server) {
      server->flush_events(srv_trace_clbk);
    });

    Event::logger->flush();
    Application::logger->flush();
    Node::logger->flush();

    write_state();

    // Apps that were evicted will be just pending in the next cycle.
    std::for_each(apps.begin(), apps.end(),
                  [](auto &app) { app->clear_evicted(); });

    uptodate_ = true;
    scheduled_at = std::chrono::high_resolution_clock::now();
  }
}

bool Master::load_bucket(int rc, const ZkClient::Path &path,
                         const ZkClient::Data &data,
                         const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  std::string bucket_name(path.substr(path_buckets.size() + 1).data());

  if (!data) {
    return false;
  }

  SPDLOG_INFO("Loading bucket: {}", bucket_name);
  try {
    YAMLParser parser;

    auto bucket = std::make_shared<Bucket>(bucket_name, empty_);
    buckets_.add(bucket_name, bucket);

    std::optional<std::string> parent;
    parser.callback("/parent", [&parent](const auto &data) {
      if (data)
        parent = data;
    });
    parser.parse_stream(data.value().data(), data.value().size());

    if (!parent) {
      cell_->add_node(bucket);
    } else {
      const auto &parent_name = parent.value();
      if (buckets_.find(parent_name) == buckets_.end()) {
        buckets_.add_async(parent_name, [this, parent_name]() {
          std::string zkpath = path_buckets + "/" + parent_name;
          watch_data(zkpath, &bucket_watch_, false);
        });
      }

      buckets_.when_ready(parent_name,
                          [bucket](auto parent) { parent->add_node(bucket); });
    }
  } catch (std::invalid_argument &yaml_err) {
    SPDLOG_WARN("{}: {}", yaml_err.what(), data.value().data());
  }

  return false;
}

bool Master::load_server(int rc, const ZkClient::Path &path,
                         const ZkClient::Data &data,
                         const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  std::string server_name(path.substr(path_servers.size() + 1).data());

  if (!data) {
    return false;
  }

  SPDLOG_INFO("Loading server: {}", server_name);
  try {
    YAMLParser parser;

    std::optional<std::string> parent;
    parser.callback("/parent", [&parent](const auto &data) {
      if (data) {
        parent = data;
      }
    });

    std::string partition = defaults::default_partition;
    parser.callback("/partition", [&partition](const auto &data) {
      if (data) {
        partition = data;
      }
    });

    volume_t capacity(empty_);
    parser.callback("/memory", [&capacity](auto data) {
      if (data) {
        capacity[0] = bytes2number(data);
      }
    });
    parser.callback("/cpu", [&capacity](auto data) {
      if (data) {
        capacity[1] = cpu2number(data);
      }
    });
    parser.callback("/disk", [&capacity](auto data) {
      if (data) {
        capacity[2] = bytes2number(data);
      }
    });
    parser.callback("/gpu", [&capacity](auto data) {
      if (data) {
        capacity[3] = to_number(data);
      }
    });

    traits_t traits = {0};
    parser.callback("/traits", [this, &traits](auto data) {
      if (data) {
        traits |= trait(data);
      }
    });

    traits_t required_traits = {0};
    parser.callback("/required_traits", [this, &required_traits](auto data) {
      if (data) {
        required_traits |= trait(data);
      }
    });

    parser.parse_stream(data.value().data(), data.value().size());

    if (!parent) {
      SPDLOG_WARN("Server {} has no parent, skipping", server_name);
      return true;
    }
    const auto &parent_name = parent.value();

    auto found = servers_.find(server_name);
    if (found != servers_.end() && found->second.element) {
      auto current_server = found->second.element.value();
      if (current_server->partition() == partition &&
          current_server->traits() == traits &&
          current_server->required_traits() == required_traits &&
          all_eq(current_server->size(), capacity) &&
          current_server->parent_name() == parent_name) {
        SPDLOG_INFO("Server {} is the same, skipping", server_name);
        return true;
      }
    }

    auto server = std::make_shared<Server>(server_name, parent_name);
    server->partition(partition);
    server->labels(label(partition));
    server->traits(traits);
    server->required_traits(required_traits);
    server->capacity(capacity);
    if (buckets_.find(parent_name) == buckets_.end()) {
      buckets_.add_async(parent_name, [this, parent_name]() {
        std::string zkpath = path_buckets + "/" + parent_name;
        watch_data(zkpath, &bucket_watch_, false);
      });
    }
    buckets_.when_ready(parent_name,
                        [server](auto bucket) { bucket->add_node(server); });
    servers_.add(server_name, server);

    // TODO: Unclear if this is proper place to create server placement node.
    //       Rather, this should be done in load_server_presence and server
    //       state set accordingly.
    namespace z = treadmill::zknamespace::constants;
    auto placement_node = fmt::format("{}/{}", z::placement, server_name);
    SPDLOG_INFO("Create server placement node: {}", placement_node);
    create(placement_node, "{}");

  } catch (std::invalid_argument &yaml_err) {
    SPDLOG_WARN("{}: {}", yaml_err.what(), data.value().data());
  }

  return true;
}

bool Master::load_servers(int rc, const ZkClient::Path &path,
                          const ZkClient::List &nodes) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!nodes) {
    SPDLOG_DEBUG("load_servers: rc = {}", rc);
    return false;
  }

  auto servers = nodes.value().sorted();
  servers_.reconcile(servers.begin(), servers.end(), [this](auto &name) {
    std::string zkpath = path_servers + "/" + name;
    watch_data(zkpath, &server_watch_, false);
  });

  msg_enqueue(Message::load_servers);
  SPDLOG_INFO("Loaded servers.");
  return true;
}

bool Master::load_blackedout_servers(int rc, const ZkClient::Path &path,
                                     const ZkClient::List &nodes) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!nodes) {
    SPDLOG_DEBUG("load_blackedout_servers: rc = {}", rc);
    return false;
  }

  auto blackedout_servers = nodes.value().sorted();

  std::vector<std::string> added;
  std::set_difference(blackedout_servers.begin(), blackedout_servers.end(),
                      blackedout_servers_.begin(), blackedout_servers_.end(),
                      std::back_inserter(added));
  for (auto &server_name : added) {
    blackedout_servers_.insert(server_name);
    if (ready_) {
      servers_.when_ready(server_name, [](auto &server) {
        server->template record<ServerBlackoutEvent>();
      });
    }
  }

  std::vector<std::string> removed;
  std::set_difference(blackedout_servers_.begin(), blackedout_servers_.end(),
                      blackedout_servers.begin(), blackedout_servers.end(),
                      std::back_inserter(removed));
  for (auto &server_name : removed) {
    blackedout_servers_.erase(server_name);
    if (ready_) {
      servers_.when_ready(server_name, [](auto &server) {
        server->template record<ServerBlackoutClearedEvent>();
      });
    }
  }

  msg_enqueue(Message::load_blackedout_servers);
  SPDLOG_INFO("Loaded blackedout servers.");
  return true;
}

bool Master::load_server_presence(int rc, const ZkClient::Path &path,
                                  const ZkClient::Data &data,
                                  const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  std::string server_name(path.substr(path_presence.size() + 1).data());

  if (!data) {
    return false;
  }

  auto ctime = msec_to_time_point(stat.value().ctime);
  std::optional<time_point_t> valid_until;

  YAMLParser parser;
  parser.callback("/valid_until", [&valid_until](auto data) {
    if (data) {
      valid_until = sec_to_time_point(data);
    }
  });
  parser.parse_stream(data.value().data(), data.value().size());

  servers_.when_ready(server_name,
                      [this, server_name, valid_until, ctime](auto &server) {
                        auto server_presence = std::make_shared<ServerPresence>(
                            server_name, server, valid_until, ctime);
                        server_presence_.add(server_name, server_presence);
                        save_server_placement(server_name, ServerState::up);
                      });

  return true;
}

bool Master::load_servers_presence(int rc, const ZkClient::Path &path,
                                   const ZkClient::List &nodes) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!nodes) {
    SPDLOG_DEBUG("load_servers_presence: rc = {}", rc);
    return false;
  }

  auto servers = nodes.value().sorted();

  server_presence_.reconcile(
      servers.begin(), servers.end(),
      [this](auto &name) {
        std::string server_zkpath = path_servers + "/" + name;
        watch_data(server_zkpath, &server_watch_, false);
        std::string presence_zkpath = path_presence + "/" + name;
        watch_data(presence_zkpath, &server_presence_watch_, false);
      },
      [this](auto &name) { save_server_placement(name, ServerState::down); });

  msg_enqueue(Message::load_servers_presence);
  SPDLOG_INFO("Loaded server presence.");
  return true;
}

bool Master::load_buckets(int rc, const ZkClient::Path &path,
                          const ZkClient::List &nodes) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!nodes) {
    SPDLOG_DEBUG("load_buckets: rc = {}", rc);
    return false;
  }

  auto buckets = nodes.value().sorted();
  buckets_.reconcile(buckets.begin(), buckets.end(), [this](auto &name) {
    std::string zkpath = path_buckets + "/" + name;
    watch_data(zkpath, &bucket_watch_, false);
  });

  msg_enqueue(Message::load_buckets);
  SPDLOG_INFO("Loaded buckets.");
  return true;
}

bool Master::load_allocation(int rc, const ZkClient::Path &path,
                             const ZkClient::Data &data,
                             const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!data) {
    SPDLOG_DEBUG("load_allocation: rc = {}", rc);
    return false;
  }

  struct assignment_entry {
    priority_t priority;
    std::string pattern;
  };

  struct alloc_entry {
    alloc_entry(volume_t empty)
        : capacity(empty), max_utilization(defaults::max_util),
          rank(defaults::default_rank), rank_adjustment(0),
          allocation_group_policy(std::nullopt) {}
    std::string name;
    std::string cell;
    std::string partition;
    volume_t capacity;
    utilization_t max_utilization;
    rank_t rank;
    rank_t rank_adjustment;
    traits_t traits;
    std::vector<assignment_entry> assignments;
    std::optional<AllocationGroupPolicy> allocation_group_policy;
  };

  std::vector<alloc_entry> entries;

  YAMLParser parser;
  parser.callback("/", [this, &entries](auto data) {
    entries.push_back(alloc_entry(empty_));
  });
  parser.callback("/name", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).name = data;
    }
  });
  parser.callback("/cell", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).cell = data;
    }
  });
  parser.callback("/partition", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).partition = data;
    }
  });
  parser.callback("/max_utilization", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).max_utilization = atof(data);
    }
  });
  parser.callback("/memory", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).capacity[0] = bytes2number(data);
    }
  });
  parser.callback("/cpu", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).capacity[1] = cpu2number(data);
    }
  });
  parser.callback("/disk", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).capacity[2] = bytes2number(data);
    }
  });
  parser.callback("/gpu", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).capacity[3] = to_number(data);
    }
  });
  parser.callback("/rank", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).rank = to_number(data);
    }
  });
  parser.callback("/rank_adjustment", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).rank_adjustment = to_number(data);
    }
  });
  parser.callback("/traits", [this, &entries](auto data) {
    if (data) {
      (*entries.rbegin()).traits |= trait(data);
    }
  });
  parser.callback("/assignments", [&entries](auto data) {
    (*entries.rbegin()).assignments.push_back(assignment_entry());
  });
  parser.callback("/assignments/pattern", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).assignments.rbegin()->pattern = data;
    }
  });
  parser.callback("/assignments/priority", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).assignments.rbegin()->priority = to_number(data);
    }
  });
  parser.callback("/allocation_group_policy", [&entries](auto data) {
    if (data) {
      (*entries.rbegin()).allocation_group_policy = str2policy(data);
    }
  });

  parser.parse_stream(data.value().data(), data.value().size());
  for (auto &e : entries) {
    SPDLOG_INFO(e.name);
    for (auto &a : e.assignments) {
      SPDLOG_INFO("  {} {}", a.pattern, a.priority);
    }
  }

  Cell::assignments_t assignments;

  // TODO: do we need map, or vector (sorted in the end) is enough?
  std::map<std::string, std::shared_ptr<Allocation>> allocations;

  for (auto it = entries.begin(); it != entries.end(); ++it) {
    replace(it->name, ':', '/');
    std::string fullname("/" + it->partition + "/" + it->name);

    labels_t labels = label(it->partition);

    auto alloc = std::make_shared<Allocation>(
        fullname, it->capacity, labels, it->rank, it->rank_adjustment,
        it->traits, it->max_utilization, it->allocation_group_policy);
    allocations.emplace(fullname, alloc);
    auto &lst = it->assignments;
    for (auto ait = lst.begin(); ait != lst.end(); ++ait) {
      auto pattern = ait->pattern;
      auto key = assignment_key(pattern);
      auto re = glob2regex(pattern) + "#\\d{10}";
      auto priority = ait->priority;
      auto by_key =
          assignments.emplace(key, Cell::assignments_t::mapped_type());
      by_key.first->second.emplace_back(std::regex(re), priority, alloc);
    }
  }

  auto root = std::make_shared<Allocation>("/", empty_, 0);
  std::vector<std::shared_ptr<Allocation>> stack{root};
  std::vector<std::shared_ptr<Allocation>> root_allocs;
  std::shared_ptr<Allocation> default_alloc(nullptr);
  std::shared_ptr<Allocation> alloc;

  for (auto &[name, leaf_alloc] : allocations) {

    size_t level = 0;
    for (auto pos = name.find("/", 1);; pos = name.find("/", pos + 1)) {

      ++level;
      auto path = name.substr(0, pos);

      if (level < stack.size() && stack[level]->name() == path) {
        // Already created.
        continue;
      }

      if (pos == std::string::npos) {
        alloc = leaf_alloc;
      } else if (path == defaults::default_allocation) {
        alloc = std::make_shared<DefaultAllocation>(empty_);
        default_alloc = alloc;
      } else {
        alloc =
            std::make_shared<Allocation>(path, empty_, leaf_alloc->labels());
      }

      if (stack.size() <= level) {
        stack.push_back(alloc);
      } else {
        stack[level] = alloc;
      }

      if (level == 1) {
        root_allocs.push_back(alloc);
      }
      stack[level - 1]->add_sub_alloc(stack[level]);
      if (pos == std::string::npos) {
        break;
      }
    }
  }

  if (default_alloc == nullptr) {
    default_alloc = std::make_shared<DefaultAllocation>(empty_);
    root_allocs.push_back(default_alloc);
  }

  for (auto &[key, lst] : assignments) {
    SPDLOG_INFO("Assignment key: {} {}", key, lst.size());
  }

  cell_->assignments(assignments);
  cell_->default_assignment({std::regex(".*"), 1, default_alloc});
  apps_.for_each([this](auto &app) { cell_->assign(app); });
  cell_->allocations(root_allocs);

  // TODO: since allocations are loaded, message does not need to go via
  //       Zookeeper watch callback, consider using direct _put instead.
  msg_enqueue(Message::load_allocations);
  return true;
}

bool Master::load_apps(int rc, const ZkClient::Path &path,
                       const ZkClient::List &nodes) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!nodes) {
    return false;
  }

  auto apps = nodes.value().sorted();
  apps_.reconcile(
      apps.begin(), apps.end(),
      [this](auto &name) {
        std::string zkpath = path_scheduled + "/" + name;
        watch_data(zkpath, &app_watch_, false);
      },
      [this](auto &name) {
        app_trace(name, Trace{"deleted", ""});
        // Create finished node if not present.
        std::optional<std::string> server_name;
        auto found = apps_.find(name);
        if (found != apps_.end() && found->second.element) {
          auto app = found->second.element.value();
          auto server = app->server().get();
          if (server) {
            server_name = server->name();
          }
        }
        if (server_name) {
          auto zkpath = fmt::format("{}/{}/{}", path_placement,
                                    server_name.value(), name);
          watch_data(zkpath, &unscheduled_app_placement_watch_, false);
        } else {
          create_finished_node(name, std::nullopt, std::nullopt, std::nullopt);
        }
      });

  msg_enqueue(Message::load_apps);
  SPDLOG_INFO("Loaded apps.");
  return true;
}

bool Master::load_app(int rc, const ZkClient::Path &path,
                      const ZkClient::Data &data, const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!data) {
    return false;
  }

  std::string app_name(path.substr(path_scheduled.size() + 1).data());
  SPDLOG_INFO("Load app: {}", app_name);

  YAMLParser parser;
  duration_t data_retention{0};
  parser.callback("/data_retention_timeout", [&data_retention](auto data) {
    if (data) {
      data_retention = str2duration(data);
    }
  });

  duration_t lease{0};
  parser.callback("/lease", [&lease](auto data) {
    if (data) {
      lease = str2duration(data);
    }
  });

  volume_t demand(empty_);
  parser.callback("/memory", [&demand](auto data) {
    if (data) {
      demand[0] = bytes2number(data);
    }
  });
  parser.callback("/cpu", [&demand](auto data) {
    if (data) {
      demand[1] = cpu2number(data);
    }
  });
  parser.callback("/disk", [&demand](auto data) {
    if (data) {
      demand[2] = bytes2number(data);
    }
  });
  parser.callback("/gpu", [&demand](auto data) {
    if (data) {
      demand[3] = to_number(data);
    }
  });

  std::string affinity = app_name.substr(0, app_name.find('.'));
  parser.callback("/affinity", [&affinity](auto data) {
    if (data) {
      affinity = data;
    }
  });

  affinity_limits_t affinity_limits(defaults::default_affinity_limit,
                                    topology_.levels.size());

  static std::vector<std::pair<std::string, size_t>> top_paths;
  if (top_paths.size() == 0) {
    for (auto &[name, level] : topology_.levels) {
      SPDLOG_INFO("Caching topology levels: {} {}", name, level);
      top_paths.emplace_back("/affinity_limits/" + name, level);
    }
  }

  for (auto &pair : top_paths) {
    auto level = pair.second;
    parser.callback(pair.first, [level, &affinity_limits](auto data) {
      if (data) {
        affinity_limits[level] = to_number(data);
      }
    });
  }

  bool schedule_once = false;
  parser.callback("/schedule_once", [&schedule_once](auto data) {
    if (data) {
      schedule_once = to_bool(data);
    }
  });

  priority_t priority = 0;
  parser.callback("/priority", [&priority](auto data) {
    if (data) {
      priority = to_number(data);
    }
  });

  std::optional<std::string> igroup_name;
  parser.callback("/identity_group", [&igroup_name](auto data) {
    if (data) {
      igroup_name = data;
    }
  });

  Identity identity = std::nullopt;

  traits_t traits = {0};
  parser.callback("/traits", [this, &traits](auto data) {
    if (data) {
      traits |= trait(data);
    }
  });

  labels_t labels = {0};
  bool renew = false;
  bool blacklisted = false;

  parser.parse_stream(data.value().data(), data.value().size());

  if (!apps_.ready(app_name)) {
    auto app = std::make_shared<Application>(
        app_name, priority, demand, Affinity(affinity, affinity_limits), lease,
        data_retention, identity, igroup_name, traits, labels, schedule_once,
        renew, blacklisted);
    apps_.add(app_name, app);
    cell_->assign(app);

    if (igroup_name) {
      igroups_.when_ready(igroup_name.value(), [app, igroup_name](auto &group) {
        app->identity_group(group);
      });
    }
  } else {
    apps_.when_ready(app_name, [priority, app_name](auto app) {
      SPDLOG_INFO("setting app priority: {} {}", app_name, priority);
      app->priority(priority);
    });
  }
  return true;
}

bool Master::load_igroups(int rc, const ZkClient::Path &path,
                          const ZkClient::List &nodes) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!nodes) {
    return false;
  }

  auto groups = nodes.value().sorted();
  igroups_.reconcile(groups.begin(), groups.end(), [this](auto &name) {
    std::string zkpath = path_igroups + "/" + name;
    watch_data(zkpath, &igroup_watch_, ready_);
  });

  msg_enqueue(Message::load_igroups);
  SPDLOG_INFO("Loaded identity groups");
  return true;
}

bool Master::load_igroup(int rc, const ZkClient::Path &path,
                         const ZkClient::Data &data,
                         const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!data) {
    return false;
  }

  std::string group_name(path.substr(path_igroups.size() + 1).data());
  SPDLOG_INFO("Load identity group: {}", group_name);

  size_t count = 0;

  YAMLParser parser;
  parser.callback("/count", [&count](auto data) {
    if (data) {
      count = to_number(data);
      SPDLOG_INFO("Identity group count: {}", count);
    }
  });
  parser.parse_stream(data.value().data(), data.value().size());

  if (!igroups_.ready(group_name)) {
    auto group = std::make_shared<IdentityGroup>(group_name, count);
    igroups_.add(group_name, group);
  }
  igroups_.when_ready(group_name,
                      [count](auto group) { group->adjust(count); });
  return true;
}

bool Master::restore_placement(int rc, const ZkClient::Path &path,
                               const ZkClient::Data &data,
                               const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  SPDLOG_INFO("Restoring placement: {}", path);

  if (!data) {
    return false;
  }

  std::string server_name(path.substr(path_placement.size() + 1).data());
  SPDLOG_INFO("Restore server state: {}", server_name);

  // Data integrity issue. Server must be acknowledged, even if it is not
  // fully loaded yet.
  if (servers_.find(server_name) == servers_.end()) {
    SPDLOG_INFO("Invalid server: /placement/{}", server_name);
    return false;
  }

  try {
    YAMLParser parser;
    time_point_t since;
    parser.callback("/since", [&since](auto data) {
      if (data) {
        since = sec_to_time_point(data);
      }
    });

    std::string state;
    parser.callback("/state", [&state](auto data) {
      if (data) {
        state = data;
      }
    });

    parser.parse_stream(data.value().data(), data.value().size());

    if (state == "frozen") {
      servers_.when_ready(server_name, [since](auto server) {
        server->state(ServerState::frozen, since);
      });
    }
  } catch (std::invalid_argument &yaml_err) {
    SPDLOG_WARN("{}: {}", yaml_err.what(), data.value().data());
  }
  return false;
}

bool Master::restore_placements(int rc, const ZkClient::Path &path,
                                const ZkClient::List &nodes) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!nodes) {
    return false;
  }

  auto servers = nodes.value().sorted();
  for (auto &name : servers) {
    std::string server_placement = path_placement + "/" + name;
    SPDLOG_DEBUG("Restore placement: {}", server_placement);
    watch_data(server_placement, &placement_watch_, false);
    watch_children(server_placement, &app_placements_watch_, false);
  }

  msg_enqueue(Message::load_placements);
  return false;
}

bool Master::restore_app_placements(int rc, const ZkClient::Path &path,
                                    const ZkClient::List &nodes) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  SPDLOG_INFO("Restoring app placements: {}", path);
  if (!nodes) {
    return false;
  }

  auto apps = nodes.value().sorted();
  for (auto &name : apps) {
    std::string app_placement_path =
        std::string(path.data(), path.size()) + "/" + name;
    SPDLOG_DEBUG("Restore app placement: {}", app_placement_path);
    watch_data(app_placement_path, &app_placement_watch_, false);
  }

  msg_enqueue(Message::load_app_placements);
  return false;
}

bool Master::restore_app_placement(int rc, const ZkClient::Path &path,
                                   const ZkClient::Data &data,
                                   const ZkClient::Stat &stat) {

  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  SPDLOG_INFO("Restoring app placement: {}", path);

  if (!data) {
    return false;
  }

  auto pos = path.find_last_of('/');
  auto app_name_v = path.substr(pos + 1);
  auto server_name_v =
      path.substr(path_placement.size() + 1, pos - path_placement.size() - 1);

  std::string app_name(app_name_v.data(), app_name_v.size());
  std::string server_name(server_name_v.data(), server_name_v.size());

  try {
    YAMLParser parser;

    std::optional<std::string> alloc;
    parser.callback("/allocation", [&alloc](const auto &data) {
      if (data) {
        alloc = data;
      }
    });

    time_point_t expires;
    parser.callback("/expires", [&expires](auto data) {
      if (data) {
        expires = sec_to_time_point(data);
      }
    });

    Identity identity;
    parser.callback("/identity", [&identity](auto data) {
      if (data) {
        identity = to_number(data);
      }
    });

    parser.parse_stream(data.value().data(), data.value().size());

    restored_allocs_[app_name] = alloc;

    auto placement_time = msec_to_time_point(stat.value().ctime);

    server_presence_.when_ready(
        server_name,
        [this, app_name, server_name, placement_time, identity,
         expires](auto &presence) {
          if (presence->ctime() <= placement_time) {
            servers_.when_ready(server_name, [this, app_name, server_name,
                                              identity, expires](auto &server) {
              apps_.when_ready(
                  app_name,
                  [server, identity, expires, app_name](auto &app) {
                    if (identity) {
                      SPDLOG_INFO("Restore identity: {} - {}", app_name,
                                  identity.value());
                      app->acquire_identity(identity);
                    }
                    app->server(server);
                    app->expires_at(expires);
                    SPDLOG_INFO("Restore placement: {} - {}", app->name(),
                                server->name());
                  },
                  [this, server_name, app_name]() {
                    namespace z = treadmill::zknamespace::constants;
                    auto placement_node = fmt::format("{}/{}/{}", z::placement,
                                                      server_name, app_name);
                    SPDLOG_INFO("Delete stale placement: {}", placement_node);
                    del(placement_node);
                  });
            });
          } else {
            SPDLOG_INFO("Re-evaluate placement: {} - {}", app_name,
                        server_name);
            namespace z = treadmill::zknamespace::constants;
            auto placement_node =
                fmt::format("{}/{}/{}", z::placement, server_name, app_name);
            SPDLOG_INFO("Delete stale placement: {}", placement_node);
            del(placement_node);
          }
        },
        [this, app_name, server_name]() {
          SPDLOG_INFO("Server is down: {}", server_name);
          namespace z = treadmill::zknamespace::constants;
          auto placement_node =
              fmt::format("{}/{}/{}", z::placement, server_name, app_name);
          SPDLOG_INFO("Delete stale placement: {}", placement_node);
          del(placement_node);
        });

  } catch (std::invalid_argument &yaml_err) {
    SPDLOG_WARN("{}: {}", yaml_err.what(), data.value().data());
  }
  return false;
}

bool Master::terminate_unscheduled_app(int rc, const ZkClient::Path &path,
                                       const ZkClient::Data &data,
                                       const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (rc == ZOK) {
    terminate_app(std::string(path.data(), path.size()), format_unique_id(stat),
                  std::nullopt);
  } else if (rc == ZNONODE) {
    terminate_app(std::string(path.data(), path.size()), std::nullopt,
                  std::nullopt);
  } else {
    SPDLOG_WARN("terminate_unscheduled_app rc: {}, path: {}", rc, path);
  }
  return false;
}

bool Master::terminate_schedule_once_app(int rc, const ZkClient::Path &path,
                                         const ZkClient::Data &data,
                                         const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (rc == ZOK) {
    terminate_app(std::string(path.data(), path.size()), format_unique_id(stat),
                  "schedule_once");
  } else if (rc == ZNONODE) {
    terminate_app(std::string(path.data(), path.size()), std::nullopt,
                  "schedule_once");
  } else {
    SPDLOG_WARN("terminate_schedule_once_app rc: {}, path: {}", rc, path);
  }
  return false;
}

void Master::terminate_app(const std::string &placement_path,
                           std::optional<std::string> unique_id,
                           std::optional<std::string> reason) {
  auto parts = split(placement_path, '/');
  CHECK(parts.size() == 4);
  auto app_name = parts[parts.size() - 1];
  auto server_name = parts[parts.size() - 2];

  create_finished_node(app_name, server_name, unique_id, reason);

  if (unique_id) {
    SPDLOG_INFO("zk.delete: {}", placement_path);
    del(placement_path);
  }
}

void Master::create_finished_node(std::string app_name,
                                  std::optional<std::string> server_name,
                                  std::optional<std::string> unique_id,
                                  std::optional<std::string> reason) {
  SPDLOG_INFO("Create finished node: {} {}", app_name,
              reason ? reason.value() : "");
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  {
    auto now = std::chrono::high_resolution_clock::now();
    namespace js = treadmill::js;
    js::Object obj(&writer);
    obj("state", "terminated");
    obj("when", timestamp_str(now));
    obj("host", server_name);
    obj("uniqueid", unique_id);
    obj("data", reason);
  }
  auto finished_node = fmt::format("{}/{}", path_finished, app_name);
  create(finished_node, sb.GetString());
}

bool Master::load_events(int rc, const ZkClient::Path &path,
                         const ZkClient::List &nodes) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!nodes) {
    return false;
  }

  auto events = nodes.value().sorted();
  events_.reconcile(events.begin(), events.end(), [this](auto &name) {
    std::string zkpath = path_events + "/" + name;
    watch_data(zkpath, &event_watch_, false);
  });

  msg_enqueue(Message::load_events);
  SPDLOG_INFO("Loaded events.");
  return true;
}

bool Master::load_event(int rc, const ZkClient::Path &path,
                        const ZkClient::Data &data,
                        const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (!data) {
    return false;
  }

  std::string event_name(path.substr(path_events.size() + 1).data());
  std::vector<std::string> parts = split(event_name, '-');
  if (parts.size() != 3) {
    SPDLOG_WARN("Invalid event: {}", event_name);
    del(path.data());
    return false;
  }
  auto category = parts[1];
  SPDLOG_INFO("Load event: {} {}", event_name, data.value());

  YAMLParser parser;

  struct ServerEvent {
    typedef std::function<void(const std::string &, const std::string &,
                               const std::vector<std::string> &)>
        clbk_t;
    ServerEvent(clbk_t clbk) : clbk_(clbk) {}
    clbk_t clbk_;
    std::string server;
    std::string state;
    std::vector<std::string> unschedule;

    ~ServerEvent() { clbk_(server, state, unschedule); }
  };

  std::shared_ptr<ServerEvent> srv_event;

  if (category == "apps") {
    parser.callback("/", [this](auto name) {
      SPDLOG_INFO("app event - {}", name);
      std::string zkpath = path_scheduled + "/" + name;
      watch_data(zkpath, &app_watch_, false);
    });
  } else if (category == "servers") {
    parser.callback("/", [this](auto name) {
      SPDLOG_INFO("server event - {}", name);
      std::string zkpath = path_servers + "/" + name;
      watch_data(zkpath, &server_watch_, false);
    });
  } else if (category == "server_state") {
    srv_event = std::make_shared<ServerEvent>(
        [this](auto &server, auto &state, auto &unschedule) {
          SPDLOG_INFO("server state event: {} {} {}", server, state,
                      unschedule.size());
          auto srv_state = str2state(state);
          if (srv_state) {
            save_server_placement(server, srv_state.value());
            servers_.when_ready(server, [&srv_state](auto srv) {
              srv->state(srv_state.value());
              srv->template record<ServerStateEvent>(srv_state.value());
            });
          }
          if (srv_state == ServerState::frozen) {
            for (auto &app_name : unschedule) {
              auto found = apps_.find(app_name);
              if (found != apps_.end() && found->second.element) {
                auto app = found->second.element.value();
                app->unschedule(true);
              }
            }
          }
        });

    parser.callback("/", [srv_event](auto value) {
      if (value) {
        SPDLOG_INFO("server state event - {}", value);
        if (srv_event->server.empty()) {
          srv_event->server = value;
        } else if (srv_event->state.empty()) {
          srv_event->state = value;
        } else {
          srv_event->unschedule.push_back(value);
        }
      } else {
        SPDLOG_INFO("server event - null");
      }
    });
  } else if (category == "identity_groups") {
    parser.callback("/", [this](auto name) {
      SPDLOG_INFO("identity group event - {}", name);
      std::string zkpath = path_igroups + "/" + name;
      watch_data(zkpath, &igroup_watch_, false);
    });
  } else {
    SPDLOG_ERROR("Unsupported event category: {}", category);
  }

  parser.parse_stream(data.value().data(), data.value().size());
  del(path.data());

  auto event = std::make_shared<SchedulerEvent>(event_name);
  events_.add(event_name, event);

  return false;
}

void Master::load_all() {
  CHECK(zkclient_);

  // TODO: Should we load buckets here or rely on recursive server / bucket
  //       parent discovery?
  watch_children(path_buckets, &buckets_watch_, ready_);
  watch_children(path_servers, &servers_watch_, ready_);
  watch_children(path_blackedout_servers, &blackedout_servers_watch_, ready_);
  watch_children(path_presence, &servers_presence_watch_, ready_);
  watch_data(path_allocations, &allocation_watch_, ready_);
  watch_children(path_igroups, &igroups_watch_, ready_);
  watch_children(path_scheduled, &apps_watch_, ready_);
  watch_children(path_events, &events_watch_, ready_);
}

void Master::load_placement() {
  watch_children(path_placement, &placements_watch_, false);
}

void Master::schedule(const std::string &app, const std::string &server,
                      time_point_t expires_at) {
  CHECK(scheduler_thread_id == std::this_thread::get_id());
  auto epoch = expires_at.time_since_epoch();
  auto value = std::chrono::duration_cast<std::chrono::seconds>(epoch);
  SPDLOG_INFO(" + {} {} {}", app, server, value.count());
}

void Master::unschedule(const std::string &app, const std::string &server) {
  CHECK(scheduler_thread_id == std::this_thread::get_id());
  SPDLOG_INFO(" - {} {}", app, server);
}

void Master::apply(const Placement &placement) {

  CHECK(scheduler_thread_id == std::this_thread::get_id());
  namespace z = treadmill::zknamespace::constants;

  SPDLOG_INFO("Apply placement: {} {} => {}/{}", placement.app,
              placement.old_server ? placement.old_server.value() : "~",
              placement.new_server ? placement.new_server.value() : "~",
              placement.new_expires_at
                  ? sec_from_epoch(placement.new_expires_at.value())
                  : 0);

  if (placement.new_server) {
    // Placement created or changed/renewed.
    bool renewed = false;
    if (placement.old_server) {
      if (placement.old_server.value() != placement.new_server.value()) {
        auto old_placement_node =
            fmt::format("{}/{}/{}", z::placement, placement.old_server.value(),
                        placement.app);
        SPDLOG_INFO("zk.delete: {}", old_placement_node);
        del(old_placement_node);
      } else {
        renewed = true;
      }
    }

    auto new_placement_node = fmt::format(
        "{}/{}/{}", z::placement, placement.new_server.value(), placement.app);
    auto data = placement_data(placement);
    if (!renewed) {
      SPDLOG_INFO("zk.create: {} {}", new_placement_node, data);
      create(new_placement_node, data);
    } else {
      SPDLOG_INFO("zk.set: {} {}", new_placement_node, data);
      set(new_placement_node, data);
    }
  } else {
    // Placement deleted.
    CHECK(placement.old_server);
    auto placement_node = fmt::format(
        "{}/{}/{}", z::placement, placement.old_server.value(), placement.app);

    if (placement.schedule_once) {
      // Create finished node with "schedule_once" reason and delete placement.
      watch_data(placement_node, &schedule_once_app_placement_watch_, false);
    } else {
      SPDLOG_INFO("zk.delete: {}", placement_node);
      del(placement_node);
    }
  }
}

void Master::save_placement(const std::set<placement_t> &before,
                            const std::set<placement_t> &after) {
  CHECK(scheduler_thread_id == std::this_thread::get_id());
  SPDLOG_INFO("Saving final placement.");
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);

  {
    namespace js = treadmill::js;
    js::Array arr(&writer);

    for_each_diff(
        before.begin(), before.end(), after.begin(), after.end(),
        [&arr](const auto &deleted) {
          auto data = arr.arr();
          data << std::get<0>(deleted) << std::get<1>(deleted)
               << sec_from_epoch(std::get<2>(deleted)) << std::nullopt
               << std::nullopt;
        },
        [&arr](const auto &created) {
          auto data = arr.arr();
          data << std::get<0>(created) << std::nullopt << std::nullopt
               << std::get<1>(created) << sec_from_epoch(std::get<2>(created));
        },
        [&arr](const auto &old, const auto &changed) {
          auto data = arr.arr();
          data << std::get<0>(old) << std::get<1>(old)
               << sec_from_epoch(std::get<2>(old)) << std::get<1>(changed)
               << sec_from_epoch(std::get<2>(changed));
        },
        [](const auto &l, const auto &r) {
          return std::get<0>(l) < std::get<0>(r);
        });
  }

  // 1MB limit for zookeeper nodes.
  uLong compr_len = 1024 * 1024;
  Byte *compr = (Byte *)calloc((uInt)compr_len, 1);
  int err =
      compress(compr, &compr_len, (const Bytef *)sb.GetString(), sb.GetSize());
  CHECK(err == 0);
  set(path_placement, (const char *)compr, (size_t)compr_len);
  free(compr);
}

void Master::save_server_placement(const std::string &server,
                                   ServerState state) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  std::string server_placement = path_placement + "/" + server;
  std::string data = server_placement_data(state2str(state).value());
  set(server_placement, data);
}

void Master::update_task(const std::string &appname,
                         const std::optional<std::string> &server,
                         const std::optional<std::string> &why) {
  CHECK(scheduler_thread_id == std::this_thread::get_id());
  SPDLOG_INFO("Update task: {} {} {}", appname, server ? server.value() : "-",
              why ? why.value() : "-");
}

void Master::on_create(int rc, const ZkClient::Data &path) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (updates_counter_)
    updates_counter_->dec();
  if (rc != ZOK) {
    SPDLOG_DEBUG("zk.create callback: {}", rc);
  }
}

void Master::on_ensure_exists(int rc, const ZkClient::Data &path) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (updates_counter_)
    updates_counter_->dec();
  if (!(rc == ZOK || rc == ZNODEEXISTS)) {
    SPDLOG_DEBUG("zk.ensure_exists_clbk_ callback: {}", rc);
  }
}

void Master::on_set(int rc, const ZkClient::Stat &stat) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (updates_counter_)
    updates_counter_->dec();
  if (rc != ZOK) {
    SPDLOG_DEBUG("zk.set callback: {}", rc);
  }
}

void Master::on_delete(int rc) {
  CHECK(zk_clbk_thread_id == std::this_thread::get_id());
  if (updates_counter_)
    updates_counter_->dec();
  if (rc != ZOK) {
    SPDLOG_DEBUG("zk.delete callback: {}", rc);
  }
}

void Master::trace_event(const std::string &name, const Trace &trace,
                         const std::filesystem::path &ev_dir) {
  auto now = std::chrono::high_resolution_clock::now();
  auto timestamp = timestamp_str(now);
  auto event =
      fmt::format("{},{},{},{}", timestamp, name, trace.action, trace.data);
  auto event_fname = ev_dir / event;
  SPDLOG_INFO("Trace: {}", event_fname.c_str());
  if (!readonly_) {
    atomic_write(event_fname, std::nullopt, "", 0);
  }
}

void Master::app_trace(const std::string &app_name, const Trace &trace) {
  CHECK((scheduler_thread_id == std::this_thread::get_id()) ||
        (zk_clbk_thread_id == std::this_thread::get_id()));
  trace_event(app_name, trace, appev_dir_);
}

void Master::srv_trace(const std::string &srv_name, const Trace &trace) {
  CHECK((scheduler_thread_id == std::this_thread::get_id()) ||
        (zk_clbk_thread_id == std::this_thread::get_id()));
  trace_event(srv_name, trace, srvev_dir_);
}

void Master::write_apps_state() const {
  CHECK(scheduler_thread_id == std::this_thread::get_id());
  auto fname = state_dir_ / "apps.json";
  atomic_write(fname, std::nullopt, std::nullopt, 0, [this](int fd) {
    FILE *fp = fdopen(dup(fd), "w+");
    char writeBuffer[65536];
    rapidjson::FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));
    rapidjson::Writer<rapidjson::FileWriteStream> writer(os);

    {
      namespace js = treadmill::js;
      js::Array arr(&writer);

      for (auto &alloc : cell_->allocations()) {
        const auto &queue = alloc->queue(cell_->size(alloc->labels()));
        for (auto entry = queue.begin(); entry != queue.end(); ++entry) {
          auto &app = entry->app;
          if (!app) {
            continue;
          }
          auto obj = arr.obj();
          obj("name", app->name());
          obj("blacklisted", app->blacklisted());
          obj("priority", app->priority());
          obj("lease", app->lease());
          obj("expires", app->expires_at());
          obj("affinity", app->affinity());
          obj("data_retention_timeout", app->data_retention_timeout());

          obj("identity", app->identity());
          auto identity_group = app->identity_group().get();
          std::optional<std::string> identity_group_name;
          if (identity_group) {
            identity_group_name = identity_group->name();
          }
          obj("identity_group", identity_group_name);

          auto traits = decode_bitset(app->traits(), traits_);
          obj("traits", std::begin(traits), std::end(traits));

          auto labels = decode_bitset(app->labels(), labels_);
          obj("labels", std::begin(labels), std::end(labels));

          write_volume(obj, "size", app->demand());

          auto server = app->server().get();
          std::optional<std::string> server_name;
          if (server) {
            server_name = server->name();
          }
          obj("server", server_name);

          std::optional<std::string> partition;
          std::optional<std::string> allocation;
          if (app->assigned_alloc()) {
            partition = alloc_partition(app->assigned_alloc().value());
            allocation = alloc_name(app->assigned_alloc().value());
          }
          obj("partition", partition);
          obj("allocation", allocation);

          obj("rank", entry->rank);
          obj("utilization_before", entry->before);
          obj("utilization_after", entry->after);
          obj("pending", entry->pending);
        }
      }
    }

    fclose(fp);
  });
}

void Master::write_servers_state() const {
  CHECK(scheduler_thread_id == std::this_thread::get_id());
  auto fname = state_dir_ / "servers.json";
  atomic_write(fname, std::nullopt, std::nullopt, 0, [this](int fd) {
    FILE *fp = fdopen(dup(fd), "w+");
    char writeBuffer[65536];
    rapidjson::FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));
    rapidjson::Writer<rapidjson::FileWriteStream> writer(os);

    {
      namespace js = treadmill::js;
      js::Array arr(&writer);

      servers_.for_each([this, &arr](auto &server) {
        auto obj = arr.obj();
        obj("name", server->name());
        obj("state", state2str(server->state()));
        obj("valid_until", server->valid_until());
        obj("topology", server->topology());
        obj("partition", server->partition());

        auto traits = decode_bitset(server->traits(), traits_);
        obj("traits", std::begin(traits), std::end(traits));

        auto required_traits =
            decode_bitset(server->required_traits(), traits_);
        obj("required_traits", std::begin(required_traits),
            std::end(required_traits));

        auto labels = decode_bitset(server->labels(), labels_);
        obj("labels", std::begin(labels), std::end(labels));

        write_volume(obj, "size", server->size());
        write_volume(obj, "capacity", server->capacity());
      });
    }

    fclose(fp);
  });
}

void Master::write_allocations_state() const {
  CHECK(scheduler_thread_id == std::this_thread::get_id());
  auto fname = state_dir_ / "allocations.json";
  atomic_write(fname, std::nullopt, std::nullopt, 0, [this](int fd) {
    FILE *fp = fdopen(dup(fd), "w+");
    char writeBuffer[65536];
    rapidjson::FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));
    rapidjson::Writer<rapidjson::FileWriteStream> writer(os);

    {
      namespace js = treadmill::js;
      js::Array arr(&writer);

      auto root_allocs = cell_->allocations();
      for (auto &root_alloc : root_allocs) {
        std::vector<std::shared_ptr<Allocation>> stack(
            root_alloc->sub_allocs());
        while (!stack.empty()) {
          auto alloc = stack.back();
          stack.pop_back();
          for (auto &sub_alloc : alloc->sub_allocs()) {
            stack.push_back(sub_alloc);
          }
          auto obj = arr.obj();
          obj("name", alloc_name(alloc->name()));
          obj("partition", alloc_partition(alloc->name()));

          obj("rank", alloc->rank());
          obj("rank_adjustment", alloc->rank_adjustment());

          std::optional<utilization_t> max_utilization;
          if (alloc->max_utilization() < defaults::max_util) {
            max_utilization = alloc->max_utilization();
          }
          obj("max_utilization", max_utilization);

          auto traits = decode_bitset(alloc->traits(), traits_);
          obj("traits", std::begin(traits), std::end(traits));

          auto labels = decode_bitset(alloc->labels(), labels_);
          obj("labels", std::begin(labels), std::end(labels));

          write_volume(obj, "reserved", alloc->reserved());

          std::optional<std::string> allocation_group_policy;
          if (alloc->allocation_group_policy()) {
            allocation_group_policy =
                policy2str(alloc->allocation_group_policy().value());
          }
          obj("allocation_group_policy", allocation_group_policy);
        }
      }
    }

    fclose(fp);
  });
}

void Master::write_labels_state() const {
  CHECK(scheduler_thread_id == std::this_thread::get_id());
  auto fname = state_dir_ / "labels.json";
  atomic_write(fname, std::nullopt, std::nullopt, 0, [this](int fd) {
    FILE *fp = fdopen(dup(fd), "w+");
    char writeBuffer[65536];
    rapidjson::FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));
    rapidjson::Writer<rapidjson::FileWriteStream> writer(os);

    {
      namespace js = treadmill::js;
      js::Array arr(&writer);

      for (auto &lbl : labels_) {
        auto obj = arr.obj();
        obj("name", lbl.first);
      }
    }

    fclose(fp);
  });
}

void Master::write_traits_state() const {
  CHECK(scheduler_thread_id == std::this_thread::get_id());
  auto fname = state_dir_ / "traits.json";
  atomic_write(fname, std::nullopt, std::nullopt, 0, [this](int fd) {
    FILE *fp = fdopen(dup(fd), "w+");
    char writeBuffer[65536];
    rapidjson::FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));
    rapidjson::Writer<rapidjson::FileWriteStream> writer(os);

    {
      namespace js = treadmill::js;
      js::Array arr(&writer);

      for (auto &trait : traits_) {
        auto obj = arr.obj();
        obj("name", trait.first);
      }
    }

    fclose(fp);
  });
}

void Master::write_state() const {
  CHECK(scheduler_thread_id == std::this_thread::get_id());
  write_labels_state();
  write_traits_state();
  write_apps_state();
  write_servers_state();
  write_allocations_state();
}

labels_t Master::label(const std::string &name) {
  static constexpr labels_t init{2};
  static std::mutex labels_mx;
  static int shift = 0;

  std::unique_lock lck(labels_mx);
  auto [found, inserted] = labels_.emplace(name, (init << shift));
  if (inserted) {
    SPDLOG_INFO("label: {} {}", name, (*found).second.to_string());
    ++shift;
  }

  return (*found).second;
}

traits_t Master::trait(const std::string &name) {
  static constexpr traits_t init{1};
  static std::mutex traits_mx;
  static int shift = 0;

  std::unique_lock lck(traits_mx);
  auto [found, inserted] = traits_.emplace(name, (init << shift));
  if (inserted) {
    SPDLOG_INFO("trait: {} {}", name, (*found).second.to_string());
    ++shift;
  }

  return (*found).second;
}

} // namespace scheduler
} // namespace treadmill
