#include <treadmill/event_publisher.h>
#include <treadmill/logging.h>
#include <treadmill/master.h>
#include <treadmill/scheduler.h>
#include <treadmill/zkclient.h>
#include <treadmill/zklock.h>
#include <treadmill/zknamespace.h>

#include <chrono>
#include <filesystem>
#include <regex>
#include <vector>

#include <cxxopts.hpp>
#include <death_handler.h>
#include <spdlog/sinks/rotating_file_sink.h>

using namespace treadmill;
using namespace treadmill::scheduler;

static constexpr std::size_t default_log_size = 16 * 1024 * 1024;
static constexpr std::size_t default_log_files = 10;

cxxopts::ParseResult parse(int argc, char *argv[]) {
  try {
    cxxopts::Options options(argv[0],
                             "Syncronize Treadmill Zookeeper to filesystem.\n");
    options.positional_help("[optional args]").show_positional_help();

    // clang-format off
    options.allow_unrecognised_options().add_options()
      ("debug", "Enable debug level loggin.")
      (
        "z,zookeeper", "Zookeeper connection string.",
        cxxopts::value<std::string>()
      )
      (
        "l,logs", "Scheduler logs directory.",
        cxxopts::value<std::string>()
      )
      (
        "log-size", "Max log size.",
        cxxopts::value<std::size_t>()->default_value(
          std::to_string(default_log_size)
        )
      )
      (
        "log-files", "Max log files.",
        cxxopts::value<std::size_t>()->default_value(
          std::to_string(default_log_files)
        )
      )
      ("once", "Run once (exit after model load).")
      (
        "cell", "Cell name.",
        cxxopts::value<std::string>()
      )
      (
        "state", "Scheduler state directory.",
        cxxopts::value<std::string>()
      )
      (
        "app-events", "App events spool directory.",
        cxxopts::value<std::string>()
      )
      (
        "srv-events", "Server events spool directory.",
        cxxopts::value<std::string>()
      )
      (
        "eventpub", "Publish trace events to Zookeeper.",
        cxxopts::value<bool>()->default_value("true")
      )
      ("readonly", "Run readonly (and without a lock).""")
    ;
    // clang-format on

    auto opts = options.parse(argc, argv);

    if (opts.count("help")) {
      std::cout << options.help({"", "Group"}) << std::endl;
      exit(0);
    }

    if (!opts.count("z")) {
      std::cout << options.help({"", "Group"}) << std::endl;
      exit(0);
    }

    if (!opts.count("l")) {
      std::cout << options.help({"", "Group"}) << std::endl;
      exit(0);
    }

    if (!opts.count("cell")) {
      std::cout << options.help({"", "Group"}) << std::endl;
      exit(0);
    }

    if (!opts.count("app-events")) {
      std::cout << options.help({"", "Group"}) << std::endl;
      exit(0);
    }

    if (!opts.count("srv-events")) {
      std::cout << options.help({"", "Group"}) << std::endl;
      exit(0);
    }

    return opts;

  } catch (const cxxopts::OptionException &e) {
    std::cout << "error parsing options: " << e.what() << std::endl;
    exit(1);
  }
}

void process_trace(uv_loop_t *loop, const std::string &appev_spool_dir,
                   const std::string &srvev_spool_dir, ZkClient *zkclient) {

  DirWatch appev_watch(loop, appev_spool_dir);
  DirWatch srvev_watch(loop, srvev_spool_dir);
  EventPublisher appev_pub(appev_spool_dir, zkclient);

  if (appev_spool_dir != "-") {
    appev_watch.on_created = [&appev_pub](const auto &name, const auto &data) {
      appev_pub.on_event(name, data);
    };
    appev_watch.start();
  }

  if (srvev_spool_dir != "-") {
    srvev_watch.on_created = [](const auto &name, const auto &data) {
      SPDLOG_INFO("[NOT IMPLEMENTED] srv-event created: {}", name);
    };
    srvev_watch.start();
  }

  // Starting uvloop will drain all existing events.
  uv_run(loop, UV_RUN_DEFAULT);
}

void run() {}

int main(int argc, char **argv) {

  // TODO: need ot experiment more with the deatch handler, it seems to print
  //       stack trace but does not display the symbols...
  // Debug::DeathHandler dh;

  init_logging(argc, argv);

  namespace fs = std::filesystem;

  auto opts = parse(argc, argv);

  auto connstr = opts["zookeeper"].as<std::string>();
  auto once = opts["once"].as<bool>();
  auto readonly = opts["readonly"].as<bool>();
  auto logs = fs::path(opts["logs"].as<std::string>());
  auto log_size = opts["log-size"].as<std::size_t>();
  auto log_files = opts["log-files"].as<std::size_t>();
  auto cell = opts["cell"].as<std::string>();
  auto state_dir = fs::path(opts["state"].as<std::string>());
  auto appev_spool_dir = fs::path(opts["app-events"].as<std::string>());
  auto srvev_spool_dir = fs::path(opts["srv-events"].as<std::string>());
  auto eventpub = opts["eventpub"].as<bool>();
  auto debug = opts["debug"].as<bool>();
  if (debug) {
    spdlog::set_level(spdlog::level::debug);
  } else {
    spdlog::set_level(spdlog::level::info);
  }

  SPDLOG_INFO("Connecting to: {}", connstr);

  auto make_logger = [&logs](const std::string &name, std::size_t log_size,
                             std::size_t log_files) {
    if (auto logger = spdlog::get(name)) {
      return logger;
    }

    auto l(logs);
    l /= fmt::format("{}.log", name);
    std::cout << " log: " << l.string() << "\n";
    return spdlog::rotating_logger_mt(fmt::format("scheduler.{}", name),
                                      l.string(), log_size, log_files);
  };

  Application::logger = make_logger("apps", log_size, log_files);
  Node::logger = make_logger("nodes", log_size, log_files);
  Event::logger = make_logger("schedule", log_size, log_files);

  // TODO: initialize topology from command line. Alternatively, store
  //       topology information in zookeeper and read it first, before creating
  //       the cell.
  Topology top{4,
               {{"server", 4},
                {"rack", 3},
                {"bunker", 2},
                {"building", 1},
                {"cell", 0}}};

  Master master(top, cell, state_dir, appev_spool_dir, srvev_spool_dir,
                readonly);

  ZkClient zkclient;

  master.use(&zkclient);

  ZkLock::LockClbk on_lock = [&master] {
    SPDLOG_INFO("Locked");
    master.start();
  };

  ZkLock lock(&zkclient, zknamespace::constants::scheduler_lock, &on_lock);

  SPDLOG_INFO("Connecting to zookeeper: {}", connstr);
  zkclient.connect(connstr, 600,
                   [&lock, &zkclient, &master, &readonly](int type, int state) {
                     if (state == ZOO_CONNECTED_STATE) {
                       SPDLOG_DEBUG("Connected.");
                       zknamespace::set_permissions(zkclient);
                       if (!readonly) {
                         zkclient.create(zknamespace::constants::scheduler_lock,
                                         "", 0, 0, nullptr);
                         lock.lock();
                       } else {
                         SPDLOG_INFO("Starting without lock (readonly).");
                         master.start();
                       }
                     } else if (state == ZOO_EXPIRED_SESSION_STATE ||
                                state == ZOO_AUTH_FAILED_STATE ||
                                state == ZOO_NOTCONNECTED_STATE) {
                       SPDLOG_INFO("Disconnected.");
                       std::exit(0);
                     } else {
                       SPDLOG_INFO("Disconnected: state = {}", state);
                       std::exit(0);
                     }
                   });

  master.init();
  if (!once) {
    master.run();

    std::optional<std::thread> trace_thr;
    uv_loop_t loop;
    uv_loop_init(&loop);

    if (eventpub) {
      SPDLOG_INFO("Starting thread publishing trace events to Zookeeper.");
      trace_thr = std::make_optional<std::thread>(
          process_trace, &loop, appev_spool_dir, srvev_spool_dir, &zkclient);
    }

    if (trace_thr) {
      SPDLOG_INFO("stopping uvloop.");
      uv_stop(&loop);
      trace_thr.value().join();
    }

    uv_loop_close(&loop);
  }

  SPDLOG_INFO("Finished.");
  return 0;
}
