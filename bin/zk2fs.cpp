#include <chrono>
#include <filesystem>
#include <regex>
#include <vector>

#include <cxxopts.hpp>

#include <treadmill/logging.h>
#include <treadmill/thread.h>
#include <treadmill/zk2fs.h>
#include <treadmill/zkclient.h>

using namespace treadmill;

cxxopts::ParseResult parse(int argc, char *argv[]) {
  try {
    cxxopts::Options options(argv[0],
                             "Syncronize Treadmill Zookeeper to filesystem.\n");
    options.positional_help("[optional args]").show_positional_help();

    // clang-format off
    options.allow_unrecognised_options().add_options()
      ("debug", "Enable debug level loggin.")
      (
        "z,zookeeper", "Zookeeper connection string. [required]",
        cxxopts::value<std::string>()
      )
      (
        "r,root", "Root directory on the filesystem. [required]",
        cxxopts::value<std::string>()
      )

      ("endpoints", "Sync application endpoints.")
      ("discovery", "Sync discovery.")
      ("discovery-state", "Sync discovery state.")
      ("scheduled", "Sync scheduled apps.")
      ("running", "Sync running apps.")
      ("placement", "Sync placement.")
      ("servers", "Sync servers.")
      ("blackedout-servers", "Sync blacked out servers.")
      ("once", "Run once (exit after model load).")
      ("identity-groups", "Sync identity groups.")
      ("app-groups", "Sync appgroups.")
      ("app-monitors", "Sync app monitors.")
      ("trace", "Sync trace.")
      ("cron-trace", "Sync cron trace.")
      ("trigger-trace", "Sync trigger trace.")
      ("server-trace", "Sync server trace.")
      ("data-records", "Sync data records.")


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

    if (!opts.count("r")) {
      std::cout << options.help({"", "Group"}) << std::endl;
      exit(0);
    }

    return opts;

  } catch (const cxxopts::OptionException &e) {
    std::cout << "error parsing options: " << e.what() << std::endl;
    exit(1);
  }
}

int treadmill_topology(const std::string &path) {

  // top level
  static const std::regex toplevel("^/[^/]+$");
  // sub level
  static const std::regex app_groups("^/app-groups/[^/]+$");
  static const std::regex app_monitors("^/app-monitors/[^/]+$");
  static const std::regex blackedout_servers("^/blackedout.servers/[^/]+$");
  static const std::regex cron_trace("^/cron-trace/[^/]+$");
  static const std::regex cron_trace_shard("^/cron-trace/[^/]+/[^/]+$");
  static const std::regex cron_trace_history("^/cron-trace.history/[^/]+$");
  static const std::regex data_records("^/data-records/[^/]+$");
  static const std::regex data_records_type("^/data-records/[^/]+/[^/]+$");
  static const std::regex discovery("^/discovery/[^/]+$");
  static const std::regex discovery_state("^/discovery.state/[^/]+$");
  static const std::regex endpoints("^/endpoints/[^/]+$");
  static const std::regex endpoints_proid("^/endpoints/[^/]+/[^/]+$");
  static const std::regex identity_groups("^/identity-groups/[^/]+$");
  static const std::regex identity_group_members(
      "^/identity-groups/[^/]+/[^/]+$");
  static const std::regex placement("^/placement/[^/]+$");
  static const std::regex placement_apps("^/placement/[^/]+/[^/]+$");
  static const std::regex running("^/running/[^/]+$");
  static const std::regex scheduled("^/scheduled/[^/]+$");
  static const std::regex servers("^/servers/[^/]+$");
  static const std::regex srv_trace("^/server-trace/[^/]+$");
  static const std::regex srv_trace_shard("^/server-trace/[^/]+/[^/]+$");
  static const std::regex srv_trace_history("^/server-trace.history/[^/]+$");
  static const std::regex trace("^/trace/[^/]+$");
  static const std::regex trace_shard("^/trace/[^/]+/[^/]+$");
  static const std::regex trace_history("^/trace.history/[^/]+$");
  static const std::regex trigger_trace("^/trigger-trace/[^/]+$");
  static const std::regex trigger_trace_shard("^/trigger-trace/[^/]+/[^/]+$");
  static const std::regex trigger_trace_history("^/trigger-trace.history/[^/]+$");

  std::smatch match;
  typedef Zk2Fs::SyncOpts so;

  // The order reflects the expected frequency of the calls. Trace is the most
  // frequent one. It can be some sort of fancy dynamic reordering list, as
  // there is a hit, the pattern will move on top, but seems like overkill for
  // now.
  if (false) {
  } else if (std::regex_match(path, match, trace_shard)) {
    return so::File | so::Empty;
  } else if (std::regex_match(path, match, scheduled)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, running)) {
    return so::File;
  } else if (std::regex_match(path, match, endpoints)) {
    return so::Dir | so::Watch;
  } else if (std::regex_match(path, match, endpoints_proid)) {
    return so::File;
  } else if (std::regex_match(path, match, discovery)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, discovery_state)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, servers)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, identity_groups)) {
    return so::File | so::Dir | so::Watch;
  } else if (std::regex_match(path, match, identity_group_members)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, app_groups)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, app_monitors)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, placement)) {
    return so::File | so::Dir | so::Watch;
  } else if (std::regex_match(path, match, placement_apps)) {
    return so::File;
  } else if (std::regex_match(path, match, trace)) {
    return so::Dir | so::Watch;
  } else if (std::regex_match(path, match, trace_history)) {
    return so::File | so::Compressed;
  } else if (std::regex_match(path, match, cron_trace)) {
    return so::Dir | so::Watch;
  } else if (std::regex_match(path, match, cron_trace_shard)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, cron_trace_history)) {
    return so::File | so::Compressed;
  } else if (std::regex_match(path, match, srv_trace)) {
    return so::Dir | so::Watch;
  } else if (std::regex_match(path, match, srv_trace_shard)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, srv_trace_history)) {
    return so::File | so::Compressed;
  } else if (std::regex_match(path, match, data_records)) {
    return so::Dir | so::Watch;
  } else if (std::regex_match(path, match, data_records_type)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, blackedout_servers)) {
    return so::File;
  } else if (std::regex_match(path, match, trigger_trace)) {
    return so::Dir | so::Watch;
  } else if (std::regex_match(path, match, trigger_trace_shard)) {
    return so::File | so::Watch;
  } else if (std::regex_match(path, match, trigger_trace_history)) {
    return so::File | so::Compressed;
  } else if (std::regex_match(path, match, toplevel)) {
    return so::Dir | so::Watch;
  } else {
    return so::Skip;
  }
}

int main(int argc, char **argv) {
  init_logging(argc, argv);

  auto opts = parse(argc, argv);

  auto connstr = opts["zookeeper"].as<std::string>();
  auto fsroot = opts["root"].as<std::string>();

  auto sync_endpoints = opts["endpoints"].as<bool>();
  auto sync_discovery = opts["discovery"].as<bool>();
  auto sync_discovery_state = opts["discovery-state"].as<bool>();
  auto sync_running = opts["running"].as<bool>();
  auto sync_scheduled = opts["scheduled"].as<bool>();
  auto sync_placement = opts["placement"].as<bool>();
  auto sync_servers = opts["servers"].as<bool>();
  auto sync_identity_groups = opts["identity-groups"].as<bool>();
  auto sync_appgroups = opts["app-groups"].as<bool>();
  auto sync_appmonitors = opts["app-monitors"].as<bool>();
  auto sync_trace = opts["trace"].as<bool>();
  auto sync_cron_trace = opts["cron-trace"].as<bool>();
  auto sync_trigger_trace = opts["trigger-trace"].as<bool>();
  auto sync_srv_trace = opts["server-trace"].as<bool>();
  auto sync_data_records = opts["data-records"].as<bool>();
  auto sync_blackouts = opts["blackedout-servers"].as<bool>();

  auto once = opts["once"].as<bool>();

  auto debug = opts["debug"].as<bool>();
  if (debug) {
    spdlog::set_level(spdlog::level::debug);
  } else {
    spdlog::set_level(spdlog::level::info);
  }

  SPDLOG_INFO("Connecting to: {}", connstr);

  std::vector<std::string> paths;
  if (sync_endpoints) {
    paths.push_back("/endpoints");
  }
  if (sync_discovery) {
    paths.push_back("/discovery");
  }
  if (sync_discovery_state) {
    paths.push_back("/discovery.state");
  }
  if (sync_scheduled) {
    paths.push_back("/scheduled");
  }
  if (sync_running) {
    paths.push_back("/running");
  }
  if (sync_servers) {
    paths.push_back("/servers");
  }
  if (sync_identity_groups) {
    paths.push_back("/identity-groups");
  }
  if (sync_appgroups) {
    paths.push_back("/app-groups");
  }
  if (sync_appmonitors) {
    paths.push_back("/app-monitors");
  }
  if (sync_placement) {
    paths.push_back("/placement");
  }
  if (sync_trace) {
    paths.push_back("/trace");
    paths.push_back("/trace.history");
  }
  if (sync_cron_trace) {
    paths.push_back("/cron-trace");
    paths.push_back("/cron-trace.history");
  }
  if (sync_trigger_trace) {
    paths.push_back("/trigger-trace");
    paths.push_back("/trigger-trace.history");
  }
  if (sync_srv_trace) {
    paths.push_back("/server-trace");
    paths.push_back("/server-trace.history");
  }
  if (sync_data_records) {
    paths.push_back("/data-records");
  }
  if (sync_blackouts) {
    paths.push_back("/blackedout.servers");
  }

  Zk2Fs zk2fs(fsroot, treadmill_topology);
  while (true) {
    SPDLOG_INFO("Starting zk2fs loop.");
    ZkClient zkclient;

    Event disconnected_ev;
    auto zk_connect_watch = [&disconnected_ev, &zkclient, &once, &zk2fs,
                             &paths](int type, int state) {
      SPDLOG_INFO("Connection watch - type: {}, state: {}", type, state);
      if (state == ZOO_CONNECTED_STATE) {
        zk2fs.use(&zkclient);
        zk2fs.synchronize(paths, once);
      } else if (state == ZOO_EXPIRED_SESSION_STATE ||
                 state == ZOO_AUTH_FAILED_STATE ||
                 state == ZOO_NOTCONNECTED_STATE) {
        disconnected_ev.notify();
      } else {
        SPDLOG_DEBUG("Ignore session event: {} {}", type, state);
      }
    };

    zkclient.connect(connstr, 600, zk_connect_watch);

    disconnected_ev.wait();
    SPDLOG_INFO("Disconnected.");
    zkclient.disconnect();
  }

  SPDLOG_INFO("Finished.");
  return 0;
}
