#include <treadmill/dirwatch.h>
#include <treadmill/event_publisher.h>
#include <treadmill/logging.h>
#include <treadmill/thread.h>
#include <treadmill/zkclient.h>

#include <limits.h>
#include <stdio.h>
#include <unistd.h>
#include <uv.h>

#include <deque>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include <cxxopts.hpp>

using namespace treadmill;

namespace fs = std::filesystem;

cxxopts::ParseResult parse(int argc, char *argv[]) {
  try {
    cxxopts::Options options(argv[0], "Publish trace events to Zookeeper.\n");
    options.positional_help("[optional args]").show_positional_help();

    // clang-format off
    options.allow_unrecognised_options().add_options()
      (
        "z,zookeeper", "Zookeeper connection string.",
        cxxopts::value<std::string>()
      )
      (
        "a,app-events", "App events spool directory.",
        cxxopts::value<std::string>()
      )
      (
        "s,srv-events", "Server events spool directory.",
        cxxopts::value<std::string>()
      )
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

    if (!opts.count("a")) {
      std::cout << options.help({"", "Group"}) << std::endl;
      exit(0);
    }

    if (!opts.count("s")) {
      std::cout << options.help({"", "Group"}) << std::endl;
      exit(0);
    }

    return opts;

  } catch (const cxxopts::OptionException &e) {
    std::cout << "error parsing options: " << e.what() << std::endl;
    exit(1);
  }
}

int main(int argc, char **argv) {

  init_logging(argc, argv);

  auto opts = parse(argc, argv);

  auto connstr = opts["zookeeper"].as<std::string>();
  auto appev_spool_dir = fs::path(opts["app-events"].as<std::string>());
  auto srvev_spool_dir = fs::path(opts["srv-events"].as<std::string>());

  ZkClient zkclient;

  uv_loop_t loop;
  uv_loop_init(&loop);

  DirWatch appev_watch(&loop, appev_spool_dir);
  EventPublisher appev_pub(appev_spool_dir, &zkclient);

  DirWatch srvev_watch(&loop, srvev_spool_dir);

  zkclient.connect(
      connstr, 600,
      [&appev_watch, &appev_pub, &srvev_watch, &loop, &appev_spool_dir,
       &srvev_spool_dir](int type, int state) {
        if (state == ZOO_CONNECTED_STATE) {
          if (appev_spool_dir != "-") {
            appev_watch.on_created = [&appev_pub](const auto &name,
                                                  const auto &data) {
              appev_pub.on_event(name, data);
            };
            appev_watch.start();
          }
          if (srvev_spool_dir != "-") {
            srvev_watch.on_created = [](auto &name, auto &data) {
              SPDLOG_INFO("[NOT IMPLEMENTED] srv-event created: {}", name);
            };
            srvev_watch.on_deleted = [](auto &name) {
              SPDLOG_INFO("[NOT IMPLEMENTED] srv-event deleted: {}", name);
            };
            srvev_watch.start();
          }
        } else if (state == ZOO_EXPIRED_SESSION_STATE ||
                   state == ZOO_AUTH_FAILED_STATE ||
                   state == ZOO_NOTCONNECTED_STATE) {
          SPDLOG_INFO("stopping uvloop.");
          uv_stop(&loop);
          uv_loop_close(&loop);
          std::exit(0);
        } else {
        }
      });

  uv_run(&loop, UV_RUN_DEFAULT);
}
