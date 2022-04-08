#include <treadmill/logging.h>
#include <treadmill/thread.h>
#include <treadmill/zklock.h>

#include <cxxopts.hpp>

using namespace treadmill;

cxxopts::ParseResult parse(int argc, char *argv[]) {
  try {
    cxxopts::Options options(argv[0], "Run program under zookeeper lock.\n");
    options.positional_help("[optional args]").show_positional_help();

    // clang-format off
    options.allow_unrecognised_options().add_options()
      ("debug", "Enable debug level loggin.")
      (
        "z,zookeeper", "Zookeeper connection string.",
        cxxopts::value<std::string>()
      )
      (
        "l,lock", "Zookeeper lock path.",
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

    if (!opts.count("l")) {
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
  auto locknode = opts["lock"].as<std::string>();

  SPDLOG_INFO("Running zklock: {} {}", connstr, locknode);

  ZkLock::LockClbk on_lock = [] { SPDLOG_INFO("Locked"); };

  ZkClient zkclient;
  ZkLock lock(&zkclient, locknode, &on_lock);

  zkclient.connect(connstr, 600, [&lock](int type, int state) {
    if (state == ZOO_CONNECTED_STATE) {
      SPDLOG_DEBUG("connection callback: {} {}", type, state);
      lock.lock();
    } else if (state == ZOO_EXPIRED_SESSION_STATE ||
               state == ZOO_AUTH_FAILED_STATE ||
               state == ZOO_NOTCONNECTED_STATE) {
      SPDLOG_INFO("Disconnected.");
      std::exit(0);
    } else {
    }
  });

  sleep_forever();
}
