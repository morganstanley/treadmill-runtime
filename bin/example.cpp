#include <atomic>
#include <bitset>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <valarray>
#include <variant>

#include <stdlib.h>
#include <sys/time.h>

#include <bitmask/bitmask.hpp>

#include <rapidjson/filewritestream.h>
#include <rapidjson/writer.h>

#include <fmt/format.h>
#include <fmt/printf.h>
#include <treadmill/convert.h>
#include <treadmill/fs.h>
#include <treadmill/logging.h>
#include <treadmill/scheduler.h>
#include <treadmill/zk2fs.h>
#include <treadmill/zkclient.h>
#include <treadmill/zknamespace.h>

#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#include <treadmill/diff.h>
#include <treadmill/json.h>
#include <treadmill/thread.h>
#include <treadmill/yaml.h>

#include <zlib.h>

using namespace treadmill;
using namespace treadmill::scheduler;

static bool get_children_clbk(int rc, const ZkClient::Path &path,
                              const ZkClient::List &nodes) {

  for (const auto &node : nodes.value()) {
    SPDLOG_INFO("node: {}", node);
  }
  return true;
}

int main(int argc, char **argv) {
  init_logging(argc, argv);
  const char *connstr =
      "zookeeper+sasl://"
      "ivapp1174917.devin3.ms.com:4000,ivapp1174917.devin3.ms.com:4001,"
      "ivapp1174917.devin3.ms.com:4002/treadmill/andreikdev#service=treadmld";
  ZkClient zkclient;
  ZkClient::ChildrenWatch clbk = get_children_clbk;

  treadmill::Event disconnected_ev;
  auto zk_connect_watch = [&zkclient, &disconnected_ev, &clbk](int type,
                                                               int state) {
    SPDLOG_INFO("Connection watch - type: {}, state: {}", type, state);
    if (state == ZOO_CONNECTED_STATE) {
      SPDLOG_INFO("Connected.");
      zkclient.watch_children("/servers", &clbk, true);
      ZkClient::ExistsWatch c = [&zkclient, &clbk](int, const ZkClient::Path &,
                                                   const ZkClient::Stat &) {
        SPDLOG_INFO("continue ............");
        zkclient.watch_children("/scheduled", &clbk, true);
        return false;
      };
      zkclient.watch_exists("/", &c, false);
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
}
