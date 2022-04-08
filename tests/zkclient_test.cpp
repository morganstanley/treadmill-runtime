#include <catch2/catch.hpp>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string_view>
#include <vector>

#include "mock_zookeeper.h"
#include <treadmill/zkclient.h>

using namespace treadmill;
using namespace treadmill::test;
using namespace std::chrono_literals;

TEST_CASE("Zookeeper client test", "[zk]") {

  SECTION("connect_test") {
    ZkClient zkclient;
    zkclient.connect("zookeeper://server:1234", 60);
    zhandle_t *zh = ZMock::zh(zkclient);
    ZMock::mock(zh)->join();
  }

  SECTION("children_test") {
    ZkClient zkclient;
    zkclient.connect("zookeeper://someserver:1234", 60);
    zhandle_t *zh = ZMock::zh(zkclient);

    std::promise<std::set<std::string>> promise;
    auto clbk =
        ZkClient::ChildrenWatch([&promise](int rc, const ZkClient::Path &path,
                                           const ZkClient::List &children) {
          std::set<std::string> result;
          for (const auto &s : children.value()) {
            result.insert(s);
          }
          promise.set_value(result);
          return false;
        });

    zoo_create(zh, "/scheduled/c1", "", 0, nullptr, 0, nullptr, 0);
    zoo_create(zh, "/scheduled/c2", "", 0, nullptr, 0, nullptr, 0);
    zkclient.watch_children("/scheduled", &clbk, false);

    std::future<std::set<std::string>> future = promise.get_future();
    future.wait();
    std::set<std::string> result = future.get();

    REQUIRE(result.size() == 2UL);
    REQUIRE(result.find("c1") != result.end());
    REQUIRE(result.find("c2") != result.end());
  }

  SECTION("children_watch_test") {
    ZkClient zkclient;
    zkclient.connect("zookeeper://someserver:1234", 60);
    zhandle_t *zh = ZMock::zh(zkclient);

    std::promise<std::set<std::string>> promise1;
    std::promise<std::set<std::string>> *promise_ptr = &promise1;

    auto clbk = ZkClient::ChildrenWatch(
        [&promise_ptr](int rc, const ZkClient::Path &path,
                       const ZkClient::List &children) {
          std::set<std::string> result;
          for (const auto &s : children.value()) {
            result.insert(s);
          }
          promise_ptr->set_value(result);
          return true;
        });

    auto get_result = [](std::promise<std::set<std::string>> &promise) {
      auto future = promise.get_future();
      future.wait();
      return future.get();
    };

    zoo_create(zh, "/scheduled/c1", "", 0, nullptr, 0, nullptr, 0);
    zoo_create(zh, "/scheduled/c2", "", 0, nullptr, 0, nullptr, 0);
    zkclient.watch_children("/scheduled", &clbk, true);

    auto result = get_result(promise1);
    ZMock::mock(zh)->join();

    REQUIRE(result.size() == 2UL);
    REQUIRE(result.find("c1") != result.end());
    REQUIRE(result.find("c2") != result.end());

    std::promise<std::set<std::string>> promise2;
    promise_ptr = &promise2;

    zoo_create(zh, "/scheduled/c3", "", 0, nullptr, 0, nullptr, 0);

    ZMock::mock(zh)->fire_children_ev("/scheduled");

    result = get_result(promise2);
    ZMock::mock(zh)->join();

    REQUIRE(result.size() == 3UL);
    REQUIRE(result.find("c1") != result.end());
    REQUIRE(result.find("c2") != result.end());
    REQUIRE(result.find("c3") != result.end());
  }

  SECTION("data_test") {
    ZkClient zkclient;
    zkclient.connect("zookeeper://server:1234", 60);
    zhandle_t *zh = ZMock::zh(zkclient);

    std::promise<std::string> promise1;
    std::promise<std::string> *promise_ptr = &promise1;

    auto clbk = ZkClient::DataWatch(
        [&promise_ptr](int rc, const ZkClient::Path &path,
                       const ZkClient::Data &data, const ZkClient::Stat &stat) {
          if (data) {
            promise_ptr->set_value(data.value().data());
          } else {
            promise_ptr->set_value("__fail__");
          }
          return true;
        });

    auto get_result = [](std::promise<std::string> &promise) {
      auto future = promise.get_future();
      future.wait();
      return future.get();
    };

    zoo_create(zh, "/scheduled", "aaa", 3, nullptr, 0, nullptr, 0);
    zkclient.watch_data("/scheduled", &clbk, false);

    auto result = get_result(promise1);
    ZMock::mock(zh)->join();
    REQUIRE(result == "aaa");
  }

  SECTION("data_watch_test") {
    ZkClient zkclient;
    zkclient.connect("zookeeper://server:1234", 60);
    zhandle_t *zh = ZMock::zh(zkclient);

    std::promise<std::string> promise1;
    std::promise<std::string> *promise_ptr = &promise1;

    auto clbk = ZkClient::DataWatch(
        [&promise_ptr](int rc, const ZkClient::Path &path,
                       const ZkClient::Data &data, const ZkClient::Stat &stat) {
          if (data) {
            promise_ptr->set_value(data.value().data());
          } else {
            promise_ptr->set_value("__fail__");
          }
          return true;
        });

    auto get_result = [](std::promise<std::string> &promise) {
      auto future = promise.get_future();
      future.wait();
      return future.get();
    };

    zoo_create(zh, "/scheduled", "aaa", 3, nullptr, 0, nullptr, 0);
    zkclient.watch_data("/scheduled", &clbk, true);

    auto result = get_result(promise1);
    ZMock::mock(zh)->join();
    REQUIRE(result == "aaa");

    std::promise<std::string> promise2;
    promise_ptr = &promise2;

    zoo_set(zh, "/scheduled", "bbb", 3, 0);
    ZMock::mock(zh)->fire_data_ev("/scheduled");

    result = get_result(promise2);
    ZMock::mock(zh)->join();
    REQUIRE(result == "bbb");
  }

  SECTION("parse_connstr_test") {
    std::string scheme;
    std::string hosts;
    std::string chroot;
    std::unordered_map<std::string, std::string> params;

    params.erase(params.begin(), params.end());
    treadmill::parse_zk_connstr(
        "zookeeper+sasl://aaa:123,server.foo.com:1234/d/f", scheme, hosts,
        chroot, params);
    REQUIRE(scheme == "zookeeper+sasl");
    REQUIRE(hosts == "aaa:123,server.foo.com:1234");
    REQUIRE(chroot == "/d/f");

    params.erase(params.begin(), params.end());
    treadmill::parse_zk_connstr(
        "zookeeper+sasl://server-123.foo.com:1234/d/f#service=foo", scheme,
        hosts, chroot, params);
    REQUIRE(scheme == "zookeeper+sasl");
    REQUIRE(hosts == "server-123.foo.com:1234");
    REQUIRE(chroot == "/d/f");
    REQUIRE(params.find("service")->second == "foo");

    params.erase(params.begin(), params.end());
    treadmill::parse_zk_connstr(
        "zookeeper://aaa:123,server.foo.com:1234/d/f#a=1,b=2", scheme, hosts,
        chroot, params);
    REQUIRE(scheme == "zookeeper");
    REQUIRE(hosts == "aaa:123,server.foo.com:1234");
    REQUIRE(chroot == "/d/f");
    REQUIRE(params.find("a")->second == "1");
    REQUIRE(params.find("b")->second == "2");

    params.erase(params.begin(), params.end());
    treadmill::parse_zk_connstr(
        "zookeeper+sasl://aaa:123,server.foo.com:1234/d/f", scheme, hosts,
        chroot, params);
    REQUIRE(scheme == "zookeeper+sasl");
    REQUIRE(hosts == "aaa:123,server.foo.com:1234");
    REQUIRE(chroot == "/d/f");

    params.erase(params.begin(), params.end());
    treadmill::parse_zk_connstr("zookeeper+sasl://aaa:123,server.foo.com:1234",
                                scheme, hosts, chroot, params);
    REQUIRE(scheme == "zookeeper+sasl");
    REQUIRE(hosts == "aaa:123,server.foo.com:1234");
    REQUIRE(chroot == "");
  }

  SECTION("check_connstr_test") {
    REQUIRE(treadmill::check_zk_connstr("zookeeper://aaa:123"));
    REQUIRE(treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345"));
    REQUIRE(treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345/"));
    REQUIRE(treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345/a/b"));
    REQUIRE(treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345#a=1"));
    REQUIRE(!treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345#"));
    REQUIRE(!treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345#a="));
    REQUIRE(!treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345#a"));
    REQUIRE(treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345/a#a=1"));
    REQUIRE(!treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345/a#"));
    REQUIRE(!treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345/a#a="));
    REQUIRE(!treadmill::check_zk_connstr("zookeeper://aaa:123,bbb:345/a#a"));

    REQUIRE(treadmill::check_zk_connstr(
        "zookeeper+sasl://aaa:123,server.foo.com:1234/d/f"));

    REQUIRE(!treadmill::check_zk_connstr("foo:123"));
  }

  SECTION("child_of_test") {
    auto child_of_servers = child_of("/servers");
    REQUIRE(child_of_servers("/servers"));
    REQUIRE(child_of_servers("/servers/a"));
    REQUIRE(!child_of_servers("/serversa"));
    REQUIRE(!child_of_servers("/server"));

    auto child_of_root = child_of("");
    REQUIRE(child_of_root("/servers"));
    REQUIRE(child_of_root("/"));
  }
}
