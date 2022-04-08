#include <catch2/catch.hpp>

#include <treadmill/master.h>

namespace {

using namespace treadmill;
using namespace treadmill::scheduler;

struct Entity {
  Entity(const std::string &name) : name_(name) {}
  ~Entity() {}

  std::string name_;
};

TEST_CASE("Reconcile test.", "[master]") {

  Application::logger = spdlog::default_logger();
  Node::logger = spdlog::default_logger();
  Event::logger = spdlog::default_logger();

  SECTION("create_delete") {
    Collection<Entity> entities;
    std::vector<std::string> v{"1", "2"};
    entities.reconcile(v.begin(), v.end(), [&entities](auto &name) {
      entities.add(name, std::make_shared<Entity>(name));
    });

    REQUIRE(entities.size() == 2);

    v.clear();
    entities.reconcile(v.begin(), v.end(), [&entities](auto &name) {
      entities.add(name, std::make_shared<Entity>(name));
    });
    REQUIRE(entities.size() == 0);
  }

  SECTION("add") {
    Collection<Entity> entities;
    std::vector<std::string> v{"1", "3"};
    entities.reconcile(v.begin(), v.end(), [&entities](auto &name) {
      entities.add(name, std::make_shared<Entity>(name));
    });
    REQUIRE(entities.exists("1"));
    REQUIRE(entities.exists("3"));

    REQUIRE(entities.size() == 2);

    v = {"3", "4", "5"};
    entities.reconcile(v.begin(), v.end(), [&entities](auto &name) {
      entities.add(name, std::make_shared<Entity>(name));
    });
    REQUIRE(entities.exists("3"));
    REQUIRE(entities.exists("4"));
    REQUIRE(entities.exists("5"));
    REQUIRE(!entities.exists("1"));
  }

  SECTION("labels") {
    Topology top{3,
                 {{"server", 4},
                  {"rack", 3},
                  {"bunker", 2},
                  {"building", 1},
                  {"cell", 0}}};

    Master m(top, "cell", "", "", "", true);
    REQUIRE(m.label("a") == labels_t{0x2});
    REQUIRE(m.label("a") == labels_t{0x2});
    REQUIRE(m.label("b") == labels_t{0x4});
    REQUIRE(m.label("a") == labels_t{0x2});
    REQUIRE(m.label("_default") == labels_t{0x1});
  }
}

} // namespace
