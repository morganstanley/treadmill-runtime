#include <catch2/catch.hpp>
#include <set>
#include <vector>

#include <treadmill/diff.h>

namespace treadmill {
namespace scheduler {
namespace test {

TEST_CASE("diff_for_each_test.", "[tools]") {

  SECTION("different") {
    std::set<int> l{1, 2};
    std::set<int> r{1, 3};
    std::vector<int> l_only;
    std::vector<int> r_only;

    for_each_diff(
        l.begin(), l.end(), r.begin(), r.end(),
        [&l_only](auto &s) { l_only.push_back(s); },
        [&r_only](auto &s) { r_only.push_back(s); });
    REQUIRE(l_only == std::vector<int>{2});
    REQUIRE(r_only == std::vector<int>{3});
  }

  SECTION("left_empty") {
    std::set<int> l{};
    std::set<int> r{1, 3};
    std::vector<int> l_only;
    std::vector<int> r_only;

    for_each_diff(
        l.begin(), l.end(), r.begin(), r.end(),
        [&l_only](auto &s) { l_only.push_back(s); },
        [&r_only](auto &s) { r_only.push_back(s); });
    REQUIRE(l_only == std::vector<int>{});
    REQUIRE(r_only == std::vector<int>{1, 3});
  }

  SECTION("right_empty") {
    std::set<int> l{1, 2};
    std::set<int> r{};
    std::vector<int> l_only;
    std::vector<int> r_only;

    for_each_diff(
        l.begin(), l.end(), r.begin(), r.end(),
        [&l_only](auto &s) { l_only.push_back(s); },
        [&r_only](auto &s) { r_only.push_back(s); });
    REQUIRE(l_only == std::vector<int>{1, 2});
    REQUIRE(r_only == std::vector<int>{});
  }

  SECTION("equal") {
    std::set<int> l{1, 2};
    std::set<int> r{1, 2};
    std::vector<int> l_only;
    std::vector<int> r_only;

    for_each_diff(
        l.begin(), l.end(), r.begin(), r.end(),
        [&l_only](auto &s) { l_only.push_back(s); },
        [&r_only](auto &s) { r_only.push_back(s); });
    REQUIRE(l_only == std::vector<int>{});
    REQUIRE(r_only == std::vector<int>{});
  }

  SECTION("both_empty") {
    std::set<int> l{};
    std::set<int> r{};
    std::vector<int> l_only;
    std::vector<int> r_only;

    for_each_diff(
        l.begin(), l.end(), r.begin(), r.end(),
        [&l_only](auto &s) { l_only.push_back(s); },
        [&r_only](auto &s) { r_only.push_back(s); });
    REQUIRE(l_only == std::vector<int>{});
    REQUIRE(r_only == std::vector<int>{});
  }
}

TEST_CASE("apply_merge_test", "[tools]") {

  SECTION("different") {
    std::set<int> l{2, 3};
    std::set<int> r{2, 5};
    std::vector<int> m;

    for_each_diff(
        l.begin(), l.end(), r.begin(), r.end(),
        [&m](auto &s) { m.push_back(s); }, [&m](auto &s) { m.push_back(s); },
        [&m](auto &left, auto &right) { m.push_back(left + right); },
        std::less<int>());

    REQUIRE(m == std::vector<int>{4, 3, 5});
  }

  SECTION("different") {
    std::set<int> l{2, 4};
    std::set<int> r{2, 4};
    std::vector<int> m;

    for_each_diff(
        l.begin(), l.end(), r.begin(), r.end(),
        [&m](auto &s) { m.push_back(s); }, [&m](auto &s) { m.push_back(s); },
        [&m](auto &left, auto &right) { m.push_back(left + right); },
        std::less<int>());

    REQUIRE(m == std::vector<int>{4, 8});
  }
}

} // namespace test
} // namespace scheduler
} // namespace treadmill
