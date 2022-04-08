#include <catch2/catch.hpp>

#include <treadmill/scopeguard.h>

namespace {

TEST_CASE("Scope guard test.", "[scopeguard]") {
  int value = 1;

  SECTION("lambda") {
    auto guard = treadmill::make_scopeguard([&value](void *) { value = 0; });
  }

  REQUIRE(value == 0);
}

} // namespace
