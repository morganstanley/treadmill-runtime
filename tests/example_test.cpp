#include <catch2/catch.hpp>

namespace {

TEST_CASE("example test", "[example]") {

  bool ok = true;
  REQUIRE(ok == true);
  SECTION("some test") { REQUIRE(ok == true); }
}

} // namespace
