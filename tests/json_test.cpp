#include <catch2/catch.hpp>

#include <treadmill/json.h>

#include <rapidjson/writer.h>

using namespace treadmill;

namespace {

TEST_CASE("Json serialization test.", "[json]") {
  namespace js = treadmill::js;

  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);

  SECTION("empty") {
    { js::Object obj(&writer); }
    REQUIRE(std::string(sb.GetString()) == "{}");
  }
  SECTION("simple") {
    {
      js::Object obj(&writer);
      obj("1", 1)("2", "2");
    }
    REQUIRE(std::string(sb.GetString()) == "{\"1\":1,\"2\":\"2\"}");
  }
  SECTION("array") {
    {
      js::Array arr(&writer);
      arr << 1 << "1";
    }
    REQUIRE(std::string(sb.GetString()) == "[1,\"1\"]");
  }
  SECTION("complex") {
    {
      js::Object obj(&writer);
      {
        auto o1 = obj.obj("a");
        o1("1", 1);
      }
    }
    REQUIRE(std::string(sb.GetString()) == "{\"a\":{\"1\":1}}");
  }
  SECTION("null") {
    auto x = std::make_optional<int>(5);
    {
      js::Object obj(&writer);
      obj("1", x);
      x = std::nullopt;
      obj("2", x);
      obj("3", std::nullopt);
    }
    REQUIRE(std::string(sb.GetString()) == "{\"1\":5,\"2\":null,\"3\":null}");
  }
  SECTION("array-iterator") {
    {
      js::Object obj(&writer);
      std::vector<int> v{1, 2, 3};
      obj("a", v.begin(), v.end());
    }
    REQUIRE(std::string(sb.GetString()) == "{\"a\":[1,2,3]}");
  }
}

} // namespace
