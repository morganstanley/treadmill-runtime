#include <catch2/catch.hpp>

#include <treadmill/fs.h>

#include <fmt/format.h>

#include <string.h>
#include <sys/stat.h>

using namespace treadmill;

namespace {

TEST_CASE("Atomic write test.", "[fs]") {
  std::string tempdir;

  char tmp[256]{"/tmp/fstestXXXXXX"};
  tempdir = ::mkdtemp(tmp);

  SECTION("create_small") {
    std::string fname = fmt::format("{}/a.txt", tempdir);
    atomic_write(fname, std::nullopt, 1548054846, "test", strlen("test"));

    struct stat s {};
    stat(fname.c_str(), &s);
    REQUIRE(s.st_mtime == 1548054846);
    REQUIRE(s.st_atime == 1548054846);
    REQUIRE(s.st_size == 4);
  }

  SECTION("create_large") {
    std::string fname = fmt::format("{}/a.txt", tempdir);
    struct stat s {};

    decltype(s.st_size) size = 32 * 1024 * 1024;
    char *data = (char *)malloc(size);
    REQUIRE(data != nullptr);
    atomic_write(fname, std::nullopt, 1548054846, data, size);
    free(data);

    stat(fname.c_str(), &s);
    REQUIRE(s.st_mtime == 1548054846);
    REQUIRE(s.st_atime == 1548054846);
    REQUIRE(s.st_size == size);
  }

  SECTION("create_empty") {
    std::string fname = fmt::format("{}/a.txt", tempdir);
    atomic_write(fname, std::nullopt, 1548054846, nullptr, 0);

    struct stat s {};
    stat(fname.c_str(), &s);
    REQUIRE(s.st_mtime == 1548054846);
    REQUIRE(s.st_atime == 1548054846);
    REQUIRE(s.st_size == 0);
  }

  int rc = ::system(fmt::format("rm -rf {}", tempdir).c_str());
  REQUIRE(rc == 0);
}

} // namespace
