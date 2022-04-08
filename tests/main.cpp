#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>

#include <treadmill/logging.h>

int main(int argc, char *argv[]) {
  ::treadmill::init_logging(argc, argv);

  if (getenv("TREADMILL_TEST_DISABLE_LOGGING")) {
    spdlog::default_logger()->set_level(spdlog::level::off);
  }

  int result = Catch::Session().run(argc, argv);
  return result;
}
