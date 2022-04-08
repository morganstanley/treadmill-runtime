#include <cxxopts.hpp>
#include <treadmill/logging.h>

#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

namespace treadmill {

cxxopts::ParseResult parse(int argc, char *argv[]) {
  try {
    cxxopts::Options options(argv[0], "Initialize logging options.\n");
    options.positional_help("[optional args]").show_positional_help();

    // clang-format off
    options.allow_unrecognised_options().add_options()
      (
        "log", "Path to log file.",
        cxxopts::value<std::string>()
      )
    ;
    // clang-format on

    auto opts = options.parse(argc, argv);
    return opts;

  } catch (const cxxopts::OptionException &e) {
    std::cout << "error parsing options: " << e.what() << std::endl;
    exit(1);
  }
}

void init_logging(int argc, char **argv) {}

void print_stacktrace() { raise(SIGSEGV); }

} // namespace treadmill
