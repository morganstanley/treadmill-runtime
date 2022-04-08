cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
option(BUILD_SHARED_LIBS "Build using shared libraries" ON)

project(treadmill-runtime)

add_definitions(-DSPDLOG_FMT_EXTERNAL)

enable_testing()

add_subdirectory(lib)
add_subdirectory(bin)
add_subdirectory(tests)
