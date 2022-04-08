cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
add_definitions(-DHAVE_CYRUS_SASL_H -DTHREADED)

add_library(
    treadmill-runtime
    dirwatch.cpp
    err.cpp
    event_publisher.cpp
    fs.cpp
    gssapi.cpp
    logging.cpp
    master.cpp
    profiler.cpp
    scheduler.cpp
    yaml.cpp
    zk2fs.cpp
    zkclient.cpp
    zklock.cpp
    zknamespace.cpp
)
target_include_directories(treadmill-runtime PRIVATE
    "${PROJECT_BINARY_DIR}"
    "${PROJECT_SOURCE_DIR}/lib"
)
install(TARGETS treadmill-runtime DESTINATION lib)
