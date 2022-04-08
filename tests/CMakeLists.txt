cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
add_definitions(-DHAVE_CYRUS_SASL_H -DTHREADED -DUNIT_TEST)

enable_testing()

add_library(testmain STATIC main.cpp)
target_include_directories(testmain PRIVATE
    "${PROJECT_BINARY_DIR}"
    "${PROJECT_SOURCE_DIR}/lib"
)

add_library(mockzk STATIC mock_zookeeper.cpp)
target_include_directories(mockzk PRIVATE
    "${PROJECT_BINARY_DIR}"
    "${PROJECT_SOURCE_DIR}/lib"
)

foreach(TARGET example diff fs json scheduler)
    add_executable(${TARGET}_test ${TARGET}_test.cpp)
    target_include_directories(${TARGET}_test PRIVATE
        "${PROJECT_BINARY_DIR}"
        "${PROJECT_SOURCE_DIR}/lib"
    )
    target_link_libraries(
        ${TARGET}_test
        testmain
        treadmill-runtime
        mockzk
        gssapi_krb5
        krb5
        yaml
        uv
        pthread
        z
        sasl2
        ssl
        fmt
        cpp-base64-17
    )
    add_test(${TARGET}_test ${TARGET}_test)
endforeach(TARGET)

set(
    UNIT_TEST_TARGETS
    example_test diff_test fs_test json_test scheduler_test
)
add_custom_target(
    all_tests ALL
    DEPENDS ${UNIT_TEST_TARGETS}
)
add_custom_command(
    TARGET all_tests
    COMMENT "Run tests"
    POST_BUILD COMMAND LD_LIBRARY_PATH=${PROJECT_BINARY_DIR}/lib ctest
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
