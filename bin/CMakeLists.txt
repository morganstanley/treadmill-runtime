cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
add_definitions(-DHAVE_CYRUS_SASL_H -DTHREADED)

add_executable(treadmill-scheduler scheduler.cpp)
add_executable(treadmill-eventpub eventpub.cpp)
add_executable(treadmill-zk2fs zk2fs.cpp)
add_executable(zklock zklock.cpp)

foreach(TARGET treadmill-scheduler treadmill-eventpub treadmill-zk2fs zklock)

    target_include_directories(${TARGET} PRIVATE
        "${PROJECT_BINARY_DIR}"
        "${PROJECT_SOURCE_DIR}/lib"
    )

    target_link_libraries(
        ${TARGET}
        treadmill-runtime
        zookeeper
        hashtable
        gssapi_krb5
        krb5
        yaml
        uv
        pthread
        z
        sasl2
        crypto
        ssl
        spdlog
        fmt
        cpp-base64-17
    )
    install(TARGETS ${TARGET} DESTINATION bin)

endforeach(TARGET)
