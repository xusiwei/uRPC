add_library(urpc_core
  source/version.cpp
  source/urpc_status.cpp
  source/urpc_log.cpp
  source/urpc_loop.cpp
  source/urpc_codec.cpp
  source/urpc_router.cpp
  source/urpc_h2_session.cc
  source/urpc_server.cc
  source/urpc_channel.cc
  source/platform/urpc_platform.cpp
)

target_include_directories(urpc_core PUBLIC include)
target_link_libraries(urpc_core
  PUBLIC ${URPC_LIBUV_TARGET}
  PRIVATE ${URPC_NGHTTP2_TARGET}
  PRIVATE urpc_upb
)

if(BUILD_TESTING AND URPC_BUILD_TESTS)
  file(GLOB URPC_CORE_TESTS test/test_*.cpp)
  if(URPC_CORE_TESTS)
    add_executable(urpc_core_tests ${URPC_CORE_TESTS})
    target_link_libraries(urpc_core_tests PRIVATE urpc_core GTest::gtest GTest::gtest_main)
    # wire-protocol tests need raw HTTP/2 error codes (test-only linkage)
    target_link_libraries(urpc_core_tests PRIVATE ${URPC_NGHTTP2_TARGET})
    add_test(NAME urpc-core COMMAND urpc_core_tests)
  endif()
endif()

if(BUILD_TESTING AND URPC_BUILD_BENCH)
  file(GLOB URPC_CORE_BENCH bench/bench_*.cpp)
  if(URPC_CORE_BENCH)
    add_executable(urpc_core_bench ${URPC_CORE_BENCH})
    target_link_libraries(urpc_core_bench PRIVATE urpc_core benchmark::benchmark benchmark::benchmark_main)
    add_test(NAME urpc-core-bench COMMAND urpc_core_bench --benchmark_min_time=0.01s LABELS bench)
  endif()
endif()
