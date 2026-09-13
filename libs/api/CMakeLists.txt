add_library(urpc_api
  source/urpc_api_impl.cc
)

target_include_directories(urpc_api PUBLIC include)
target_link_libraries(urpc_api PUBLIC urpc_cabi urpc_upb urpc_core)

if(BUILD_TESTING AND URPC_BUILD_BENCH)
  add_executable(urpc_bench_unary bench/bench_unary.cpp)
  target_link_libraries(urpc_bench_unary PRIVATE urpc_api urpc_core
                                             urpc_echo_proto
                                             benchmark::benchmark
                                             benchmark::benchmark_main)
  add_test(NAME urpc-bench-unary
    COMMAND urpc_bench_unary --benchmark_min_time=0.05s
            --benchmark_format=json
            --benchmark_out=${CMAKE_BINARY_DIR}/bench-unary-last.json)
  set_tests_properties(urpc-bench-unary PROPERTIES LABELS bench TIMEOUT 300)
endif()

if(BUILD_TESTING AND URPC_BUILD_TESTS)
  file(GLOB URPC_API_TESTS test/test_*.cc)
  if(URPC_API_TESTS)
    add_executable(urpc_api_tests ${URPC_API_TESTS})
    target_link_libraries(urpc_api_tests PRIVATE urpc_api urpc_core urpc_echo_proto urpc_streaming_proto GTest::gtest GTest::gtest_main)
    # Path to the generated service header for the codegen contract tests.
    target_compile_definitions(urpc_api_tests PRIVATE
      URPC_ECHO_SERVICE_HEADER="$<TARGET_PROPERTY:urpc_echo_proto,URPC_SERVICE_HEADER>")
    add_test(NAME urpc-api COMMAND urpc_api_tests)
  endif()
endif()
