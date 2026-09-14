add_library(urpc_api
  source/urpc_api_impl.cc
  # stable C-ABI surface (constitution Principle II) absorbed from the
  # former urpc/cabi module
  source/urpc_c_status.c
  source/version.c
)

target_include_directories(urpc_api PUBLIC include)
target_link_libraries(urpc_api PUBLIC urpc_upb urpc_core)

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
  set_tests_properties(urpc-bench-unary PROPERTIES LABELS bench TIMEOUT 900)

  add_executable(urpc_bench_streaming bench/bench_streaming.cc)
  target_link_libraries(urpc_bench_streaming PRIVATE urpc_api urpc_core
                                             urpc_streaming_proto
                                             benchmark::benchmark
                                             benchmark::benchmark_main)
  add_test(NAME urpc-bench-streaming
    COMMAND urpc_bench_streaming --benchmark_min_time=0.05s
            --benchmark_format=json
            --benchmark_out=${CMAKE_BINARY_DIR}/bench-streaming-last.json)
  set_tests_properties(urpc-bench-streaming PROPERTIES LABELS bench TIMEOUT 900)
endif()

if(BUILD_TESTING AND URPC_BUILD_TESTS)
  file(GLOB URPC_API_TESTS test/test_*.cc)
  if(URPC_API_TESTS)
    add_executable(urpc_api_tests ${URPC_API_TESTS})
    target_link_libraries(urpc_api_tests PRIVATE urpc_api urpc_core urpc_echo_proto urpc_streaming_proto GTest::gtest GTest::gtest_main)
    # Path to the generated service header for the codegen contract tests.
    if(TARGET urpc_echo_proto)
      target_compile_definitions(urpc_api_tests PRIVATE
        URPC_ECHO_SERVICE_HEADER="$<TARGET_PROPERTY:urpc_echo_proto,URPC_SERVICE_HEADER>")
    endif()
    add_test(NAME urpc-api COMMAND urpc_api_tests)
  endif()
endif()
