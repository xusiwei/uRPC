add_library(urpc_cabi
  source/version.c
  source/urpc_c_status.c
)

target_include_directories(urpc_cabi PUBLIC include)
target_link_libraries(urpc_cabi PRIVATE urpc_core)

if(BUILD_TESTING AND URPC_BUILD_TESTS)
  file(GLOB URPC_CABI_TESTS test/test_*.cpp)
  if(URPC_CABI_TESTS)
    add_executable(urpc_cabi_tests ${URPC_CABI_TESTS})
    target_link_libraries(urpc_cabi_tests PRIVATE urpc_cabi GTest::gtest GTest::gtest_main)
    add_test(NAME urpc-cabi COMMAND urpc_cabi_tests)
  endif()
endif()
