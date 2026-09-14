cmake_minimum_required(VERSION 3.21)

project(urpc
  VERSION 0.1.0
  DESCRIPTION "Cross-platform, gRPC-compatible RPC framework (libuv + nghttp2 + upb)"
  LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
  add_compile_options(/W4)
else()
  add_compile_options(-Wall -Wextra)
endif()

option(URPC_BUILD_TESTS    "Build urpc unit/integration tests" ON)
option(URPC_BUILD_BENCH    "Build urpc benchmarks"             ON)
option(URPC_BUILD_EXAMPLES "Build urpc examples"               ON)
option(URPC_BUILD_INTEROP  "Build gRPC interop test wiring"    ON)
option(URPC_WERROR         "Treat warnings as errors"          OFF)

if(URPC_WERROR)
  if(CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    add_compile_options(/WX)
  else()
    add_compile_options(-Werror)
  endif()
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(CTest)
include(urpc-deps)
include(urpc-proto)

add_subdirectory(urpc/core)
add_subdirectory(urpc/cabi)
add_subdirectory(urpc/api)

if(URPC_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(URPC_BUILD_INTEROP)
  add_subdirectory(interop)
endif()

if(BUILD_TESTING AND URPC_BUILD_TESTS)
  # per-module tests are registered by their add_subdirectory calls above
endif()

if(URPC_BUILD_BENCH AND BUILD_TESTING)
  # benches are registered by urpc/core
endif()

# ---------------------------------------------------------------------------
# vendored third-party guard tests (spec 002-vendor-third-party)
# ---------------------------------------------------------------------------
if(BUILD_TESTING)
  add_test(NAME vendor-third-party-idempotent
    COMMAND ${CMAKE_COMMAND}
            -DURPC_TP_CHECK_ONLY=ON
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tools/fetch_third_party.cmake)
  set_tests_properties(vendor-third-party-idempotent PROPERTIES LABELS vendor)

  add_test(NAME vendor-build-guard
    COMMAND ${CMAKE_COMMAND}
            -DCMAKE_GENERATOR=${CMAKE_GENERATOR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tools/test/build_guard.cmake)
  set_tests_properties(vendor-build-guard PROPERTIES LABELS vendor)

  add_test(NAME vendor-single-source
    COMMAND ${CMAKE_COMMAND}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tools/test/single_source.cmake)
  set_tests_properties(vendor-single-source PROPERTIES LABELS vendor)

  add_test(NAME vendor-directory-set
    COMMAND ${CMAKE_COMMAND}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tools/test/directory_set.cmake)
  set_tests_properties(vendor-directory-set PROPERTIES LABELS vendor)
endif()
