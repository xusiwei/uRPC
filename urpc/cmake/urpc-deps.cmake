# urpc-deps.cmake — consume vendored third-party dependencies.
#
# Policy (spec 002-vendor-third-party; constitution Principle V):
#   * every C++ dependency is vendored under third_party/<name>/ by
#     `cmake -P tools/fetch_third_party.cmake` (pinned in
#     third_party/versions.cmake — single reviewed source of truth)
#   * configure NEVER touches the network; missing/stale deps are a hard
#     error with fetch guidance (contracts/build-guard.md)
#   * exposed target names are stable: uv_a / nghttp2_static / urpc_upb /
#     protoc / GTest::gtest / benchmark::benchmark

include("${CMAKE_CURRENT_SOURCE_DIR}/third_party/versions.cmake")

# urpc consumes all third-party code as static archives (embeddable builds;
# also avoids non-PIC clashes when third-party subprojects flip
# BUILD_SHARED_LIBS). Override locally if you know what you are doing.
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)

# ---------------------------------------------------------------------------
# guard: a dependency is "vendored" iff its marker matches the manifest
# ---------------------------------------------------------------------------
function(urpc_tp_require NAME)
  string(TOUPPER "${NAME}" P)
  set(ver "${URPC_TP_${P}_VERSION}")
  set(sha "${URPC_TP_${P}_SHA256}")
  set(marker "${CMAKE_CURRENT_SOURCE_DIR}/third_party/${NAME}/.urpc-version")
  set(expected "version=${ver}\nsha256=${sha}\n")
  set(ok FALSE)
  if(EXISTS "${marker}")
    file(READ "${marker}" actual)
    if(actual STREQUAL expected)
      set(ok TRUE)
    endif()
  endif()
  if(NOT ok)
    message(FATAL_ERROR
      "urpc: third-party dependency '${NAME}' (version ${ver}) is not vendored.\n"
      "      Run:  cmake -P tools/fetch_third_party.cmake\n"
      "      (from the repository root; requires network access)")
  endif()
endfunction()

# ---------------------------------------------------------------------------
# MSVC CRT policy (fixes Windows CI): every third-party subproject and urpc
# itself must use the same dynamic CRT (/MD, MultiThreadedDLL). protobuf's
# static build defaults to /MT (protobuf_MSVC_STATIC_RUNTIME=ON), which
# clashes with abseil's /MD default and fails protoc-gen-upb with LNK2038.
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")

# ---------------------------------------------------------------------------
# libuv
# ---------------------------------------------------------------------------
urpc_tp_require(libuv)
set(LIBUV_BUILD_SHARED OFF CACHE INTERNAL "")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/third_party/libuv"
                 "${CMAKE_BINARY_DIR}/third_party/libuv")
if(TARGET libuv::libuv)
  set(URPC_LIBUV_TARGET libuv::libuv)
elseif(TARGET uv_a)
  set(URPC_LIBUV_TARGET uv_a)
else()
  set(URPC_LIBUV_TARGET uv)
endif()

# ---------------------------------------------------------------------------
# nghttp2
# ---------------------------------------------------------------------------
urpc_tp_require(nghttp2)
set(ENABLE_SHARED_LIB OFF CACHE INTERNAL "")
set(ENABLE_STATIC_LIB ON  CACHE INTERNAL "")
set(ENABLE_BIN         OFF CACHE INTERNAL "")
set(ENABLE_EXAMPLES    OFF CACHE INTERNAL "")
set(ENABLE_TESTS       OFF CACHE INTERNAL "")
set(ENABLE_DOC         OFF CACHE INTERNAL "")
set(ENABLE_HTTP3       OFF CACHE INTERNAL "")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/third_party/nghttp2"
                 "${CMAKE_BINARY_DIR}/third_party/nghttp2")
if(TARGET nghttp2_static)
  set(URPC_NGHTTP2_TARGET nghttp2_static)
else()
  set(URPC_NGHTTP2_TARGET nghttp2)
endif()
# MSVC has no POSIX ssize_t; defining this crops the deprecated ssize_t-based
# API surface out of the public header (all *2 variants stay visible; urpc
# uses only those — see lib/includes/nghttp2/nghttp2.h).
target_compile_definitions(${URPC_NGHTTP2_TARGET}
  PUBLIC NGHTTP2_NO_SSIZE_T)

# ---------------------------------------------------------------------------
# upb runtime (C subset of the vendored protobuf tree; no abseil,
# no libprotobuf). Typed access comes from protoc --upb_out generated code.
# ---------------------------------------------------------------------------
urpc_tp_require(protobuf)
set(URPC_PROTOBUF_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf")

file(GLOB_RECURSE URPC_UPB_SOURCES
  "${URPC_PROTOBUF_DIR}/upb/*.c")
# drop tests/fuzzers/conformance/build-generated copies and reflection
# (reflection needs generated descriptor tables which we do not use)
list(FILTER URPC_UPB_SOURCES EXCLUDE REGEX "/(test|tests|fuzz|fuzzer|conformance|cmake|benchmark|benchmarks|reflection|json|text|util)/")
list(FILTER URPC_UPB_SOURCES EXCLUDE REGEX "(_test|_tests|_fuzz|_fuzzer)\\.c$")
list(FILTER URPC_UPB_SOURCES EXCLUDE REGEX "(^|/)test_[^/]*\\.c$")

# utf8_range (upb's UTF-8 validation) — vendored in the protobuf tree
list(APPEND URPC_UPB_SOURCES
  "${URPC_PROTOBUF_DIR}/third_party/utf8_range/utf8_range.c")

add_library(urpc_upb STATIC ${URPC_UPB_SOURCES})
target_include_directories(urpc_upb PUBLIC
  "${URPC_PROTOBUF_DIR}"
  "${URPC_PROTOBUF_DIR}/third_party/utf8_range")
target_compile_options(urpc_upb PRIVATE
  $<$<C_COMPILER_ID:GNU,Clang,AppleClang>:-Wno-declaration-after-statement>)

# ---------------------------------------------------------------------------
# host protoc with --upb_out support (protobuf >= 23):
# use a usable system protoc when present, otherwise build from vendored
# source (protobuf + its pinned abseil). No downloads in either path.
# ---------------------------------------------------------------------------
# Host protoc + upb generator plugins are always built from the vendored
# protobuf source (spec 002: single vendored source of truth; official
# protoc distributions ship --upb_out as plugins anyway).
urpc_tp_require(abseil)
set(ABSL_ENABLE_TESTING OFF CACHE INTERNAL "")
set(ABSL_PROPAGATE_CXX_STD ON CACHE INTERNAL "")
set(ABSL_BUILD_MONOLITHIC_CPP_LIB OFF CACHE INTERNAL "")
# keep abseil on the dynamic CRT (/MD) — matches protobuf below; MSVC refuses
# to link /MT and /MD objects together (LNK2038)
set(ABSL_MSVC_STATIC_RUNTIME OFF CACHE INTERNAL "")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/third_party/abseil"
                 "${CMAKE_BINARY_DIR}/third_party/abseil")

set(protobuf_INSTALL OFF CACHE INTERNAL "")
set(protobuf_BUILD_TESTS OFF CACHE INTERNAL "")
set(protobuf_BUILD_CONFORMANCE OFF CACHE INTERNAL "")
set(protobuf_BUILD_EXAMPLES OFF CACHE INTERNAL "")
set(protobuf_BUILD_LIBUPB ON CACHE INTERNAL "")  # builds the upb codegen plugins
set(protobuf_DISABLE_RTTI ON CACHE INTERNAL "")
set(protobuf_WITH_ZLIB OFF CACHE INTERNAL "")
set(protobuf_BUILD_SHARED_LIBS OFF CACHE INTERNAL "")
# protobuf defaults protobuf_MSVC_STATIC_RUNTIME to ON for static builds,
# which compiles its objects to /MT against abseil's /MD — link fails with
# LNK2038 on protoc-gen-upb. Pin it to the dynamic CRT like everything else.
set(protobuf_MSVC_STATIC_RUNTIME OFF CACHE INTERNAL "")
set(utf8_range_ENABLE_TESTS OFF CACHE INTERNAL "")
set(utf8_range_ENABLE_INSTALL OFF CACHE INTERNAL "")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf"
                 "${CMAKE_BINARY_DIR}/third_party/protobuf"
                 EXCLUDE_FROM_ALL)
set(URPC_PROTOC_EXECUTABLE "$<TARGET_FILE:protoc>" CACHE STRING
    "protoc used for code generation (generator expression)")
set(URPC_PROTOC_PLUGIN_UPB "$<TARGET_FILE:protoc-gen-upb>" CACHE STRING
    "protoc-gen-upb plugin (generator expression)")
set(URPC_PROTOC_PLUGIN_MINITABLE "$<TARGET_FILE:protoc-gen-upb_minitable>"
    CACHE STRING "protoc-gen-upb_minitable plugin (generator expression)")

# protoc-gen-urpc: typed service interface/proxy generator (spec 003).
# Built from urpc/generator/ against this vendored protobuf toolchain — same
# plugin mechanism as protoc-gen-upb; adds no external dependency
# (constitution V).
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/urpc/generator"
                 "${CMAKE_BINARY_DIR}/third_party/protoc-gen-urpc")
set(URPC_PROTOC_PLUGIN_URPC "$<TARGET_FILE:protoc-gen-urpc>" CACHE STRING
    "protoc-gen-urpc plugin (generator expression)")
set(URPC_PROTOC_TARGET protoc)
message(STATUS "urpc: host protoc + upb generators build from third_party/protobuf")

# ---------------------------------------------------------------------------
# GoogleTest / Google Benchmark (dev+test only; never in runtime artifacts)
# ---------------------------------------------------------------------------
if(BUILD_TESTING AND URPC_BUILD_TESTS AND NOT TARGET GTest::gtest)
  urpc_tp_require(googletest)
  set(INSTALL_GTEST OFF CACHE INTERNAL "")
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/third_party/googletest"
                   "${CMAKE_BINARY_DIR}/third_party/googletest")
  if(NOT TARGET GTest::gtest)
    add_library(GTest::gtest ALIAS gtest)
  endif()
endif()

if(BUILD_TESTING AND URPC_BUILD_BENCH AND NOT TARGET benchmark::benchmark)
  urpc_tp_require(benchmark)
  set(BENCHMARK_ENABLE_TESTING OFF CACHE INTERNAL "")
  set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE INTERNAL "")
  set(BENCHMARK_INSTALL_SHARE OFF CACHE INTERNAL "")
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/third_party/benchmark"
                   "${CMAKE_BINARY_DIR}/third_party/benchmark")
  if(NOT TARGET benchmark::benchmark)
    add_library(benchmark::benchmark ALIAS benchmark)
  endif()
endif()
