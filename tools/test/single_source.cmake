# single_source.cmake — guard: dependency version literals may exist ONLY in
# third_party/versions.cmake (spec 002 FR-004 / SC-004).

cmake_minimum_required(VERSION 3.21)

get_filename_component(URPC_REPO "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)
include("${URPC_REPO}/third_party/versions.cmake")

set(URPC_LITERALS
  "${URPC_TP_LIBUV_VERSION}"
  "${URPC_TP_NGHTTP2_VERSION}"
  "${URPC_TP_PROTOBUF_VERSION}"
  "${URPC_TP_ABSEIL_VERSION}"
  "${URPC_TP_GOOGLETEST_VERSION}"
  "${URPC_TP_BENCHMARK_VERSION}")

file(GLOB URPC_FILES
  "${URPC_REPO}/CMakeLists.txt"
  "${URPC_REPO}/CMakePresets.json"
  "${URPC_REPO}/urpc/cmake/*.cmake"
  "${URPC_REPO}/urpc/*/*.txt"
  "${URPC_REPO}/urpc/*/source/*.cpp"
  "${URPC_REPO}/tools/*.cmake"
  "${URPC_REPO}/tools/test/*.cmake")

set(URPC_HITS "")
foreach(f IN LISTS URPC_FILES)
  file(READ "${f}" content)
  foreach(lit IN LISTS URPC_LITERALS)
    string(REPLACE "." "\\." lit_re "${lit}")
    if(content MATCHES "${lit_re}")
      string(APPEND URPC_HITS "  ${f}: contains version literal '${lit}'\n")
    endif()
  endforeach()
endforeach()

# tools/test/single_source.cmake legitimately includes the manifest for the
# literal list itself; it must not *contain* hardcoded versions — guaranteed
# by construction above (it reads them from the manifest).

if(URPC_HITS STREQUAL "")
  message(STATUS "single_source: PASS (version literals confined to third_party/versions.cmake)")
else()
  message(FATAL_ERROR "single_source: FAIL — version literals found outside the manifest:\n${URPC_HITS}")
endif()
