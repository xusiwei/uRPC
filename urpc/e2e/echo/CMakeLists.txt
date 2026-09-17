# urpc/e2e/echo - end-to-end example (server + client + shared proto).
# Also builds the upb codegen used by api-layer tests and the interop peers.

# --- upb codegen for echo.proto (C++) ----------------------------------------
set(URPC_ECHO_GEN_DIR "${CMAKE_BINARY_DIR}/gen/echo")
file(MAKE_DIRECTORY "${URPC_ECHO_GEN_DIR}")

urpc_proto_upb(
  PROTO "${CMAKE_CURRENT_SOURCE_DIR}/echo.proto"
  IMPORT_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
  OUT_DIR "${URPC_ECHO_GEN_DIR}")

add_library(urpc_echo_proto STATIC
  "${URPC_ECHO_GEN_DIR}/echo.upb.c"
  "${URPC_ECHO_GEN_DIR}/echo.upb_minitable.c"
  "${URPC_ECHO_GEN_DIR}/echo.service.cc")
target_include_directories(urpc_echo_proto PUBLIC
  "${URPC_ECHO_GEN_DIR}"
  "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(urpc_echo_proto PRIVATE urpc_upb)
# The generated service header (echo.service.h) consumes api-layer types;
# propagate urpc_api to every consumer of urpc_echo_proto (spec 003).
target_link_libraries(urpc_echo_proto PUBLIC urpc_api)
set_target_properties(urpc_echo_proto PROPERTIES
  URPC_SERVICE_HEADER "${URPC_ECHO_GEN_DIR}/echo.service.h")

# --- example binaries (US3) ----------------------------------------------------
if(URPC_BUILD_EXAMPLES)
  add_executable(urpc_echo_server echo_server_main.cpp)
  target_link_libraries(urpc_echo_server PRIVATE urpc_api urpc_core urpc_echo_proto)

  add_executable(urpc_echo_client echo_client_main.cpp)
  target_link_libraries(urpc_echo_client PRIVATE urpc_api urpc_core urpc_echo_proto)

  # process-level end-to-end test (US3 / T029)
  add_executable(urpc_echo_e2e_runner echo_e2e_runner.cpp)
  target_link_libraries(urpc_echo_e2e_runner PRIVATE ${URPC_LIBUV_TARGET})
  add_test(NAME examples-echo-e2e
    COMMAND urpc_echo_e2e_runner
            $<TARGET_FILE:urpc_echo_server>
            $<TARGET_FILE:urpc_echo_client>
            51090)
  set_tests_properties(examples-echo-e2e PROPERTIES LABELS e2e TIMEOUT 60)
endif()
