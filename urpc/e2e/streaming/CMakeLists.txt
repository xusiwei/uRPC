# urpc/e2e/streaming - end-to-end example for the three streaming forms
# (spec 004: server streaming / client streaming / bidi).

set(URPC_STREAMING_GEN_DIR "${CMAKE_BINARY_DIR}/gen/streaming")
file(MAKE_DIRECTORY "${URPC_STREAMING_GEN_DIR}")

urpc_proto_upb(
  PROTO "${CMAKE_CURRENT_SOURCE_DIR}/streaming.proto"
  IMPORT_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
  OUT_DIR "${URPC_STREAMING_GEN_DIR}")

add_library(urpc_streaming_proto STATIC
  "${URPC_STREAMING_GEN_DIR}/streaming.upb.c"
  "${URPC_STREAMING_GEN_DIR}/streaming.upb_minitable.c"
  "${URPC_STREAMING_GEN_DIR}/streaming.service.cc")
target_include_directories(urpc_streaming_proto PUBLIC
  "${URPC_STREAMING_GEN_DIR}"
  "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(urpc_streaming_proto PRIVATE urpc_upb)
target_link_libraries(urpc_streaming_proto PUBLIC urpc_api)
set_target_properties(urpc_streaming_proto PROPERTIES
  URPC_SERVICE_HEADER "${URPC_STREAMING_GEN_DIR}/streaming.service.h")

if(URPC_BUILD_EXAMPLES)
  add_executable(urpc_streaming_server streaming_server_main.cc)
  target_link_libraries(urpc_streaming_server PRIVATE urpc_api urpc_core
                                                     urpc_streaming_proto)

  add_executable(urpc_streaming_client streaming_client_main.cc)
  target_link_libraries(urpc_streaming_client PRIVATE urpc_api urpc_core
                                                      urpc_streaming_proto)
endif()
