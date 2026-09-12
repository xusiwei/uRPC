# urpc-proto.cmake — proto → upb C code generation + typed service
# interface/proxy generation (spec 003).
#
# Uses the host protoc (URPC_PROTOC_EXECUTABLE, see urpc-deps.cmake) with the
# built-in --upb_out generator plus the protoc-gen-urpc service generator
# (--urpc_out). Generated code pairs with the urpc_upb runtime; the
# .service.h/.cc pair carries the typed interface + proxy for the api layer.

function(urpc_proto_upb)
  cmake_parse_arguments(ARG "" "PROTO;OUT_DIR;IMPORT_DIR" "TARGETS" ${ARGN})
  if(NOT ARG_PROTO)
    message(FATAL_ERROR "urpc_proto_upb: PROTO is required")
  endif()
  if(NOT ARG_OUT_DIR)
    message(FATAL_ERROR "urpc_proto_upb: OUT_DIR is required")
  endif()

  get_filename_component(proto_abs "${ARG_PROTO}" ABSOLUTE)
  get_filename_component(proto_name_we "${proto_abs}" NAME_WE)
  set(gen_h "${ARG_OUT_DIR}/${proto_name_we}.upb.h")
  set(gen_c "${ARG_OUT_DIR}/${proto_name_we}.upb.c")

  set(gen_mt_h "${ARG_OUT_DIR}/${proto_name_we}.upb_minitable.h")
  set(gen_mt_c "${ARG_OUT_DIR}/${proto_name_we}.upb_minitable.c")
  set(gen_svc_h "${ARG_OUT_DIR}/${proto_name_we}.service.h")
  set(gen_svc_cc "${ARG_OUT_DIR}/${proto_name_we}.service.cc")
  add_custom_command(OUTPUT "${gen_h}" "${gen_c}" "${gen_mt_h}" "${gen_mt_c}"
                          "${gen_svc_h}" "${gen_svc_cc}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${ARG_OUT_DIR}"
    COMMAND "${URPC_PROTOC_EXECUTABLE}"
            "--plugin=protoc-gen-upb=${URPC_PROTOC_PLUGIN_UPB}"
            "--plugin=protoc-gen-upb_minitable=${URPC_PROTOC_PLUGIN_MINITABLE}"
            "--plugin=protoc-gen-urpc=${URPC_PROTOC_PLUGIN_URPC}"
            "--proto_path=${ARG_IMPORT_DIR}"
            "--upb_out=${ARG_OUT_DIR}"
            "--upb_minitable_out=${ARG_OUT_DIR}"
            "--urpc_out=${ARG_OUT_DIR}"
            "${proto_abs}"
    # plugin exes are regular deps so generator changes re-run codegen
    DEPENDS "${proto_abs}" ${URPC_PROTOC_TARGET} protoc-gen-upb
            protoc-gen-upb_minitable protoc-gen-urpc
    COMMENT "urpc: upb codegen ${proto_name_we}.proto"
    VERBATIM)

  if(ARG_TARGETS)
    foreach(t IN LISTS ARG_TARGETS)
      target_sources(${t} PRIVATE "${gen_c}" "${gen_svc_cc}")
      target_include_directories(${t} PUBLIC "${ARG_OUT_DIR}")
      # The generated service header consumes api-layer types
      # (ServerContext/UnaryDone/Channel/Client) — propagate urpc_api so
      # every consumer of the generated pair compiles (spec 003).
      if(TARGET urpc_api)
        target_link_libraries(${t} PUBLIC urpc_api)
      endif()
    endforeach()
  endif()
endfunction()
