# protoc-gen-urpc — service interface/proxy generator plugin (spec 003).
#
# Built against the vendored protobuf toolchain (same plugin mechanism as
# protoc-gen-upb; see urpc-deps.cmake for the add_subdirectory hookup).
# Standard CMake entry; zero new external dependencies (constitution V).

add_library(urpc_codegen_emit STATIC
  codegen_model.cc
  codegen_emit.cc)
target_include_directories(urpc_codegen_emit PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(urpc_codegen_emit PUBLIC cxx_std_17)

add_executable(protoc-gen-urpc urpc_codegen.cc)
target_link_libraries(protoc-gen-urpc PRIVATE urpc_codegen_emit libprotoc libprotobuf)
target_compile_features(protoc-gen-urpc PRIVATE cxx_std_17)

if(BUILD_TESTING AND URPC_BUILD_TESTS)
  add_executable(urpc_codegen_tests test/test_codegen_emit.cc)
  target_link_libraries(urpc_codegen_tests PRIVATE urpc_codegen_emit GTest::gtest GTest::gtest_main)
  add_test(NAME urpc-codegen COMMAND urpc_codegen_tests)
endif()
