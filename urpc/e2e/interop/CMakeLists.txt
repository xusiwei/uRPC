# gRPC Python interop peers (spec 001 US5).
# Runs via ctest when python3 + grpcio are available; SKIPs otherwise.

find_package(Python3 COMPONENTS Interpreter QUIET)
if(BUILD_TESTING AND URPC_BUILD_INTEROP AND Python3_FOUND)
  add_test(NAME interop-grpc-python
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/python/run_interop.py)
  set_tests_properties(interop-grpc-python PROPERTIES
    LABELS interop TIMEOUT 120 SKIP_RETURN_CODE 77)
endif()
