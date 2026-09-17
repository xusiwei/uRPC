# urpc/e2e - end-to-end material: runnable example services (echo,
# streaming) plus the official-gRPC Python interop peers.
if(URPC_BUILD_EXAMPLES)
  add_subdirectory(echo)
  add_subdirectory(streaming)
endif()

if(URPC_BUILD_INTEROP)
  add_subdirectory(interop)
endif()
