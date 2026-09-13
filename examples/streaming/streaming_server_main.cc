// streaming example server (spec 004). Registrations land with the
// typed streaming API batches (US1-US3); this file anchors the build.
#include <cstdio>

int main(int argc, char* argv[]) {
  const char* addr = argc > 1 ? argv[1] : "127.0.0.1:50052";
  std::printf("streaming server would listen on %s\n", addr);
  return 0;
}
