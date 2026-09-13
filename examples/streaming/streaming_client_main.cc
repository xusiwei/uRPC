// streaming example client (spec 004). Modes (range/sum/chat) land with
// the typed streaming API batches (US1-US3).
#include <cstdio>

int main(int argc, char* argv[]) {
  const char* addr = argc > 1 ? argv[1] : "127.0.0.1:50052";
  std::printf("streaming client would dial %s\n", addr);
  return 0;
}
