// echo_e2e_runner — portable process-level e2e driver (US3 / T029).
// Spawns urpc_echo_server, waits for readiness, spawns urpc_echo_client,
// checks exit codes, tears the server down. Uses uv_spawn (cross-platform).

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>

#include <uv.h>

namespace {

struct Proc {
  uv_process_t handle{};
  uv_pipe_t out{};
  int exit_code = -100;
  bool exited = false;
};

void OnExit(uv_process_t* proc, int64_t status, int) {
  auto* p = static_cast<Proc*>(proc->data);
  p->exit_code = static_cast<int>(status);
  p->exited = true;
  uv_close(reinterpret_cast<uv_handle_t*>(proc), nullptr);
}

int Spawn(uv_loop_t* loop, Proc* p, const std::string& file, char** args,
          bool pipe_stdout) {
  uv_stdio_container_t stdio[3];
  stdio[0].flags = UV_IGNORE;
  stdio[1].flags = static_cast<uv_stdio_flags>(
      pipe_stdout ? (UV_CREATE_PIPE | UV_WRITABLE_PIPE) : UV_IGNORE);
  stdio[1].data.stream = reinterpret_cast<uv_stream_t*>(&p->out);
  stdio[2].flags = UV_INHERIT_FD;
  stdio[2].data.fd = 2;

  uv_process_options_t opt;
  memset(&opt, 0, sizeof(opt));
  opt.file = file.c_str();
  opt.args = args;
  opt.stdio = stdio;
  opt.stdio_count = 3;
  opt.exit_cb = &OnExit;
  p->handle.data = p;
  if (pipe_stdout) uv_pipe_init(loop, &p->out, 0);
  return uv_spawn(loop, &p->handle, &opt);
}

int Wait(uv_loop_t* loop, Proc* p, int timeout_ms) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (!p->exited &&
         std::chrono::steady_clock::now() < deadline) {
    uv_run(loop, UV_RUN_ONCE);
  }
  if (!p->exited) {
    uv_process_kill(&p->handle, SIGTERM);
    uv_run(loop, UV_RUN_ONCE);
    std::fprintf(stderr, "echo_e2e: process timed out\\n");
    return -1;
  }
  return p->exit_code;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 4) {
    std::fprintf(stderr,
                 "usage: echo_e2e_runner <server> <client> <port> "
                 "[client-args...]");
    return 2;
  }
  const std::string server_bin = argv[1];
  const std::string client_bin = argv[2];
  const std::string port = argv[3];
  const std::string addr = "127.0.0.1:" + port;

  uv_loop_t loop;
  uv_loop_init(&loop);

  Proc server;
  std::string server_arg = addr;
  char* sargs[] = {const_cast<char*>(server_bin.c_str()),
                   const_cast<char*>(server_arg.data()), nullptr};
  if (Spawn(&loop, &server, server_bin, sargs, false) != 0) {
    std::fprintf(stderr, "echo_e2e: failed to spawn server\\n");
    return 2;
  }
  // readiness: brief settle (the server binds before serving)
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  Proc client;
  std::string caddr = addr;
  char* cargs[8] = {const_cast<char*>(client_bin.c_str()),
                    const_cast<char*>(caddr.data()), nullptr};
  int cn = 2;
  for (int i = 4; i < argc && cn < 7; i++) {
    cargs[cn++] = argv[i];
  }
  cargs[cn] = nullptr;
  if (Spawn(&loop, &client, client_bin, cargs, false) != 0) {
    std::fprintf(stderr, "echo_e2e: failed to spawn client\\n");
    uv_process_kill(&server.handle, SIGTERM);
    return 2;
  }

  int client_rc = Wait(&loop, &client, 30000);
  int server_rc = 0;
  if (!server.exited) {
    uv_process_kill(&server.handle, SIGTERM);
    server_rc = Wait(&loop, &server, 5000);
  }
  uv_run(&loop, UV_RUN_DEFAULT);
  uv_loop_close(&loop);

  if (client_rc != 0) {
    std::fprintf(stderr, "echo_e2e: client exit=%d\\n", client_rc);
    return 1;
  }
  std::printf("echo_e2e: PASS (client exit 0)\\n");
  return 0;
}
