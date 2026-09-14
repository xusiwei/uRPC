#include "urpc/c/status.h"

/* Numeric values mirror urpc::core::StatusCode (verified from both sides by
 * urpc-cabi / urpc-core unit tests); the C surface must not include C++
 * headers, so the mapping is intentionally self-contained. */

const char* urpc_c_status_code_name(int code) {
  switch (code) {
    case URPC_C_OK: return "OK";
    case URPC_C_DEADLINE_EXCEEDED: return "DEADLINE_EXCEEDED";
    case URPC_C_UNIMPLEMENTED: return "UNIMPLEMENTED";
    case URPC_C_INTERNAL: return "INTERNAL";
    case URPC_C_UNAVAILABLE: return "UNAVAILABLE";
    case 8: return "RESOURCE_EXHAUSTED";
    case URPC_C_DATA_LOSS: return "DATA_LOSS";
  }
  return "UNKNOWN";
}

int urpc_c_status_ok(const urpc_c_status* s) {
  return s != (const urpc_c_status*)0 && s->code == URPC_C_OK;
}
