#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define URPC_CABI_VERSION "0.1.0"

const char* urpc_cabi_version(void);

/* Stable C mirror of urpc::core::StatusCode (gRPC codes, closed set for
 * this phase). Values MUST stay stable — future bindings depend on them. */
typedef enum urpc_c_status_code {
  URPC_C_OK = 0,
  URPC_C_DEADLINE_EXCEEDED = 4,
  URPC_C_UNIMPLEMENTED = 12,
  URPC_C_INTERNAL = 13,
  URPC_C_UNAVAILABLE = 14,
  URPC_C_DATA_LOSS = 15
} urpc_c_status_code;

/* Opaque status object; never dereferenced by bindings. */
typedef struct urpc_c_status {
  int code;
  const char* message; /* owned by the status producer, valid until freed */
} urpc_c_status;

const char* urpc_c_status_code_name(int code);
int urpc_c_status_ok(const urpc_c_status* s);

#ifdef __cplusplus
}
#endif
