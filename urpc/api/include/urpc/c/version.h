#pragma once

#define URPC_CABI_VERSION "0.1.0"

#ifdef __cplusplus
extern "C" {
#endif

/* stable C surface for future language bindings (constitution Principle II) */
const char* urpc_cabi_version(void);

#ifdef __cplusplus
}
#endif
