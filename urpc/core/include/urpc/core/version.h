#pragma once

#define URPC_CORE_VERSION_MAJOR 0
#define URPC_CORE_VERSION_MINOR 1
#define URPC_CORE_VERSION_PATCH 0

#ifdef __cplusplus
extern "C++" {
namespace urpc {
namespace core {
const char* version();
}
}
}
#endif
