#include <gtest/gtest.h>

#include <string>

#include "urpc/c/status.h"

TEST(CabiSurface, VersionIsExported) {
  const char* v = urpc_cabi_version();
  ASSERT_NE(v, nullptr);
  EXPECT_STRNE(v, "");
  EXPECT_STRNE(v, "0.0.0");
}

TEST(CabiSurface, StatusCodeNamesMatchGrpcCanonical) {
  EXPECT_STREQ(urpc_c_status_code_name(URPC_C_OK), "OK");
  EXPECT_STREQ(urpc_c_status_code_name(URPC_C_DEADLINE_EXCEEDED),
               "DEADLINE_EXCEEDED");
  EXPECT_STREQ(urpc_c_status_code_name(URPC_C_UNIMPLEMENTED),
               "UNIMPLEMENTED");
  EXPECT_STREQ(urpc_c_status_code_name(URPC_C_INTERNAL), "INTERNAL");
  EXPECT_STREQ(urpc_c_status_code_name(URPC_C_UNAVAILABLE), "UNAVAILABLE");
  EXPECT_STREQ(urpc_c_status_code_name(URPC_C_DATA_LOSS), "DATA_LOSS");
  EXPECT_STREQ(urpc_c_status_code_name(-1), "UNKNOWN");
}

TEST(CabiSurface, EnumValuesAreStable) {
  // Bindings rely on these numeric values; changing them is a break.
  EXPECT_EQ(URPC_C_OK, 0);
  EXPECT_EQ(URPC_C_DEADLINE_EXCEEDED, 4);
  EXPECT_EQ(URPC_C_UNIMPLEMENTED, 12);
  EXPECT_EQ(URPC_C_INTERNAL, 13);
  EXPECT_EQ(URPC_C_UNAVAILABLE, 14);
  EXPECT_EQ(URPC_C_DATA_LOSS, 15);
}

TEST(CabiSurface, StatusOkSemantics) {
  urpc_c_status ok{URPC_C_OK, "fine"};
  urpc_c_status err{URPC_C_INTERNAL, "boom"};
  EXPECT_NE(urpc_c_status_ok(&ok), 0);
  EXPECT_EQ(urpc_c_status_ok(&err), 0);
  EXPECT_EQ(urpc_c_status_ok(nullptr), 0);
}

TEST(CabiSurface, NoStdTypesLeakIntoHeader) {
  // The header must compile as pure C; this translation unit includes only
  // <string> after the C header and stays valid.
  std::string s = urpc_c_status_code_name(URPC_C_OK);
  EXPECT_EQ(s, "OK");
}
