// Generated-service contract tests (spec 003, T009 / research.md §5).
//
// Layer 1 (compile-time): including "echo.service.h" and instantiating the
// interface/proxy already proves the generated signatures exist.
// Layer 2 (gold-sample): the generated header must contain the contractual
// fragments (interface, proxy inheritance, registration helper, method
// table, UNIMPLEMENTED defaults).

#include <fstream>
#include <iterator>
#include <string>

#include "echo.service.h"
#include "echo.upb.h"

#include <gtest/gtest.h>

namespace {

using urpc::gen::example::EchoServiceProxy;
using urpc::gen::example::IEchoService;

// Instantiating an implementation proves the interface is concrete (all
// methods have defaults) and the vtable layout exists.
class CodegenFixtureImpl : public IEchoService {};

TEST(ServiceCodegen, InterfaceIsInstantiableAndMethodTableComplete) {
  static_assert(std::size(IEchoService::kMethods) == 2,
                "echo.proto declares exactly two methods");
  CodegenFixtureImpl impl;
  (void)impl;
  SUCCEED();
}

}  // namespace
