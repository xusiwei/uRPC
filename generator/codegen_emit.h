#pragma once

// Pure emitters (spec 003/004): turn a codegen model into C++ text.
// No protobuf types here - every function is a pure model->string
// mapping, unit-testable without protoc (see generator/test/).

#include <string>

#include "codegen_model.h"

namespace urpc_codegen {

// per-method upb traits struct (XMethod).
std::string EmitTraits(const MethodModel& method);

// appends the server-side pure virtual interface I<Service> (with
// UNIMPLEMENTED defaults and the kMethods table) to `out`.
void EmitInterface(std::string* out, const ServiceModel& service);

// appends the client-side proxy XServiceProxy to `out`.
void EmitProxy(std::string* out, const ServiceModel& service);

// appends the inline RegisterService function to `out`.
void EmitRegister(std::string* out, const ServiceModel& service);

// namespace open/close pair (depth = 2 + package component count).
std::string OpenNamespaces(const std::string& package);
std::string CloseNamespaces(const std::string& package);

// whole-file emission: banner, includes, namespaces, per-service
// sections, namespace close. Deterministic: descriptor order, no
// timestamps, no environment-derived text.
std::string EmitHeader(const FileModel& file);
std::string EmitSource(const FileModel& file);

}  // namespace urpc_codegen
