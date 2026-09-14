#pragma once

// Pure code-generation model (spec 003/004): a protobuf-free description
// of what protoc-gen-urpc emits. The plugin adapter fills the model from
// descriptor objects; the emitters in codegen_emit turn the model into
// C++ text. Keeping descriptors out of the model makes every emitter
// unit-testable without running protoc.

#include <string>
#include <vector>

namespace urpc_codegen {

// Mirrors ::urpc::MethodForm (the emitted C++ references the api enum by
// qualified name); kept separate so the generator never includes api
// headers.
enum class MethodForm {
  kUnary,
  kServerStreaming,
  kClientStreaming,
  kBidi,
};

struct MethodModel {
  std::string name;              // "Echo"
  std::string service_full_name; // "example.EchoService"
  std::string request_type;      // "example_EchoRequest"   (upb C type)
  std::string response_type;     // "example_EchoResponse"
  std::string request_init;      // "example__EchoRequest_msg_init" symbol
  std::string response_init;     // "example__EchoResponse_msg_init" symbol
  std::string path;              // "/example.EchoService/Echo"
  MethodForm form = MethodForm::kUnary;
};

struct ServiceModel {
  std::string name;       // "EchoService"
  std::string full_name;  // "example.EchoService"
  std::vector<MethodModel> methods;
};

struct FileModel {
  std::string package;      // "example"
  std::string source_file;  // "echo.proto"
  std::vector<ServiceModel> services;
};

// example.EchoRequest -> example_EchoRequest (upb C type name).
std::string CppTypeOf(const std::string& full_name);

// upb minitable init symbol: <package>__<message path with '_'>_msg_init.
std::string MsgInitOf(const std::string& package,
                      const std::string& full_name);

// "/<service_full_name>/<method_name>".
std::string MethodPath(const std::string& service_full_name,
                       const std::string& method_name);

// Qualified ::urpc::MethodForm spelling emitted into generated code.
const char* FormName(MethodForm form);

}  // namespace urpc_codegen
