// protoc-gen-urpc — protoc plugin adapter (spec 003/004).
//
// Thin plumbing only: walks the descriptor tree into a codegen model
// (urpc_codegen namespace) and hands it to the pure emitters. All
// emission logic lives in codegen_emit.cc and is unit-tested there
// (generator/test/test_codegen_emit.cc) without running protoc.
//
// Invoked by protoc as `--urpc_out=<dir>`. For every .proto file that
// declares services it emits a deterministic header/source pair
// <basename>.service.h/.cc.

#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>

#include <cstring>
#include <memory>
#include <string>

#include "codegen_emit.h"
#include "codegen_model.h"

namespace {

using google::protobuf::FileDescriptor;
using google::protobuf::MethodDescriptor;
using google::protobuf::ServiceDescriptor;
using google::protobuf::compiler::CodeGenerator;
using google::protobuf::compiler::GeneratorContext;

using urpc_codegen::FileModel;
using urpc_codegen::MethodForm;
using urpc_codegen::MethodModel;
using urpc_codegen::ServiceModel;

MethodForm FormOf(const MethodDescriptor& m) {
  const bool cs = m.client_streaming();
  const bool ss = m.server_streaming();
  if (cs && ss) return MethodForm::kBidi;
  if (cs) return MethodForm::kClientStreaming;
  if (ss) return MethodForm::kServerStreaming;
  return MethodForm::kUnary;
}

MethodModel ModelOf(const MethodDescriptor& m,
                    const ServiceDescriptor& service,
                    const std::string& package) {
  MethodModel mm;
  mm.name = std::string(m.name());
  mm.service_full_name = std::string(service.full_name());
  mm.request_type =
      urpc_codegen::CppTypeOf(std::string(m.input_type()->full_name()));
  mm.response_type =
      urpc_codegen::CppTypeOf(std::string(m.output_type()->full_name()));
  mm.request_init =
      urpc_codegen::MsgInitOf(package,
                              std::string(m.input_type()->full_name()));
  mm.response_init =
      urpc_codegen::MsgInitOf(package,
                              std::string(m.output_type()->full_name()));
  mm.path =
      urpc_codegen::MethodPath(std::string(service.full_name()),
                               std::string(m.name()));
  mm.form = FormOf(m);
  return mm;
}

FileModel ModelOf(const FileDescriptor& file) {
  FileModel fm;
  fm.package = std::string(file.package());
  fm.source_file = std::string(file.name());
  for (int s = 0; s < file.service_count(); ++s) {
    const ServiceDescriptor* service = file.service(s);
    ServiceModel sm;
    sm.name = std::string(service->name());
    sm.full_name = std::string(service->full_name());
    for (int i = 0; i < service->method_count(); ++i)
      sm.methods.push_back(
          ModelOf(*service->method(i), *service, fm.package));
    fm.services.push_back(std::move(sm));
  }
  return fm;
}

void WriteOutput(GeneratorContext* context, const std::string& filename,
                 const std::string& content) {
  std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> stream(
      context->Open(filename));
  const char* data = content.data();
  std::size_t remaining = content.size();
  while (remaining > 0) {
    void* buffer = nullptr;
    int size = 0;
    if (!stream->Next(&buffer, &size) || size == 0) break;
    const std::size_t n = remaining < static_cast<std::size_t>(size)
                              ? remaining
                              : static_cast<std::size_t>(size);
    std::memcpy(buffer, data, n);
    data += n;
    remaining -= n;
    if (remaining == 0)
      stream->BackUp(static_cast<int>(size) - static_cast<int>(n));
  }
}

class UrpcServiceGenerator final : public CodeGenerator {
 public:
  uint64_t GetSupportedFeatures() const override {
    return FEATURE_PROTO3_OPTIONAL;
  }

  bool Generate(const FileDescriptor* file, const std::string& /*parameter*/,
                GeneratorContext* context, std::string* error) const override {
    // No services in the file: nothing to emit for this generator.
    if (file->service_count() == 0) return true;

    // A package is required to derive the upb minitable symbols
    // deterministically.
    const std::string package = std::string(file->package());
    const std::string file_name = std::string(file->name());
    if (package.empty()) {
      *error = "urpc: a package is required for --urpc_out (file: " +
               file_name + ")";
      return false;
    }

    const FileModel model = ModelOf(*file);
    const std::string base = file_name.substr(0, file_name.size() - 6);
    WriteOutput(context, base + ".service.h",
                urpc_codegen::EmitHeader(model));
    WriteOutput(context, base + ".service.cc",
                urpc_codegen::EmitSource(model));
    return true;
  }
};

}  // namespace

int main(int argc, char* argv[]) {
  UrpcServiceGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
