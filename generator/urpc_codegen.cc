// protoc-gen-urpc — urpc service interface/proxy generator (spec 003/004).
//
// Invoked by protoc as `--urpc_out=<dir>`. For every .proto file that
// declares services it emits a deterministic header/source pair
// <basename>.service.h/.cc containing:
//   - per-method upb traits (bridge to the upb generated symbols),
//     including the MethodForm (unary / server-streaming / client-
//     streaming / bidi, spec 004)
//   - server-side pure virtual interface  I<Service>  (FR-001, unary)
//   - client-side proxy  <Service>Proxy  inheriting the same interface
//     (unary; streaming interface/proxy generation lands with the typed
//     streaming API, spec 004 US4)
//   - RegisterService(Server&, I<Service>&): one call publishes every
//     method of `impl` (unary)
//
// Deterministic output: iteration follows descriptor order; no
// timestamps, no absolute paths, no environment-derived text (FR-008).

#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>

#include <cstring>
#include <memory>
#include <string>

namespace {

using google::protobuf::FileDescriptor;
using google::protobuf::MethodDescriptor;
using google::protobuf::ServiceDescriptor;
using google::protobuf::compiler::CodeGenerator;
using google::protobuf::compiler::GeneratorContext;

std::string ReplaceAll(std::string s, char from, char to) {
  for (char& c : s)
    if (c == from) c = to;
  return s;
}

// example.EchoRequest -> example_EchoRequest (upb C type name).
std::string CppTypeOf(const std::string& full_name) {
  return ReplaceAll(full_name, '.', '_');
}

// upb minitable init symbol: <package>__<message path with '_'>.
std::string MsgInitOf(const std::string& package,
                      const std::string& full_name) {
  std::string rest = full_name;
  if (!package.empty() && rest.rfind(package + ".", 0) == 0)
    rest = rest.substr(package.size() + 1);
  return package + "__" + ReplaceAll(rest, '.', '_') + "_msg_init";
}

std::string MethodPath(const std::string& service_full_name,
                       const std::string& method_name) {
  return "/" + service_full_name + "/" + method_name;
}

// MethodForm derivation from the proto `stream` keywords (spec 004).
const char* FormOf(const MethodDescriptor* m) {
  if (m->client_streaming() && m->server_streaming())
    return "::urpc::MethodForm::kBidi";
  if (m->client_streaming()) return "::urpc::MethodForm::kClientStreaming";
  if (m->server_streaming()) return "::urpc::MethodForm::kServerStreaming";
  return "::urpc::MethodForm::kUnary";
}

bool IsStreaming(const MethodDescriptor* m) {
  return m->client_streaming() || m->server_streaming();
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

void EmitMethodTraits(std::string* out, const MethodDescriptor& method,
                      const std::string& package) {
  const std::string req =
      CppTypeOf(std::string(method.input_type()->full_name()));
  const std::string res =
      CppTypeOf(std::string(method.output_type()->full_name()));
  const std::string req_init =
      MsgInitOf(package, std::string(method.input_type()->full_name()));
  const std::string res_init =
      MsgInitOf(package, std::string(method.output_type()->full_name()));
  const std::string path = MethodPath(
      std::string(method.service()->full_name()), std::string(method.name()));

  *out += "struct " + std::string(method.name()) + "Method {\n";
  *out += "  using ReqType = " + req + ";\n";
  *out += "  using ResType = " + res + ";\n";
  *out += "  static constexpr ::urpc::MethodForm kForm = " +
          std::string(FormOf(&method)) + ";\n";
  *out += "\n";
  *out += "  static const char* service_name() { return \"" +
          std::string(method.service()->full_name()) + "\"; }\n";
  *out += "  static const char* method_name() { return \"" +
          std::string(method.name()) + "\"; }\n";
  *out += "  static const char* path() { return \"" + path + "\"; }\n";
  *out += "\n";
  *out += "  static ReqType* ParseRequest(const char* data, size_t size,\n";
  *out += "                               upb_Arena* arena) {\n";
  *out += "    return " + req + "_parse(data, size, arena);\n";
  *out += "  }\n";
  *out += "  static ResType* ParseResponse(const char* data, size_t size,\n";
  *out += "                                upb_Arena* arena) {\n";
  *out += "    return " + res + "_parse(data, size, arena);\n";
  *out += "  }\n";
  *out += "  static const upb_MiniTable* ReqTable() { return &" + req_init +
          "; }\n";
  *out += "  static const upb_MiniTable* ResTable() { return &" + res_init +
          "; }\n";
  *out += "};\n\n";
}

void EmitInterface(std::string* out, const ServiceDescriptor& service) {
  const std::string iface = "I" + std::string(service.name());

  *out += "// Server-side pure virtual interface for " +
          std::string(service.full_name()) + ".\n";
  *out += "// Inherit and override to implement the service; methods left\n";
  *out += "// unoverridden answer UNIMPLEMENTED (FR-004).\n";
  *out += "class " + iface + " {\n";
  *out += " public:\n";
  *out += "  " + iface + "() = default;\n";
  *out += "  virtual ~" + iface + "() = default;\n";
  *out += "  " + iface + "(const " + iface + "&) = delete;\n";
  *out += "  " + iface + "& operator=(const " + iface + "&) = delete;\n";
  *out += "\n";

  for (int i = 0; i < service.method_count(); ++i) {
    const MethodDescriptor* method = service.method(i);
    const std::string req =
        CppTypeOf(std::string(method->input_type()->full_name()));
    const std::string res =
        CppTypeOf(std::string(method->output_type()->full_name()));
    *out += "  virtual void " + std::string(method->name()) +
            "(::urpc::ServerContext& ctx,\n";
    *out += "                    const " + req + "* request,\n";
    *out += "                    ::urpc::UnaryDone<" + res + "> done) {\n";
    *out += "    (void)ctx;\n";
    *out += "    (void)request;\n";
    *out += "    done(::urpc::Status(::urpc::StatusCode::kUnimplemented,\n";
    *out += "                        \"method not implemented: " +
            std::string(method->name()) + "\"),\n";
    *out += "         nullptr);\n";
    *out += "  }\n";
    if (i + 1 < service.method_count()) *out += "\n";
  }

  *out += "\n";
  *out += "  // Method table: mirrors the .proto service definition\n";
  *out +=
      "  // (registration + contract tests; declaration order preserved).\n";
  *out += "  static constexpr ::urpc::MethodDescriptor kMethods[] = {\n";
  for (int i = 0; i < service.method_count(); ++i) {
    const MethodDescriptor* md = service.method(i);
    *out += "      {\"" + std::string(md->name()) + "\", \"" +
            MethodPath(std::string(service.full_name()),
                       std::string(md->name())) +
            "\", " + std::string(FormOf(md)) + "},\n";
  }
  *out += "  };\n";
  *out += "};\n\n";
}

void EmitProxy(std::string* out, const ServiceDescriptor& service) {
  const std::string proxy = std::string(service.name()) + "Proxy";

  *out += "// Client-side proxy for " + std::string(service.full_name()) +
          " (inherits the\n";
  *out += "// same interface; the typed sync/async members below are the\n";
  *out += "// client calling surface - no connection-management members).\n";
  *out += "class " + proxy + " : public I" + std::string(service.name()) +
          " {\n";
  *out += " public:\n";
  *out += "  explicit " + proxy +
          "(const std::shared_ptr<::urpc::Channel>& channel)\n";
  *out += "      : channel_(channel) {}\n";
  *out += "  explicit " + proxy +
          "(const std::weak_ptr<::urpc::Channel>& channel)\n";
  *out += "      : channel_(channel) {}\n";
  *out += "  explicit " + proxy + "(const ::urpc::Client& client)\n";
  *out += "      : channel_(client.channel_ref()) {}\n";
  *out += "\n";

  for (int i = 0; i < service.method_count(); ++i) {
    const MethodDescriptor* method = service.method(i);
    const std::string m = std::string(method->name());
    const std::string req =
        CppTypeOf(std::string(method->input_type()->full_name()));
    const std::string res =
        CppTypeOf(std::string(method->output_type()->full_name()));
    *out += "  // " + m + ": async form (callback fires exactly once;\n";
    *out += "  // returns the call id, see Channel).\n";
    *out += "  uint64_t " + m + "Async(const " + req + "* request,\n";
    *out += "                         uint64_t timeout_ms,\n";
    *out += "                         std::function<void(::urpc::Result<" + res +
            ">)> done) {\n";
    *out += "    return ::urpc::detail::ProxyCallAsync<" + m +
            "Method>(channel_, request, timeout_ms, std::move(done));\n";
    *out += "  }\n";
    *out += "  // " + m + ": sync convenience form (fast-fails on the urpc\n";
    *out += "  // event-loop thread).\n";
    *out += "  ::urpc::Result<" + res + "> " + m + "(const " + req +
            "* request, uint64_t timeout_ms) {\n";
    *out += "    return ::urpc::detail::ProxyCall<" + m +
            "Method>(channel_, request, timeout_ms);\n";
    *out += "  }\n";
    if (i + 1 < service.method_count()) *out += "\n";
  }
  *out += "\n";
  *out += " private:\n";
  *out += "  std::weak_ptr<::urpc::Channel> channel_;\n";
  *out += "};\n\n";
}

void EmitRegister(std::string* out, const ServiceDescriptor& service) {
  const std::string iface = "I" + std::string(service.name());

  *out += "// Registers every method of `impl` on `server`: one call publishes\n";
  *out += "// the whole service. Fails when the service name is already\n";
  *out += "// registered. Handler exceptions are contained per call\n";
  *out += "// (INTERNAL to that call; the server stays alive).\n";
  *out += "inline ::urpc::Status RegisterService(::urpc::Server& server, " +
          iface + "& impl) {\n";
  *out += "  ::urpc::Status st;\n";
  for (int i = 0; i < service.method_count(); ++i) {
    const MethodDescriptor* method = service.method(i);
    const std::string m = std::string(method->name());
    const std::string req =
        CppTypeOf(std::string(method->input_type()->full_name()));
    const std::string res =
        CppTypeOf(std::string(method->output_type()->full_name()));
    *out += "  st = server.RegisterUnaryFor<" + m + "Method>(\n";
    *out += "      " + m + "Method::service_name(), " + m +
            "Method::method_name(),\n";
    *out += "      [&impl](::urpc::ServerContext& ctx, const " + req +
            "* request,\n";
    *out += "              ::urpc::UnaryDone<" + res + "> done) {\n";
    *out += "        try {\n";
    *out += "          impl." + m + "(ctx, request, done);\n";
    *out += "        } catch (...) {\n";
    *out += "          done(::urpc::Status(::urpc::StatusCode::kInternal,\n";
    *out += "                              \"service handler raised an "
            "exception\"),\n";
    *out += "               nullptr);\n";
    *out += "        }\n";
    *out += "      });\n";
    *out += "  if (!st.ok()) return st;\n";
  }
  *out += "  return ::urpc::Status::Ok();\n";
  *out += "}\n\n";
}

std::string OpenNamespaces(const std::string& package) {
  std::string out = "namespace urpc {\nnamespace gen {\n";
  std::string part;
  for (const char c : package) {
    if (c == '.') {
      out += "namespace " + part + " {\n";
      part.clear();
    } else {
      part += c;
    }
  }
  out += "namespace " + part + " {\n";
  return out;
}

std::string CloseNamespaces(const std::string& package) {
  std::string out;
  std::size_t depth = 3;  // urpc, gen + final package component
  for (const char c : package)
    if (c == '.') ++depth;
  for (std::size_t i = 0; i < depth; ++i) out += "}  // namespace\n";
  return out;
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

    const std::string base = file_name.substr(0, file_name.size() - 6);
    const std::string header_name = base + ".service.h";
    const std::string source_name = base + ".service.cc";

    std::string header;
    header +=
        "// Generated by protoc-gen-urpc (urpc spec "
        "003-typed-service-interface / 004-streaming-rpc).\n";
    header += "// source: " + file_name + "\n";
    header +=
        "// DO NOT EDIT! Deterministic output; regenerate via the standard "
        "build.\n";
    header += "#pragma once\n\n";
    header += "#include <cstddef>\n";
    header += "#include <cstdint>\n";
    header += "#include <functional>\n";
    header += "#include <memory>\n";
    header += "#include <string>\n\n";
    header += "#include \"" + base + ".upb.h\"\n\n";
    header += "#include <urpc/client.h>\n";
    header += "#include <urpc/proxy.h>\n";
    header += "#include <urpc/server.h>\n";
    header += "#include <urpc/service.h>\n\n";
    header += OpenNamespaces(package);
    header += "\n";

    for (int s = 0; s < file->service_count(); ++s) {
      const ServiceDescriptor* service = file->service(s);
      bool has_streaming = false;
      for (int m = 0; m < service->method_count(); ++m) {
        EmitMethodTraits(&header, *service->method(m), package);
        if (IsStreaming(service->method(m))) has_streaming = true;
      }
      // Typed interface/proxy generation for streaming forms arrives with
      // the typed streaming API (spec 004 US4); unary-only services keep
      // the full 003 generation.
      if (!has_streaming) {
        EmitInterface(&header, *service);
        EmitProxy(&header, *service);
        EmitRegister(&header, *service);
      }
    }

    header += CloseNamespaces(package);
    WriteOutput(context, header_name, header);

    std::string source;
    source +=
        "// Generated by protoc-gen-urpc (urpc spec "
        "003-typed-service-interface / 004-streaming-rpc).\n";
    source += "// source: " + file_name + "\n";
    source +=
        "// DO NOT EDIT! Deterministic output; regenerate via the standard "
        "build.\n";
    source += "#include \"" + base + ".service.h\"\n\n";
    source +=
        "// Interface, proxy and registration symbols are header-inline\n";
    source +=
        "// (constexpr method table + inline templates); this translation\n";
    source += "// unit anchors the generated pair in the build graph.\n";
    WriteOutput(context, source_name, source);
    return true;
  }
};

}  // namespace

int main(int argc, char* argv[]) {
  UrpcServiceGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
