#include "codegen_emit.h"

namespace urpc_codegen {

std::string EmitTraits(const MethodModel& m) {
  std::string out;
  out += "struct " + m.name + "Method {\n";
  out += "  using ReqType = " + m.request_type + ";\n";
  out += "  using ResType = " + m.response_type + ";\n";
  out += "  static constexpr ::urpc::MethodForm kForm = " +
         std::string(FormName(m.form)) + ";\n";
  out += "\n";
  out += "  static const char* service_name() { return \"" +
         m.service_full_name + "\"; }\n";
  out += "  static const char* method_name() { return \"" + m.name + "\"; }\n";
  out += "  static const char* path() { return \"" + m.path + "\"; }\n";
  out += "\n";
  out += "  static ReqType* ParseRequest(const char* data, size_t size,\n";
  out += "                               upb_Arena* arena) {\n";
  out += "    return " + m.request_type + "_parse(data, size, arena);\n";
  out += "  }\n";
  out += "  static ResType* ParseResponse(const char* data, size_t size,\n";
  out += "                                upb_Arena* arena) {\n";
  out += "    return " + m.response_type + "_parse(data, size, arena);\n";
  out += "  }\n";
  out += "  static const upb_MiniTable* ReqTable() { return &" + m.request_init +
         "; }\n";
  out += "  static const upb_MiniTable* ResTable() { return &" + m.response_init +
         "; }\n";
  out += "};\n\n";
  return out;
}

namespace {

// Per-form server-side interface method declaration with the
// UNIMPLEMENTED default body (FR-004).
void EmitInterfaceMethod(std::string* out, const MethodModel& m) {
  const std::string& name = m.name;
  if (m.form == MethodForm::kUnary) {
    *out += "  virtual void " + name + "(::urpc::ServerContext& ctx,\n";
    *out += "                    const " + m.request_type + "* request,\n";
    *out += "                    ::urpc::UnaryDone<" + m.response_type +
            "> done) {\n";
    *out += "    (void)ctx;\n";
    *out += "    (void)request;\n";
    *out += "    done(::urpc::Status(::urpc::StatusCode::kUnimplemented,\n";
    *out += "                        \"method not implemented: " + name +
            "\"),\n";
    *out += "         nullptr);\n";
    *out += "  }\n";
  } else if (m.form == MethodForm::kServerStreaming) {
    *out += "  virtual void " + name + "(::urpc::ServerContext& ctx,\n";
    *out += "                    const " + m.request_type + "* request,\n";
    *out += "                    ::urpc::ServerWriter<" + m.response_type +
            ">& writer) {\n";
    *out += "    (void)ctx;\n";
    *out += "    (void)request;\n";
    *out += "    writer.Finish(::urpc::Status(\n";
    *out += "        ::urpc::StatusCode::kUnimplemented,\n";
    *out += "        \"method not implemented: " + name + "\"));\n";
    *out += "  }\n";
  } else if (m.form == MethodForm::kClientStreaming) {
    *out += "  virtual void " + name + "(::urpc::ServerContext& ctx,\n";
    *out += "                    ::urpc::ServerReader<" + m.request_type +
            ">& reader,\n";
    *out += "                    ::urpc::UnaryDone<" + m.response_type +
            "> done) {\n";
    *out += "    (void)ctx;\n";
    *out += "    (void)reader;\n";
    *out += "    done(::urpc::Status(::urpc::StatusCode::kUnimplemented,\n";
    *out += "                        \"method not implemented: " + name +
            "\"),\n";
    *out += "         nullptr);\n";
    *out += "  }\n";
  } else {
    *out += "  virtual void " + name + "(::urpc::ServerContext& ctx,\n";
    *out += "                    ::urpc::ServerReaderWriter<" + m.request_type +
            ", " + m.response_type + ">& stream) {\n";
    *out += "    (void)ctx;\n";
    *out += "    (void)stream;\n";
    *out += "    stream.Finish(::urpc::Status(\n";
    *out += "        ::urpc::StatusCode::kUnimplemented,\n";
    *out += "        \"method not implemented: " + name + "\"));\n";
    *out += "  }\n";
  }
}

void EmitProxyMethod(std::string* out, const MethodModel& m) {
  const std::string& name = m.name;
  if (m.form == MethodForm::kUnary) {
    *out += "  // " + name + ": async form (callback fires exactly once;\n";
    *out += "  // returns the call id, see Channel).\n";
    *out += "  uint64_t " + name + "Async(const " + m.request_type +
            "* request,\n";
    *out += "                         uint64_t timeout_ms,\n";
    *out += "                         std::function<void(::urpc::Result<" +
            m.response_type + ">)> done) {\n";
    *out += "    return ::urpc::detail::ProxyCallAsync<" + name +
            "Method>(channel_, request, timeout_ms, std::move(done));\n";
    *out += "  }\n";
    *out += "  // " + name + ": sync convenience form (fast-fails on the urpc\n";
    *out += "  // event-loop thread).\n";
    *out += "  ::urpc::Result<" + m.response_type + "> " + name + "(const " +
            m.request_type + "* request, uint64_t timeout_ms) {\n";
    *out += "    return ::urpc::detail::ProxyCall<" + name +
            "Method>(channel_, request, timeout_ms);\n";
    *out += "  }\n";
  } else if (m.form == MethodForm::kServerStreaming) {
    *out += "  // " + name + ": server streaming (single request; read the\n";
    *out += "  // response sequence from the returned reader).\n";
    *out += "  std::unique_ptr<::urpc::ClientReader<" + m.response_type +
            ">> " + name + "(const " + m.request_type +
            "* request, uint64_t timeout_ms) {\n";
    *out += "    return ::urpc::OpenClientReader<" + name +
            "Method>(channel_.lock(),\n";
    *out += "        " + name + "Method::service_name(), " + name +
            "Method::method_name(), request, timeout_ms);\n";
    *out += "  }\n";
  } else if (m.form == MethodForm::kClientStreaming) {
    *out += "  // " + name + ": client streaming (write requests, then Finish\n";
    *out += "  // for the single response).\n";
    *out += "  std::unique_ptr<::urpc::ClientWriter<" + m.request_type + ", " +
            m.response_type + ">> " + name + "(uint64_t timeout_ms) {\n";
    *out += "    return ::urpc::OpenClientWriter<" + name +
            "Method>(channel_.lock(),\n";
    *out += "        " + name + "Method::service_name(), " + name +
            "Method::method_name(), timeout_ms);\n";
    *out += "  }\n";
  } else {
    *out += "  // " + name + ": bidirectional streaming.\n";
    *out += "  std::unique_ptr<::urpc::ClientReaderWriter<" + m.request_type +
            ", " + m.response_type + ">> " + name + "(uint64_t timeout_ms) {\n";
    *out += "    return ::urpc::OpenClientReaderWriter<" + name +
            "Method>(channel_.lock(),\n";
    *out += "        " + name + "Method::service_name(), " + name +
            "Method::method_name(), timeout_ms);\n";
    *out += "  }\n";
  }
}

}  // namespace

namespace {

// Per-form registration: exceptions contained per call; the wrappers pass
// `done` BY VALUE so a catch block can still answer after a throw
// (regression: never std::move the done callback out of the handler).
void EmitRegisterCall(std::string* out, const MethodModel& m) {
  const std::string& name = m.name;
  *out += "  st = server.";
  if (m.form == MethodForm::kUnary) {
    *out += "RegisterUnaryFor<" + name + "Method>(\n";
    *out += "      " + name + "Method::service_name(), " + name +
            "Method::method_name(),\n";
    *out += "      [&impl](::urpc::ServerContext& ctx, const " +
            m.request_type + "* request,\n";
    *out += "              ::urpc::UnaryDone<" + m.response_type + "> done) {\n";
    *out += "        try {\n";
    *out += "          impl." + name + "(ctx, request, done);\n";
    *out += "        } catch (...) {\n";
    *out += "          done(::urpc::Status(::urpc::StatusCode::kInternal,\n";
    *out += "                              \"service handler raised an "
            "exception\"),\n";
    *out += "               nullptr);\n";
    *out += "        }\n";
    *out += "      });\n";
  } else if (m.form == MethodForm::kServerStreaming) {
    *out += "RegisterServerStreamingFor<" + name + "Method>(\n";
    *out += "      " + name + "Method::service_name(), " + name +
            "Method::method_name(),\n";
    *out += "      [&impl](::urpc::ServerContext& ctx, const " +
            m.request_type + "* request,\n";
    *out += "              ::urpc::ServerWriter<" + m.response_type +
            ">& writer) {\n";
    *out += "        try {\n";
    *out += "          impl." + name + "(ctx, request, writer);\n";
    *out += "        } catch (...) {\n";
    *out += "          writer.Finish(::urpc::Status(\n";
    *out += "              ::urpc::StatusCode::kInternal,\n";
    *out += "              \"service handler raised an exception\"));\n";
    *out += "        }\n";
    *out += "      });\n";
  } else if (m.form == MethodForm::kClientStreaming) {
    *out += "RegisterClientStreamingFor<" + name + "Method>(\n";
    *out += "      " + name + "Method::service_name(), " + name +
            "Method::method_name(),\n";
    *out += "      [&impl](::urpc::ServerContext& ctx,\n";
    *out += "              ::urpc::ServerReader<" + m.request_type + ">& reader,\n";
    *out += "              ::urpc::UnaryDone<" + m.response_type + "> done) {\n";
    *out += "        try {\n";
    *out += "          impl." + name + "(ctx, reader, std::move(done));\n";
    *out += "        } catch (...) {\n";
    *out += "          done(::urpc::Status(::urpc::StatusCode::kInternal,\n";
    *out += "                              \"service handler raised an "
            "exception\"),\n";
    *out += "               nullptr);\n";
    *out += "        }\n";
    *out += "      });\n";
  } else {
    *out += "RegisterBidiFor<" + name + "Method>(\n";
    *out += "      " + name + "Method::service_name(), " + name +
            "Method::method_name(),\n";
    *out += "      [&impl](::urpc::ServerContext& ctx,\n";
    *out += "              ::urpc::ServerReaderWriter<" + m.request_type + ", " +
            m.response_type + ">& stream) {\n";
    *out += "        try {\n";
    *out += "          impl." + name + "(ctx, stream);\n";
    *out += "        } catch (...) {\n";
    *out += "          stream.Finish(::urpc::Status(\n";
    *out += "              ::urpc::StatusCode::kInternal,\n";
    *out += "              \"service handler raised an exception\"));\n";
    *out += "        }\n";
    *out += "      });\n";
  }
  *out += "  if (!st.ok()) return st;\n";
}

}  // namespace

void EmitInterface(std::string* out, const ServiceModel& s) {
  const std::string iface = "I" + s.name;

  *out += "// Server-side pure virtual interface for " + s.full_name + ".\n";
  *out += "// Inherit and override to implement the service; methods left\n";
  *out += "// unoverridden answer UNIMPLEMENTED (FR-004).\n";
  *out += "class " + iface + " {\n";
  *out += " public:\n";
  *out += "  " + iface + "() = default;\n";
  *out += "  virtual ~" + iface + "() = default;\n";
  *out += "  " + iface + "(const " + iface + "&) = delete;\n";
  *out += "  " + iface + "& operator=(const " + iface + "&) = delete;\n";
  *out += "\n";

  for (size_t i = 0; i < s.methods.size(); ++i) {
    EmitInterfaceMethod(out, s.methods[i]);
    if (i + 1 < s.methods.size()) *out += "\n";
  }

  *out += "\n";
  *out += "  // Method table: mirrors the .proto service definition\n";
  *out +=
      "  // (registration + contract tests; declaration order preserved).\n";
  *out += "  static constexpr ::urpc::MethodDescriptor kMethods[] = {\n";
  for (const auto& md : s.methods) {
    *out += "      {\"" + md.name + "\", \"" + md.path + "\", " +
            std::string(FormName(md.form)) + "},\n";
  }
  *out += "  };\n";
  *out += "};\n\n";
}

void EmitProxy(std::string* out, const ServiceModel& s) {
  const std::string proxy = s.name + "Proxy";

  *out += "// Client-side proxy for " + s.full_name +
          " (inherits the\n";
  *out += "// same interface; the typed members below are the client calling\n";
  *out += "// surface - no connection-management members).\n";
  *out += "class " + proxy + " : public I" + s.name + " {\n";
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

  for (size_t i = 0; i < s.methods.size(); ++i) {
    EmitProxyMethod(out, s.methods[i]);
    if (i + 1 < s.methods.size()) *out += "\n";
  }
  *out += "\n";
  *out += " private:\n";
  *out += "  std::weak_ptr<::urpc::Channel> channel_;\n";
  *out += "};\n\n";
}

void EmitRegister(std::string* out, const ServiceModel& s) {
  *out += "// Registers every method of `impl` on `server`: one call publishes\n";
  *out += "// the whole service. Fails when the service name is already\n";
  *out += "// registered. Handler exceptions are contained per call\n";
  *out += "// (INTERNAL to that call; the server stays alive).\n";
  *out += "inline ::urpc::Status RegisterService(::urpc::Server& server, I" +
          s.name + "& impl) {\n";
  *out += "  ::urpc::Status st;\n";
  for (const auto& m : s.methods) {
    const std::string& name = m.name;
    *out += "  st = server.";
    if (m.form == MethodForm::kUnary) {
      *out += "RegisterUnaryFor<" + name + "Method>(\n";
      *out += "      " + name + "Method::service_name(), " + name +
              "Method::method_name(),\n";
      *out += "      [&impl](::urpc::ServerContext& ctx, const " +
              m.request_type + "* request,\n";
      *out += "              ::urpc::UnaryDone<" + m.response_type + "> done) {\n";
      *out += "        try {\n";
      *out += "          impl." + name + "(ctx, request, done);\n";
      *out += "        } catch (...) {\n";
      *out += "          done(::urpc::Status(::urpc::StatusCode::kInternal,\n";
      *out += "                              \"service handler raised an "
              "exception\"),\n";
      *out += "               nullptr);\n";
      *out += "        }\n";
      *out += "      });\n";
    } else if (m.form == MethodForm::kServerStreaming) {
      *out += "RegisterServerStreamingFor<" + name + "Method>(\n";
      *out += "      " + name + "Method::service_name(), " + name +
              "Method::method_name(),\n";
      *out += "      [&impl](::urpc::ServerContext& ctx, const " +
              m.request_type + "* request,\n";
      *out += "              ::urpc::ServerWriter<" + m.response_type +
              ">& writer) {\n";
      *out += "        try {\n";
      *out += "          impl." + name + "(ctx, request, writer);\n";
      *out += "        } catch (...) {\n";
      *out += "          writer.Finish(::urpc::Status(\n";
      *out += "              ::urpc::StatusCode::kInternal,\n";
      *out += "              \"service handler raised an exception\"));\n";
      *out += "        }\n";
      *out += "      });\n";
    } else if (m.form == MethodForm::kClientStreaming) {
      *out += "RegisterClientStreamingFor<" + name + "Method>(\n";
      *out += "      " + name + "Method::service_name(), " + name +
              "Method::method_name(),\n";
      *out += "      [&impl](::urpc::ServerContext& ctx,\n";
      *out += "              ::urpc::ServerReader<" + m.request_type +
              ">& reader,\n";
      *out += "              ::urpc::UnaryDone<" + m.response_type + "> done) {\n";
      *out += "        try {\n";
      *out += "          impl." + name + "(ctx, reader, std::move(done));\n";
      *out += "        } catch (...) {\n";
      *out += "          done(::urpc::Status(::urpc::StatusCode::kInternal,\n";
      *out += "                              \"service handler raised an "
              "exception\"),\n";
      *out += "               nullptr);\n";
      *out += "        }\n";
      *out += "      });\n";
    } else {
      *out += "RegisterBidiFor<" + name + "Method>(\n";
      *out += "      " + name + "Method::service_name(), " + name +
              "Method::method_name(),\n";
      *out += "      [&impl](::urpc::ServerContext& ctx,\n";
      *out += "              ::urpc::ServerReaderWriter<" + m.request_type + ", " +
              m.response_type + ">& stream) {\n";
      *out += "        try {\n";
      *out += "          impl." + name + "(ctx, stream);\n";
      *out += "        } catch (...) {\n";
      *out += "          stream.Finish(::urpc::Status(\n";
      *out += "              ::urpc::StatusCode::kInternal,\n";
      *out += "              \"service handler raised an exception\"));\n";
      *out += "        }\n";
      *out += "      });\n";
    }
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

void EmitTraitsInto(std::string* out, const ServiceModel& s) {
  for (const auto& m : s.methods) *out += EmitTraits(m);
}

std::string EmitHeader(const FileModel& f) {
  const std::string base =
      f.source_file.substr(0, f.source_file.size() - 6);  // strip ".proto"

  std::string header;
  header +=
      "// Generated by protoc-gen-urpc (urpc spec "
      "003-typed-service-interface / 004-streaming-rpc).\n";
  header += "// source: " + f.source_file + "\n";
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
  header += "#include <urpc/service.h>\n";
  header += "#include <urpc/stream.h>\n";
  header += "\n";
  header += OpenNamespaces(f.package);
  header += "\n";

  for (const auto& s : f.services) {
    EmitTraitsInto(&header, s);
    EmitInterface(&header, s);
    EmitProxy(&header, s);
    EmitRegister(&header, s);
  }

  header += CloseNamespaces(f.package);
  return header;
}

std::string EmitSource(const FileModel& f) {
  const std::string base =
      f.source_file.substr(0, f.source_file.size() - 6);  // strip ".proto"

  std::string source;
  source +=
      "// Generated by protoc-gen-urpc (urpc spec "
      "003-typed-service-interface / 004-streaming-rpc).\n";
  source += "// source: " + f.source_file + "\n";
  source +=
      "// DO NOT EDIT! Deterministic output; regenerate via the standard "
      "build.\n";
  source += "#include \"" + base + ".service.h\"\n\n";
  source +=
      "// Interface, proxy and registration symbols are header-inline\n";
  source +=
      "// (constexpr method table + inline templates); this translation\n";
  source += "// unit anchors the generated pair in the build graph.\n";
  return source;
}

}  // namespace urpc_codegen
