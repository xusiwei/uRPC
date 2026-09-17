// Unit tests for the pure code-generation emitters (spec 003/004).
//
// These construct a codegen model BY HAND (no protoc) and assert the
// exact C++ the emitters produce. End-to-end coverage of the
// descriptor->model wiring lives in urpc-api's generated-header
// contract tests (test_service_codegen.cc).

#include <string>

#include "codegen_emit.h"
#include "codegen_model.h"

#include <gtest/gtest.h>

namespace {

using namespace urpc_codegen;

MethodModel UnaryMethod(const std::string& name) {
  MethodModel m;
  m.name = name;
  m.service_full_name = "example.EchoService";
  m.request_type = "example_EchoRequest";
  m.response_type = "example_EchoResponse";
  m.request_init = "example__EchoRequest_msg_init";
  m.response_init = "example__EchoResponse_msg_init";
  m.path = "/example.EchoService/" + name;
  m.form = MethodForm::kUnary;
  return m;
}

ServiceModel FourFormService() {
  ServiceModel s;
  s.name = "StreamService";
  s.full_name = "example.StreamService";

  MethodModel unary;
  unary.name = "Echo";
  unary.service_full_name = s.full_name;
  unary.request_type = "example_EchoRequest";
  unary.response_type = "example_EchoResponse";
  unary.request_init = "example__EchoRequest_msg_init";
  unary.response_init = "example__EchoResponse_msg_init";
  unary.path = "/example.StreamService/Echo";
  unary.form = MethodForm::kUnary;
  s.methods.push_back(unary);

  MethodModel ss = unary;
  ss.name = "Download";
  ss.request_type = "example_DownloadRequest";
  ss.response_type = "example_Chunk";
  ss.response_init = "example__Chunk_msg_init";
  ss.path = "/example.StreamService/Download";
  ss.form = MethodForm::kServerStreaming;
  s.methods.push_back(ss);

  MethodModel cs = unary;
  cs.name = "Upload";
  cs.request_type = "example_Chunk";
  cs.response_type = "example_UploadResponse";
  cs.request_init = "example__Chunk_msg_init";
  cs.response_init = "example__UploadResponse_msg_init";
  cs.path = "/example.StreamService/Upload";
  cs.form = MethodForm::kClientStreaming;
  s.methods.push_back(cs);

  MethodModel bidi = unary;
  bidi.name = "Chat";
  bidi.request_type = "example_ChatMsg";
  bidi.response_type = "example_ChatMsg";
  bidi.request_init = "example__ChatMsg_msg_init";
  bidi.response_init = "example__ChatMsg_msg_init";
  bidi.path = "/example.StreamService/Chat";
  bidi.form = MethodForm::kBidi;
  s.methods.push_back(bidi);

  return s;
}

FileModel FourFormFile() {
  FileModel f;
  f.package = "example";
  f.source_file = "streaming.proto";
  f.services.push_back(FourFormService());
  return f;
}

std::string EmitInterfaceText(const ServiceModel& s) {
  std::string out;
  urpc_codegen::EmitInterface(&out, s);
  return out;
}

std::string EmitProxyText(const ServiceModel& s) {
  std::string out;
  urpc_codegen::EmitProxy(&out, s);
  return out;
}

std::string EmitRegisterText(const ServiceModel& s) {
  std::string out;
  urpc_codegen::EmitRegister(&out, s);
  return out;
}

std::size_t CountOf(const std::string& text, const std::string& needle) {
  std::size_t count = 0;
  for (std::size_t pos = 0; (pos = text.find(needle, pos)) != std::string::npos;
       pos += needle.size()) {
    ++count;
  }
  return count;
}

// ---- string helpers ----------------------------------------------------------

TEST(CodeGenHelpers, CppTypeOfReplacesDots) {
  EXPECT_EQ("example_EchoRequest", CppTypeOf("example.EchoRequest"));
  EXPECT_EQ("a_b_c", CppTypeOf("a.b.c"));
}

TEST(CodeGenHelpers, MsgInitOfStripsPackagePrefix) {
  EXPECT_EQ("example__EchoRequest_msg_init",
            MsgInitOf("example", "example.EchoRequest"));
  EXPECT_EQ("example__stream_Chunk_msg_init",
            MsgInitOf("example", "example.stream.Chunk"));
}

TEST(CodeGenHelpers, MethodPathJoinsParts) {
  EXPECT_EQ("/example.EchoService/Echo",
            MethodPath("example.EchoService", "Echo"));
}

TEST(CodeGenHelpers, FormNameIsQualified) {
  EXPECT_STREQ("::urpc::MethodForm::kUnary", FormName(MethodForm::kUnary));
  EXPECT_STREQ("::urpc::MethodForm::kServerStreaming",
               FormName(MethodForm::kServerStreaming));
  EXPECT_STREQ("::urpc::MethodForm::kClientStreaming",
               FormName(MethodForm::kClientStreaming));
  EXPECT_STREQ("::urpc::MethodForm::kBidi", FormName(MethodForm::kBidi));
}

// ---- namespace emitters --------------------------------------------------------

TEST(CodeGenNamespaces, DepthIsTwoPlusComponentCount) {
  EXPECT_EQ("namespace urpc {\nnamespace gen {\nnamespace example {\n",
            OpenNamespaces("example"));
  EXPECT_EQ("}  // namespace\n}  // namespace\n}  // namespace\n",
            CloseNamespaces("example"));
}

TEST(CodeGenNamespaces, MultiComponentPackage) {
  const std::string open = OpenNamespaces("a.b.c");
  EXPECT_EQ(5, CountOf(open, "namespace "));
  EXPECT_EQ(5, CountOf(CloseNamespaces("a.b.c"), "}  // namespace"));
}

// ---- trait emission ------------------------------------------------------------

TEST(CodeGenTraits, ContainsFormAndSymbolsPerForm) {
  const auto svc = FourFormService();
  for (const auto& m : svc.methods) {
    const std::string text = EmitTraits(m);
    EXPECT_NE(std::string::npos,
              text.find("static constexpr ::urpc::MethodForm kForm = " +
                        std::string(FormName(m.form)) + ";")) << m.name;
    EXPECT_NE(std::string::npos,
              text.find("return " + m.request_type + "_parse(data, size, arena);")) << m.name;
    EXPECT_NE(std::string::npos, text.find("return &" + m.request_init + ";")) << m.name;
    EXPECT_NE(std::string::npos, text.find("return &" + m.response_init + ";")) << m.name;
  }
}

// ---- interface emission ----------------------------------------------------------

TEST(CodeGenInterface, UnaryDefaultAnswersUnimplemented) {
  const auto text = EmitInterfaceText(FourFormService());
  EXPECT_NE(std::string::npos,
            text.find("done(::urpc::Status(::urpc::StatusCode::kUnimplemented"));
}

TEST(CodeGenInterface, StreamingFormsHaveTypedSignatures) {
  const auto text = EmitHeader(FourFormFile());
  EXPECT_NE(std::string::npos,
            text.find("::urpc::ServerWriter<example_Chunk>& writer"));
  EXPECT_NE(std::string::npos,
            text.find("::urpc::ServerReader<example_Chunk>& reader"));
  EXPECT_NE(std::string::npos,
            text.find("::urpc::ServerReaderWriter<example_ChatMsg"));
  EXPECT_NE(std::string::npos, text.find("method not implemented: Echo"));
  EXPECT_NE(std::string::npos, text.find("method not implemented: Upload"));
  EXPECT_NE(std::string::npos, text.find("method not implemented: Chat"));
}

TEST(CodeGenInterface, MethodTableMirrorsDeclarationOrderAndForms) {
  const auto text = EmitInterfaceText(FourFormService());
  const std::size_t echo = text.find("{\"Echo\", \"/example.StreamService/Echo\", "
                                     "::urpc::MethodForm::kUnary}");
  const std::size_t download = text.find("kServerStreaming}");
  const std::size_t upload = text.find("kClientStreaming}");
  const std::size_t chat = text.find("kBidi}");
  ASSERT_NE(std::string::npos, echo);
  ASSERT_NE(std::string::npos, download);
  ASSERT_NE(std::string::npos, upload);
  ASSERT_NE(std::string::npos, chat);
  EXPECT_LT(echo, download);
  EXPECT_LT(download, upload);
  EXPECT_LT(upload, chat);
}

// ---- proxy emission ---------------------------------------------------------------

TEST(CodeGenProxy, StreamingEntriesUseLockAndOpeners) {
  const auto text = EmitProxyText(FourFormService());
  EXPECT_NE(std::string::npos,
            text.find("OpenClientReader<DownloadMethod>(channel_.lock()"));
  EXPECT_NE(std::string::npos,
            text.find("OpenClientWriter<UploadMethod>(channel_.lock()"));
  EXPECT_NE(std::string::npos,
            text.find("OpenClientReaderWriter<ChatMethod>(channel_.lock()"));
  EXPECT_NE(std::string::npos,
            text.find("ProxyCallAsync<EchoMethod>(channel_, request"));
}

// ---- registration emission ----------------------------------------------------------

TEST(CodeGenRegister, DispatchesPerFormWithContainment) {
  const auto text = EmitRegisterText(FourFormService());
  EXPECT_NE(std::string::npos,
            text.find("RegisterUnaryFor<EchoMethod>("));
  EXPECT_NE(std::string::npos,
            text.find("RegisterServerStreamingFor<DownloadMethod>("));
  EXPECT_NE(std::string::npos,
            text.find("RegisterClientStreamingFor<UploadMethod>("));
  EXPECT_NE(std::string::npos, text.find("RegisterBidiFor<ChatMethod>("));
  EXPECT_NE(std::string::npos, text.find("done(::urpc::Status("));
  EXPECT_NE(std::string::npos, text.find("writer.Finish(::urpc::Status("));
  EXPECT_NE(std::string::npos, text.find("stream.Finish(::urpc::Status("));
}

// regression (003): the registration wrapper must NOT move `done` out of
// the handler - a catch block calling a moved-from std::function aborts.
TEST(CodeGenRegister, UnaryHandlerReceivesDoneByValue) {
  // only the unary registration should be in scope
  auto svc = FourFormService();
  svc.methods.erase(
      svc.methods.begin() + 1, svc.methods.end());  // keep Echo only
  const auto text = EmitRegisterText(svc);
  EXPECT_EQ(0, CountOf(text, "std::move(done)"));
  EXPECT_NE(std::string::npos, text.find("impl.Echo(ctx, request, done);"));
}

// ---- header/source emission ---------------------------------------------------------

TEST(CodeGenHeader, BracesBalanceAndStructureComplete) {
  const auto header = EmitHeader(FourFormFile());
  EXPECT_EQ(CountOf(header, "{"), CountOf(header, "}"));
  EXPECT_NE(std::string::npos, header.find("#include <urpc/stream.h>"));
  EXPECT_NE(std::string::npos, header.find("#include <urpc/service.h>"));
  EXPECT_NE(std::string::npos, header.find("#include \"streaming.upb.h\""));
  EXPECT_NE(std::string::npos, header.find("DO NOT EDIT"));
}

TEST(CodeGenSource, AnchorsGeneratedPair) {
  const auto text = EmitSource(FourFormFile());
  EXPECT_NE(std::string::npos, text.find("#include \"streaming.service.h\""));
  EXPECT_NE(std::string::npos, text.find("anchors the generated pair"));
}

TEST(CodeGenHeader, DeterministicOutput) {
  EXPECT_EQ(EmitHeader(FourFormFile()), EmitHeader(FourFormFile()));
}

// ---- shutdown/backpressure regression ----------------------------------------------

TEST(CodeGenRegister, ClientStreamingPassesDoneByValueForContainment) {
  const auto text = EmitRegisterText(FourFormService());
  EXPECT_NE(std::string::npos, text.find("impl.Upload(ctx, reader, std::move(done));"));
}

}  // namespace
