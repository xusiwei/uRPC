#include "codegen_model.h"

namespace urpc_codegen {

std::string ReplaceAll(std::string s, char from, char to) {
  for (char& c : s)
    if (c == from) c = to;
  return s;
}

std::string CppTypeOf(const std::string& full_name) {
  return ReplaceAll(full_name, '.', '_');
}

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

const char* FormName(MethodForm form) {
  switch (form) {
    case MethodForm::kServerStreaming: return "::urpc::MethodForm::kServerStreaming";
    case MethodForm::kClientStreaming: return "::urpc::MethodForm::kClientStreaming";
    case MethodForm::kBidi: return "::urpc::MethodForm::kBidi";
    case MethodForm::kUnary: break;
  }
  return "::urpc::MethodForm::kUnary";
}

}  // namespace urpc_codegen
