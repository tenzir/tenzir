#include "tenzir/detail/strip_leading_indentation.hpp"

#include <tenzir/generator.hpp>

namespace tenzir::detail {

auto strip_leading_indentation(std::string code) -> std::string {
  /// Yields each line, including the trailing newline.
  auto each_line = [](const std::string& code) -> generator<std::string_view> {
    auto left = 0ull;
    auto right = code.find('\n');
    while (right != std::string::npos) {
      co_yield std::string_view{code.begin() + left, code.begin() + right + 1};
      left = right + 1;
      right = code.find('\n', left);
    }
  };

  auto indentation = std::string_view{};
  for (auto line : each_line(code)) {
    if (auto x = line.find_first_not_of(" \t\n"); x != std::string::npos) {
      indentation = line.substr(0, x);
      break;
    }
  }
  if (indentation.empty())
    return code;
  auto stripped_code = std::string{};
  for (auto line : each_line(code)) {
    if (line.starts_with(indentation))
      line.remove_prefix(indentation.size());
    stripped_code += line;
  }
  return stripped_code;
}

} // namespace tenzir::detail
