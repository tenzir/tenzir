#include "tenzir/detail/strip_leading_indentation.hpp"

#include <tenzir/generator.hpp>

namespace tenzir::detail {

auto strip_leading_indentation(std::string&& code) -> std::string {
  /// Yields each line, including the trailing newline.
  auto each_line = [](std::string_view code) -> generator<std::string_view> {
    auto left = 0ull;
    auto right = code.find('\n');
    while (right != std::string_view::npos) {
      co_yield std::string_view{code.begin() + left, code.begin() + right + 1};
      left = right + 1;
      right = code.find('\n', left);
    }
  };
  auto common_prefix = [](std::string_view lhs, std::string_view rhs) {
    return std::string_view{
      lhs.begin(),
      std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end()).first};
  };

  auto indentation = std::string_view{};
  auto start = true;
  for (auto line : each_line(code)) {
    if (auto x = line.find_first_not_of(" \t\n"); x != std::string::npos) {
      auto indent = line.substr(0, x);
      if (start) {
        indentation = indent;
        start = false;
      } else {
        indentation = common_prefix(indentation, indent);
      }
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
