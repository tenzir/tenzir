#include <regex>

#include "vast/util/endpoint.h"

namespace vast {
namespace util {

bool parse_endpoint(std::string const& str, std::string& host, uint16_t& port) {
  // TODO: parse endpoint properly.
  // static constexpr std::string v4 = "[0-9.]+";
  // static constexpr std::string v6 = "[a-fA-F0-9:]+";
  // static constexpr std::string name = "[a-fA-F0-9_.-]+";
  // static constexpr std::string port = "\\d{1,5}";
  // static auto const rx =
  //  std::regex{'('v4 + '|' + name + ")(" + port + ")?|" +
  //             "\\[(" + v6 + ")\\]:(" + port + ")|" +
  //             '(' + v6 + ')';
  static auto const rx
    = std::regex{"(\\[([a-fA-F0-9:]+)\\]|[0-9.]+)?(:(\\d{1,5}))?"};
  std::smatch match;
  std::regex_match(str.cbegin(), str.cend(), match, rx);
  if (match.empty())
    return false;
  if (match.length(2) > 0)
    host = match[2];
  else if (match.length(1) > 0)
    host = match[1];
  if (match.length(4) > 0)
    port = static_cast<uint16_t>(std::stoul(match[4]));
  return true;
}

} // namespace util
} // namespace vast
