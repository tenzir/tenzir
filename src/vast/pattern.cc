#include <regex>

#include "vast/pattern.h"

namespace vast {

pattern pattern::glob(std::string const& str) {
  auto rx = std::regex_replace(str, std::regex("\\."), "\\.");
  rx = std::regex_replace(rx, std::regex("\\*"), ".*");
  return pattern{std::regex_replace(rx, std::regex("\\?"), ".")};
}

pattern::pattern(std::string str) : str_(std::move(str)) {
}

bool operator==(pattern const& lhs, pattern const& rhs) {
  return lhs.str_ == rhs.str_;
}

bool operator<(pattern const& lhs, pattern const& rhs) {
  return lhs.str_ < rhs.str_;
}

bool pattern::match(std::string const& str) const {
  return std::regex_match(str.begin(), str.end(), std::regex{str_});
}

bool pattern::search(std::string const& str) const {
  return std::regex_search(str.begin(), str.end(), std::regex{str_});
}

} // namespace vast
