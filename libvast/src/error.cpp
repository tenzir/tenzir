#include "vast/error.hpp"

namespace vast {

namespace {

const char* descriptions[] = {
  "<unspecified>",
  "filesystem_error",
  "type_clash",
  "unsupported_operator",
  "parse_error",
  "print_error",
  "invalid_query",
};

} // namespace <anonymous>

const char* to_string(ec x) {
  auto index = static_cast<size_t>(x);
  if (index > static_cast<size_t>(ec::invalid_query))
    return "<unknown>";
  return descriptions[index];
}

} // namespace vast
