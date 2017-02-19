#include "vast/detail/assert.hpp"
#include "vast/error.hpp"

namespace vast {
namespace {

const char* descriptions[] = {
  "unspecified",
  "filesystem_error",
  "type_clash",
  "unsupported_operator",
  "parse_error",
  "print_error",
  "convert_error",
  "invalid_query",
  "format_error",
  "end_of_input",
  "version_error",
  "syntax_error",
};

} // namespace <anonymous>

const char* to_string(ec x) {
  auto index = static_cast<size_t>(x) - 1;
  VAST_ASSERT(index < sizeof(descriptions));
  return descriptions[index];
}

} // namespace vast
