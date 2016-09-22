#ifndef VAST_CONCEPT_PRINTABLE_CORE_LITERAL_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_LITERAL_HPP

#include <cstddef>
#include <string>
#include <type_traits>

#include "vast/concept/printable/string/literal.hpp"

namespace vast {

inline auto operator"" _P(char c) {
  return literal_printer{c};
}

inline auto operator"" _P(char const* str) {
  return literal_printer{str};
}

inline auto operator"" _P(char const* str, size_t size) {
  return literal_printer{{str, size}};
}

inline auto operator"" _P(unsigned long long int x) {
  return literal_printer{x};
}

inline auto operator"" _P(long double x) {
  return literal_printer{x};
}

} // namespace vast

#endif
