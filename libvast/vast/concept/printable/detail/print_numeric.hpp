#ifndef VAST_CONCEPT_PRINTABLE_DETAIL_PRINT_NUMERIC_HPP
#define VAST_CONCEPT_PRINTABLE_DETAIL_PRINT_NUMERIC_HPP

#include <algorithm>
#include <limits>
#include <type_traits>

#include "vast/util/coding.hpp"

namespace vast {
namespace detail {

template <typename Iterator, typename T>
bool print_numeric(Iterator& out, T x) {
  static_assert(std::is_integral<T>{}, "T must be an integral type");
  if (x == 0) {
    *out++ = '0';
    return true;
  }
  char buf[std::numeric_limits<T>::digits10 + 1];
  auto p = buf;
  while (x > 0) {
    *p++ = util::byte_to_char(x % 10);
    x /= 10;
  }
  out = std::reverse_copy(buf, p, out);
  return true;
}

} // namespace detail
} // namespace vast

#endif
