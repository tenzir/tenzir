#ifndef VAST_CONCEPT_PRINTABLE_STREAM_H
#define VAST_CONCEPT_PRINTABLE_STREAM_H

#include <ostream>
#include <type_traits>

#include "vast/concept/printable/print.h"

namespace vast {

template <typename Char, typename Traits, typename T>
auto operator<<(std::basic_ostream<Char, Traits>& out, T const& x)
  -> std::enable_if_t<
       is_printable<std::ostreambuf_iterator<Char>, T>::value, decltype(out)
     > {
  using vast::print; // enable ADL
  if (!print(std::ostreambuf_iterator<Char>{out}, x))
    out.setstate(std::ios_base::failbit);
  return out;
}

} // namespace vast

#endif
