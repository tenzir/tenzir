#ifndef VAST_CONCEPT_PRINTABLE_VAST_PATTERN_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_PATTERN_HPP

#include "vast/access.hpp"
#include "vast/pattern.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"

namespace vast {

template <>
struct access::printer<vast::pattern>
  : vast::printer<access::printer<vast::pattern>> {
  using attribute = pattern;

  template <typename Iterator>
  bool print(Iterator& out, pattern const& p) const {
    using namespace printers;
    return any.print(out, '/') && str.print(out, p.str_) && any.print(out, '/');
  }
};

template <>
struct printer_registry<pattern> {
  using type = access::printer<pattern>;
};

} // namespace vast

#endif
