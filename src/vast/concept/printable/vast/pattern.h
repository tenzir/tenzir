#ifndef VAST_CONCEPT_PRINTABLE_VAST_PATTERN_H
#define VAST_CONCEPT_PRINTABLE_VAST_PATTERN_H

#include "vast/access.h"
#include "vast/pattern.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/string/any.h"
#include "vast/concept/printable/string/string.h"

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
