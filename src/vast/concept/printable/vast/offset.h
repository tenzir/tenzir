#ifndef VAST_CONCEPT_PRINTABLE_VAST_OFFSET_H
#define VAST_CONCEPT_PRINTABLE_VAST_OFFSET_H

#include "vast/offset.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/detail/print_delimited.h"
#include "vast/concept/printable/string/char.h"
#include "vast/concept/printable/numeric/integral.h"

namespace vast {

struct offset_printer : printer<offset_printer>
{
  using attribute = offset;

  template <typename Iterator>
  bool print(Iterator& out, offset const& o) const
  {
    using delim = char_printer<','>;
    return detail::print_delimited<size_t, delim>(o.begin(), o.end(), out);
  }
};

template <>
struct printer_registry<offset>
{
  using type = offset_printer;
};

} // namespace vast

#endif
