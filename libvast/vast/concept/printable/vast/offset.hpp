#ifndef VAST_CONCEPT_PRINTABLE_VAST_OFFSET_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_OFFSET_HPP

#include "vast/offset.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/numeric/integral.hpp"

namespace vast {

struct offset_printer : printer<offset_printer> {
  using attribute = offset;

  template <typename Iterator>
  bool print(Iterator& out, offset const& o) const {
    using delim = char_printer<','>;
    return detail::print_delimited<size_t, delim>(o.begin(), o.end(), out);
  }
};

template <>
struct printer_registry<offset> {
  using type = offset_printer;
};

namespace printers {
  auto const offset = offset_printer{};
} // namespace printers

} // namespace vast

#endif
