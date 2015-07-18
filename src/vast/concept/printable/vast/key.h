#ifndef VAST_CONCEPT_PRINTABLE_VAST_KEY_H
#define VAST_CONCEPT_PRINTABLE_VAST_KEY_H

#include "vast/key.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/detail/print_delimited.h"
#include "vast/concept/printable/numeric/integral.h"
#include "vast/concept/printable/string/char.h"

namespace vast {

struct key_printer : printer<key_printer>
{
  using attribute = key;

  template <typename Iterator>
  bool print(Iterator& out, key const& k) const
  {
    return detail::print_delimited(k.begin(), k.end(), out, '.');
  }
};

template <>
struct printer_registry<key>
{
  using type = key_printer;
};

} // namespace vast

#endif
