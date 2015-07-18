#ifndef VAST_CONCEPT_PRINTABLE_VAST_SUBNET_H
#define VAST_CONCEPT_PRINTABLE_VAST_SUBNET_H

#include "vast/subnet.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/numeric/integral.h"
#include "vast/concept/printable/string/any.h"
#include "vast/concept/printable/vast/address.h"

namespace vast {

struct subnet_printer : printer<subnet_printer>
{
  using attribute = subnet;

  template <typename Iterator>
  bool print(Iterator& out, subnet const& sn) const
  {
    using namespace printers;
    return addr.print(out, sn.network())
        && any.print(out, '/')
        && u8.print(out, sn.length());
  }
};

template <>
struct printer_registry<subnet>
{
  using type = subnet_printer;
};

} // namespace vast

#endif
