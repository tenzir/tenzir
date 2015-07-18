#ifndef VAST_CONCEPT_PRINTABLE_VAST_PORT_H
#define VAST_CONCEPT_PRINTABLE_VAST_PORT_H

#include "vast/port.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/numeric/integral.h"
#include "vast/concept/printable/string/string.h"
#include "vast/concept/printable/string/any.h"

namespace vast {

template <>
struct access::printer<port> : vast::printer<access::printer<port>>
{
  using attribute = port;

  template <typename Iterator>
  bool print(Iterator& out, port const& p) const
  {
    using namespace printers;
    if (! (u16.print(out, p.number_) && any.print(out, '/')))
      return false;
    switch (p.type())
    {
      default:
        return any.print(out, '?');
      case port::tcp:
        return str.print(out, "tcp");
      case port::udp:
        return str.print(out, "udp");
      case port::icmp:
        return str.print(out, "icmp");
    }
  }
};

template <>
struct printer_registry<port>
{
  using type = access::printer<port>;
};

} // namespace vast

#endif
