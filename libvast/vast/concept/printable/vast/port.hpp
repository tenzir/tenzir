#ifndef VAST_CONCEPT_PRINTABLE_VAST_PORT_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_PORT_HPP

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/port.hpp"

namespace vast {

struct port_printer : vast::printer<port_printer> {
  using attribute = port;

  template <typename Iterator>
  bool print(Iterator& out, port const& p) const {
    using namespace printers;
    if (!(u16(out, p.number()) && chr<'/'>(out)))
      return false;
    switch (p.type()) {
      default:
        return chr<'?'>(out);
      case port::tcp:
        return str(out, "tcp");
      case port::udp:
        return str(out, "udp");
      case port::icmp:
        return str(out, "icmp");
    }
  }
};

template <>
struct printer_registry<port> {
  using type = port_printer;
};

} // namespace vast

#endif
