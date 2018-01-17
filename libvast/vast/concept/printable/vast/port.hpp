/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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
