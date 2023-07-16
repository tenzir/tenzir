//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/numeric/integral.hpp"
#include "tenzir/concept/printable/string/char.hpp"
#include "tenzir/concept/printable/string/string.hpp"
#include "tenzir/port.hpp"

namespace tenzir {

struct port_printer : printer_base<port_printer> {
  using attribute = port;

  template <class Iterator>
  bool print(Iterator& out, const port& p) const {
    using namespace printers;
    if (!(u16(out, p.number()) && chr<'/'>(out)))
      return false;
    switch (p.type()) {
      default:
        return chr<'?'>(out);
      case port_type::icmp:
        return str(out, "icmp");
      case port_type::tcp:
        return str(out, "tcp");
      case port_type::udp:
        return str(out, "udp");
      case port_type::icmp6:
        return str(out, "icmp6");
      case port_type::sctp:
        return str(out, "sctp");
    }
  }
};

template <>
struct printer_registry<port> {
  using type = port_printer;
};

} // namespace tenzir
