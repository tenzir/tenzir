// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/subnet.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/vast/address.hpp"

namespace vast {

struct subnet_printer : printer<subnet_printer> {
  using attribute = subnet;

  template <class Iterator>
  bool print(Iterator& out, const subnet& sn) const {
    using namespace printers;
    return (addr << chr<'/'> << u8)(out, sn.network(), sn.length());
  }
};

template <>
struct printer_registry<subnet> {
  using type = subnet_printer;
};

} // namespace vast

