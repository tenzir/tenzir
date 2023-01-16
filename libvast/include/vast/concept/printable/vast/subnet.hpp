//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/vast/ip.hpp"
#include "vast/subnet.hpp"

namespace vast {

struct subnet_printer : printer_base<subnet_printer> {
  using attribute = subnet;

  template <class Iterator>
  bool print(Iterator& out, const subnet& sn) const {
    using printers::ip, printers::chr, printers::u8;
    return (ip << chr<'/'> << u8)(out, sn.network(), sn.length());
  }
};

template <>
struct printer_registry<subnet> {
  using type = subnet_printer;
};

} // namespace vast
