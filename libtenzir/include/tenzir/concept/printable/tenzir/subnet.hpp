//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core.hpp"
#include "tenzir/concept/printable/numeric/integral.hpp"
#include "tenzir/concept/printable/string/char.hpp"
#include "tenzir/concept/printable/tenzir/ip.hpp"
#include "tenzir/subnet.hpp"

namespace tenzir {

struct subnet_printer : printer_base<subnet_printer> {
  using attribute = subnet;

  template <class Iterator>
  bool print(Iterator& out, const subnet& sn) const {
    using printers::ip, printers::ipv6, printers::chr, printers::u8;
    const auto length = sn.length();
    const auto network = sn.network();
    const auto is_v4 = network.is_v4();
    if (is_v4 and length >= 96) {
      return (ip << chr<'/'> << u8)(out, sn.network(), sn.length() - 96);
    }
    return (ipv6 << chr<'/'> << u8)(out, sn.network(), sn.length());
  }
};

template <>
struct printer_registry<subnet> {
  using type = subnet_printer;
};

} // namespace tenzir
