//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core.hpp"
#include "tenzir/concept/printable/string/any.hpp"
#include "tenzir/concept/printable/string/char.hpp"
#include "tenzir/detail/coding.hpp"
#include "tenzir/uuid.hpp"

#include <cstddef>

namespace tenzir {

struct uuid_printer : printer_base<uuid_printer> {
  using attribute = uuid;

  static constexpr auto hexbyte = printers::any << printers::any;

  template <class Iterator>
  bool print(Iterator& out, const uuid& x) const {
    for (size_t i = 0; i < 16; ++i) {
      auto hi = detail::byte_to_char((x[i] >> 4) & std::byte{0x0f});
      auto lo = detail::byte_to_char(x[i] & std::byte{0x0f});
      if (not hexbyte(out, hi, lo))
        return false;
      if (i == 3 or i == 5 or i == 7 or i == 9)
        if (not printers::chr<'-'>(out))
          return false;
    }
    return true;
  }
};

template <>
struct printer_registry<uuid> {
  using type = uuid_printer;
};

} // namespace tenzir
