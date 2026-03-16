//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/string/string.hpp"

#include <caf/none.hpp>

namespace tenzir {

struct none_printer : printer_base<none_printer> {
  using attribute = caf::none_t;

  template <class Iterator>
  bool print(Iterator& out, caf::none_t) const {
    return printers::str.print(out, "null");
  }
};

template <>
struct printer_registry<caf::none_t> {
  using type = none_printer;
};

} // namespace tenzir
