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

#include <type_traits>

namespace tenzir {

// TODO: Customize via policy and merge policies with Parseable concept.
struct bool_printer : printer_base<bool_printer> {
  using attribute = bool;

  template <class Iterator>
  bool print(Iterator& out, bool x) const {
    return printers::str.print(out, x ? "true" : "false");
  }
};

template <>
struct printer_registry<bool> {
  using type = bool_printer;
};

namespace printers {

auto const tf = bool_printer{};

} // namespace printers
} // namespace tenzir
