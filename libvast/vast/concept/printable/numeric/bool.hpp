// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <type_traits>

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"

namespace vast {

// TODO: Customize via policy and merge policies with Parseable concept.
struct bool_printer : printer<bool_printer> {
  using attribute = bool;

  template <class Iterator>
  bool print(Iterator& out, bool x) const {
    return printers::any.print(out, x ? 'T' : 'F');
  }
};

template <>
struct printer_registry<bool> {
  using type = bool_printer;
};

namespace printers {

auto const tf = bool_printer{};

} // namespace printers
} // namespace vast

