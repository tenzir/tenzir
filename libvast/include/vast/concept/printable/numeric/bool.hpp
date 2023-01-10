//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"

#include <type_traits>

namespace vast {

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
} // namespace vast
