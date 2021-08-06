//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

struct any_printer : printer_base<any_printer> {
  using attribute = char;

  template <class Iterator>
  bool print(Iterator& out, char x) const {
    // TODO: in the future when we have ranges, we should add a mechanism to
    // check whether we exceed the bounds instead of just deref'ing the
    // iterator and pretending it'll work out.
    *out++ = x;
    return true;
  }
};

template <>
struct printer_registry<char> {
  using type = any_printer;
};

namespace printers {

auto const any = any_printer{};

} // namespace printers
} // namespace vast
