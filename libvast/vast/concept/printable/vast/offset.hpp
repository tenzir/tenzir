//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/offset.hpp"

namespace vast {

struct offset_printer : printer<offset_printer> {
  using attribute = offset;

  template <class Iterator>
  bool print(Iterator& out, const offset& o) const {
    using delim = char_printer<','>;
    return detail::print_delimited<size_t, delim>(o.begin(), o.end(), out);
  }
};

template <>
struct printer_registry<offset> {
  using type = offset_printer;
};

namespace printers {
  auto const offset = offset_printer{};
} // namespace printers

} // namespace vast

