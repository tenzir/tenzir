//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/detail/print_delimited.hpp"
#include "tenzir/concept/printable/numeric/integral.hpp"
#include "tenzir/concept/printable/string/char.hpp"
#include "tenzir/offset.hpp"

namespace tenzir {

struct offset_printer : printer_base<offset_printer> {
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

} // namespace tenzir
