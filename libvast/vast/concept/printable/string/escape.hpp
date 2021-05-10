//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"

#include <string>

namespace vast {

template <class Escaper>
struct escape_printer : printer<escape_printer<Escaper>> {
  using attribute = std::string_view;

  explicit escape_printer(Escaper f) : escaper{f} {
    // nop
  }

  template <class Iterator>
  bool print(Iterator& out, std::string_view str) const {
    auto f = str.begin();
    auto l = str.end();
    while (f != l)
      escaper(f, out);
    return true;
  }

  Escaper escaper;
};

namespace printers {

template <class Escaper>
auto escape(Escaper escaper) {
  return escape_printer<Escaper>{escaper};
}

} // namespace printers
} // namespace vast
