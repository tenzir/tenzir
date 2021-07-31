//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"

namespace vast {

struct string_printer : printer_base<string_printer> {
  using attribute = std::string_view;

  template <class Iterator>
  bool print(Iterator& out, std::string_view str) const {
    auto f = str.begin();
    auto l = str.end();
    while (f != l)
      if (!printers::any.print(out, *f++))
        return false;
    return true;
  }
};

template <size_t N>
struct printer_registry<const char(&)[N]> {
  using type = string_printer;
};

template <size_t N>
struct printer_registry<char[N]> {
  using type = string_printer;
};

template <>
struct printer_registry<const char*> {
  using type = string_printer;
};

template <>
struct printer_registry<char*> {
  using type = string_printer;
};

template <>
struct printer_registry<std::string> {
  using type = string_printer;
};

template <>
struct printer_registry<std::string_view> {
  using type = string_printer;
};

namespace printers {

auto const str = string_printer{};

} // namespace printers
} // namespace vast
