/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"

namespace vast {

struct string_printer : printer<string_printer> {
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
