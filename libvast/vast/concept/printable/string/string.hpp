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

#ifndef VAST_CONCEPT_PRINTABLE_STRING_STRING_HPP
#define VAST_CONCEPT_PRINTABLE_STRING_STRING_HPP

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"

namespace vast {

struct string_printer : printer<string_printer> {
  using attribute = std::string;

  template <typename Iterator>
  static bool print_string(Iterator& out, char c) {
    return printers::any.print(out, c);
  }

  template <typename Iterator, typename StringIterator>
  static bool print_string(Iterator& out, StringIterator f, StringIterator l) {
    while (f != l)
      if (!printers::any.print(out, *f++))
        return false;
    return true;
  }

  template <typename Iterator>
  static bool print_string(Iterator& out, char const* str) {
    while (*str != '\0')
      if (!printers::any.print(out, *str++))
        return false;
    return true;
  }

  template <typename Iterator>
  static bool print_string(Iterator& out, const std::string& str) {
    return print_string(out, str.begin(), str.end());
  }

  template <typename Iterator, size_t N>
  static bool print(Iterator& out, const char(&str)[N]) {
    return print_string(out, str, str + N - 1); // without the last NUL byte.
  }

  template <typename Iterator, typename Attribute>
  bool print(Iterator& out, const Attribute& str) const {
    return print_string(out, str);
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
struct printer_registry<char const*> {
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

namespace printers {

auto const str = string_printer{};

} // namespace printers
} // namespace vast

#endif
