//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/print.hpp"
#include "tenzir/concept/support/unused_type.hpp"

namespace tenzir {
namespace detail {

/// Prints a delimited Iterator range.
template <
  class T,
  class Delimiter,
  class InputIterator,
  class OutputIterator
>
bool print_delimited(InputIterator begin, InputIterator end,
                     OutputIterator&& out) {
  static auto const printer = make_printer<T>{};
  static auto const delim = Delimiter{};
  if (begin == end)
    return true;
  if (!printer.print(out, *begin))
    return false;
  while (++begin != end)
    if (!(delim.print(out, unused) && printer.print(out, *begin)))
      return false;
  return true;
}

/// Prints a delimited Iterator range.
template <class InputIterator, class OutputIterator, class Delimiter>
bool print_delimited(InputIterator begin, InputIterator end,
                     OutputIterator&& out, const Delimiter& delim) {
  if (begin == end)
    return true;
  if (!print(out, *begin))
    return false;
  while (++begin != end)
    if (!(print(out, delim) && print(out, *begin)))
      return false;
  return true;
}

} // namespace detail
} // namespace tenzir
