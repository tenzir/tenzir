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

#ifndef VAST_CONCEPT_PRINTABLE_DETAIL_PRINT_DELIMITED_HPP
#define VAST_CONCEPT_PRINTABLE_DETAIL_PRINT_DELIMITED_HPP

#include "vast/concept/printable/print.hpp"
#include "vast/concept/support/unused_type.hpp"

namespace vast {
namespace detail {

/// Prints a delimited Iterator range.
template <
  typename T,
  typename Delimiter,
  typename InputIterator,
  typename OutputIterator
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
template <typename InputIterator, typename OutputIterator, typename Delimiter>
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
} // namespace vast

#endif
