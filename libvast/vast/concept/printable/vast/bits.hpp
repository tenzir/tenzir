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

#include "vast/bits.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/any.hpp"

namespace vast {
namespace policy {

struct expanded {};
struct rle {};

} // namespace policy

template <class T, class Policy = policy::expanded>
struct bits_printer : printer<bits_printer<T , Policy>> {
  using attribute = bits<T>;
  using word_type = typename bits<T>::word_type;

  template <class Iterator, class P = Policy>
  auto print(Iterator& out, const bits<T>& b) const
  -> std::enable_if_t<std::is_same_v<P, policy::rle>, bool> {
    auto print_run = [&](auto bit, auto length) {
      using size_type = typename word_type::size_type;
      return printers::integral<size_type>(out, length) &&
             printers::any(out, bit ? 'T' : 'F');
    };
    if (b.homogeneous()) {
      if (!print_run(!!b.data(), b.size()))
        return false;
    } else {
      auto n = 1u;
      bool x = b.data() & word_type::lsb1;
      for (auto i = 1u; i < b.size(); ++i) {
        bool y = b.data() & word_type::mask(i);
        if (x == y) {
          ++n;
        } else if (!print_run(x, n)) {
          return false;
        } else {
          n = 1;
          x = y;
        }
      }
      if (!print_run(x, n))
        return false;
    }
    return true;
  }

  template <class Iterator, class P = Policy>
  auto print(Iterator& out, const bits<T>& b) const
  -> std::enable_if_t<std::is_same_v<P, policy::expanded>, bool> {
    if (b.size() > word_type::width) {
      auto c = b.data() ? '1' : '0';
      for (auto i = 0u; i < b.size(); ++i)
        if (!printers::any(out, c))
          return false;
    } else {
      for (auto i = 0u; i < b.size(); ++i) {
        auto c = b.data() & word_type::mask(i) ? '1' : '0';
        if (!printers::any(out, c))
          return false;
       }
    }
    return true;
  }
};

template <class T>
struct printer_registry<bits<T>> {
  using type = bits_printer<T, policy::expanded>;
};

namespace printers {

template <class T, class Policy>
auto const bits = bits_printer<T, Policy>{};

} // namespace printers
} // namespace vast

