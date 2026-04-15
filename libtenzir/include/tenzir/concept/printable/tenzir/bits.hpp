//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/bits.hpp"
#include "tenzir/concept/printable/core.hpp"
#include "tenzir/concept/printable/numeric/integral.hpp"
#include "tenzir/concept/printable/string/any.hpp"
#include "tenzir/concepts.hpp"

namespace tenzir {
namespace policy {

struct expanded {};
struct rle {};

} // namespace policy

template <class T, class Policy = policy::expanded>
struct bits_printer : printer_base<bits_printer<T, Policy>> {
  using attribute = bits<T>;
  using word_type = typename bits<T>::word_type;

  template <class Iterator, std::same_as<policy::rle> P = Policy>
  auto print(Iterator& out, const bits<T>& b) const -> bool {
    auto print_run = [&](auto bit, auto length) {
      using size_type = typename word_type::size_type;
      return printers::integral<size_type>(out, length)
             and printers::any(out, bit ? 'T' : 'F');
    };
    if (b.homogeneous()) {
      if (not print_run(! ! b.data(), b.size())) {
        return false;
      }
    } else {
      auto n = 1u;
      bool x = b.data() & word_type::lsb1;
      for (auto i = 1u; i < b.size(); ++i) {
        bool y = b.data() & word_type::mask(i);
        if (x == y) {
          ++n;
        } else if (not print_run(x, n)) {
          return false;
        } else {
          n = 1;
          x = y;
        }
      }
      if (not print_run(x, n)) {
        return false;
      }
    }
    return true;
  }

  template <class Iterator, std::same_as<policy::expanded> P = Policy>
  auto print(Iterator& out, const bits<T>& b) const -> bool {
    if (b.size() > word_type::width) {
      auto c = b.data() ? '1' : '0';
      for (auto i = 0u; i < b.size(); ++i) {
        if (not printers::any(out, c)) {
          return false;
        }
      }
    } else {
      for (auto i = 0u; i < b.size(); ++i) {
        auto c = b.data() & word_type::mask(i) ? '1' : '0';
        if (not printers::any(out, c)) {
          return false;
        }
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
} // namespace tenzir
