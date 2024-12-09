//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/access.hpp"
#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/numeric.hpp"
#include "tenzir/concept/printable/print.hpp"
#include "tenzir/concept/printable/std/chrono.hpp"
#include "tenzir/concept/printable/string.hpp"
#include "tenzir/concept/printable/tenzir/blob.hpp"
#include "tenzir/concept/printable/tenzir/ip.hpp"
#include "tenzir/concept/printable/tenzir/none.hpp"
#include "tenzir/concept/printable/tenzir/port.hpp"
#include "tenzir/concept/printable/tenzir/subnet.hpp"
#include "tenzir/detail/escapers.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/view.hpp"

#include <string_view>

namespace tenzir {

// view<T> resolves to just T for all primitive types such as numbers as well
// as IP addresses, etc. Hence, we only need to deal with a couple of view
// types here.

// -- printer implementations --------------------------------------------------

struct string_view_printer : printer_base<string_view_printer> {
  using attribute = view<std::string>;

  template <class Iterator>
  bool print(Iterator& out, const attribute& x) const {
    static auto escaper = detail::make_extra_print_escaper("\"");
    static auto p = '"' << printers::escape(escaper) << '"';
    return p(out, x);
  }
};

struct data_view_printer : printer_base<data_view_printer> {
  using attribute = view<data>;

  template <class Iterator>
  bool print(Iterator& out, const attribute& d) const {
    auto f = detail::overload{
      [&](const auto& x) {
        return make_printer<std::decay_t<decltype(x)>>{}(out, x);
      },
      [&](view<int64_t> x) {
        out = fmt::format_to(out, "{:+}", x);
        return true;
      },
      [&](const view<std::string>& x) {
        return string_view_printer{}(out, x);
      },
    };
    return match(d, f);
  }
};

struct pattern_view_printer : printer_base<pattern_view_printer> {
  using attribute = view<pattern>;

  template <class Iterator>
  bool print(Iterator& out, const attribute& pat) const {
    auto p = '/' << printers::str << ((pat.case_insensitive()) ? "/i" : "/");
    return p.print(out, pat.string());
  }
};

struct list_view_printer : printer_base<list_view_printer> {
  using attribute = view<list>;

  template <class Iterator>
  bool print(Iterator& out, const attribute& xs) const {
    if (!xs || xs->empty())
      return printers::str.print(out, "[]");
    auto p = '[' << ~(data_view_printer{} % ", ") << ']';
    return p.print(out, xs);
  }
};

struct map_view_printer : printer_base<map_view_printer> {
  using attribute = view<map>;

  template <class Iterator>
  bool print(Iterator& out, const attribute& xs) const {
    if (!xs || xs->empty())
      return printers::str.print(out, "{}");
    auto kvp = data_view_printer{} << " -> " << data_view_printer{};
    auto p = '{' << (kvp % ", ") << '}';
    return p.print(out, xs);
  }
};

struct record_view_printer : printer_base<record_view_printer> {
  using attribute = view<record>;

  template <class Iterator>
  bool print(Iterator& out, const attribute& xs) const {
    if (!xs || xs->empty())
      return printers::str.print(out, "<>");
    auto kvp = string_view_printer{} << ": " << data_view_printer{};
    auto p = '<' << (kvp % ", ") << '>';
    return p.print(out, xs);
  }
};

// -- printer registry setup ---------------------------------------------------

template <>
struct printer_registry<view<data>> {
  using type = data_view_printer;
};

template <>
struct printer_registry<view<pattern>> {
  using type = pattern_view_printer;
};

template <>
struct printer_registry<view<list>> {
  using type = list_view_printer;
};

template <>
struct printer_registry<view<map>> {
  using type = map_view_printer;
};

template <>
struct printer_registry<view<record>> {
  using type = record_view_printer;
};

template <>
struct printer_registry<view<blob>> {
  using type = generic_blob_printer<view<blob>>;
};

} // namespace tenzir
