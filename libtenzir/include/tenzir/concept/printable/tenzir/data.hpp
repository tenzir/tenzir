//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/numeric.hpp"
#include "tenzir/concept/printable/print.hpp"
#include "tenzir/concept/printable/std/chrono.hpp"
#include "tenzir/concept/printable/string.hpp"
#include "tenzir/concept/printable/tenzir/blob.hpp"
#include "tenzir/concept/printable/tenzir/ip.hpp"
#include "tenzir/concept/printable/tenzir/none.hpp"
#include "tenzir/concept/printable/tenzir/pattern.hpp"
#include "tenzir/concept/printable/tenzir/subnet.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/escapers.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/string.hpp"

namespace tenzir {

struct data_printer : printer_base<data_printer> {
  using attribute = data;

  template <class Iterator>
  bool print(Iterator& out, const data& d) const {
    return match(
      d,
      [&](const auto& x) {
        return make_printer<std::decay_t<decltype(x)>>{}(out, x);
      },
      [&](int64_t x) {
        // Force a sign to be printed even for positive integers.
        out = fmt::format_to(out, "{:+}", x);
        return true;
      },
      [&](const std::string& x) {
        static auto escaper = detail::make_extra_print_escaper("\"");
        static auto p = '"' << printers::escape(escaper) << '"';
        return p(out, x);
      });
  }
};

template <>
struct printer_registry<data> {
  using type = data_printer;
};

namespace printers {
auto const data = data_printer{};
} // namespace printers

struct tenzir_list_printer : printer_base<tenzir_list_printer> {
  using attribute = list;

  template <class Iterator>
  bool print(Iterator& out, const list& xs) const {
    auto p = '[' << ~(data_printer{} % ", ") << ']';
    return p.print(out, xs);
  }
};

template <>
struct printer_registry<list> {
  using type = tenzir_list_printer;
};

struct map_printer : printer_base<map_printer> {
  using attribute = map;

  template <class Iterator>
  bool print(Iterator& out, const map& xs) const {
    if (xs.empty())
      return printers::str.print(out, "{}");
    auto kvp = printers::data << " -> " << printers::data;
    auto p = '{' << (kvp % ", ") << '}';
    return p.print(out, xs);
  }
};

template <>
struct printer_registry<map> {
  using type = map_printer;
};

struct record_printer : printer_base<record_printer> {
  using attribute = record;

  template <class Iterator>
  bool print(Iterator& out, const record& xs) const {
    auto kvp = printers::str << ": " << printers::data;
    auto p = '<' << (kvp % ", ") << '>';
    return p.print(out, xs);
  }
};

template <>
struct printer_registry<record> {
  using type = record_printer;
};

template <>
struct printer_registry<blob> {
  using type = generic_blob_printer<blob>;
};

} // namespace tenzir
