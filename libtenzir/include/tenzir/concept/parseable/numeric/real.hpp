//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/numeric/integral.hpp"
#include "tenzir/concepts.hpp"
#include "tenzir/detail/type_list.hpp"

#include <cmath>
#include <limits>
#include <type_traits>

namespace tenzir {
namespace policy {

struct require_dot {};
struct optional_dot {};

} // namespace policy

template <char Separator>
struct double_parser : parser_base<double_parser<Separator>> {
  using attribute = double;
  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, double& a) const;
};

struct double_detect_separator_parser
  : parser_base<double_detect_separator_parser> {
  using attribute = double;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& val) const {
    Attribute val_dot{};
    auto f_dot = f;
    const auto r_dot = double_parser<'.'>{}(f_dot, l, val_dot);
    Attribute val_comma{};
    auto f_comma = f;
    const auto r_comma = double_parser<','>{}(f_comma, l, val_comma);
    if (not r_dot && not r_comma) {
      return false;
    }
    if (r_dot && f_dot >= f_comma) {
      val = val_dot;
      f = f_dot;
      return true;
    }
    TENZIR_ASSERT(r_comma && f_comma >= f_dot);
    val = val_comma;
    f = f_comma;
    return true;
  }
};

template <>
struct parser_registry<double> {
  using type = double_parser<'.'>;
};

namespace parsers {

inline constexpr auto real = double_parser<'.'>{};
inline constexpr auto real_comma = double_parser<','>{};
inline constexpr auto real_detect_sep = double_detect_separator_parser{};

} // namespace parsers
} // namespace tenzir
