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

struct double_parser : parser_base<double_parser> {
  using attribute = double;
  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, double& a) const;
};

template <>
struct parser_registry<double> {
  using type = double_parser;
};

namespace parsers {

auto const real = double_parser{};

} // namespace parsers
} // namespace tenzir
