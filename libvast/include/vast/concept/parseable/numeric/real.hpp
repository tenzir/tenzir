//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concepts.hpp"
#include "vast/detail/type_list.hpp"

#include <cmath>
#include <limits>
#include <type_traits>

namespace vast {
namespace policy {

struct require_dot {};
struct optional_dot {};

} // namespace policy

template <class T, class... Policies>
struct real_parser : parser_base<real_parser<T, Policies...>> {
  using attribute = T;
  using policies =
    std::conditional_t<(sizeof...(Policies) > 0),
    detail::type_list<Policies...>,
    detail::type_list<policy::require_dot>
  >;
  static constexpr bool require_dot =
    detail::tl_exists<
      policies,
      detail::tbind<std::is_same, policy::require_dot>::template type
    >::value;

  template <class Iterator>
  static bool parse_dot(Iterator& f, const Iterator& l) {
    if (f == l || *f != '.')
      return false;
    ++f;
    return true;
  }

  template <class Base, class Exp>
  static Base pow10(Exp exp) {
    return std::pow(Base{10}, exp);
  }

  static void scale(int, unused_type) {
  }

  static void scale(int exp, T& x) {
    if (exp >= 0) {
      x *= pow10<T>(exp);
    } else if (exp < std::numeric_limits<T>::min_exponent10) {
      x /= pow10<T>(-std::numeric_limits<T>::min_exponent10);
      x /= pow10<T>(-exp + std::numeric_limits<T>::min_exponent10);
    } else {
      x /= pow10<T>(-exp);
    }
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (f == l)
      return false;
    auto save = f;
    // Parse sign.
    auto negative = detail::parse_sign(f);
    // Parse an integer.
    Attribute integral = 0;
    Attribute fractional = 0;
    auto got_num = integral_parser<uint64_t>::parse_pos(f, l, integral);
    // TODO: if we did not get a number, we may have gotton Inf or NaN, which we
    // ignore at this point. Future work...
    // Parse dot.
    auto got_dot = parse_dot(f, l);
    if (!got_dot && (!got_num || require_dot)) {
      // If we require a dot but don't have it, we're out. We can neither
      // proceed if both dot and integral part are absent.
      f = save;
      return false;
    }
    // Now go for the fractional part.
    auto frac_start = f;
    if (integral_parser<uint64_t>::parse_pos(f, l, fractional)) {
      // Downscale the fractional part.
      int frac_digits = 0;
      if (!std::is_same<Attribute, unused_type>{}) {
        frac_digits = static_cast<int>(std::distance(frac_start, f));
        scale(-frac_digits, fractional);
      }
    } else if (!got_num) {
      // We need an integral or fractional part (or both).
      f = save;
      return false;
    }
    // Put the value together.
    a = integral + fractional;
    // Now check if we have an exponent
    auto got_e = parsers::chr{'e'}(f, l);
    if (got_e && f != l) {
      uint16_t exp = 0;
      int exp_sign = 1;
      if (*f == '-') {
        exp_sign = -1;
        f++;
      } else if (*f == '+') {
        f++;
      }
      if (!integral_parser<uint16_t>::parse_pos(f, l, exp)) {
        f = save;
        return false;
      }
      scale(exp * exp_sign, a);
    }
    // Flip negative values.
    if (negative)
      a = -a;
    return true;
  }
};

template <concepts::floating_point T>
struct parser_registry<T> {
  using type = real_parser<T, policy::require_dot>;
};

namespace parsers {

auto const fp = real_parser<float, policy::require_dot>{};
auto const real = real_parser<double, policy::require_dot>{};
auto const fp_opt_dot = real_parser<float, policy::optional_dot>{};
auto const real_opt_dot = real_parser<double, policy::optional_dot>{};

} // namespace parsers
} // namespace vast

