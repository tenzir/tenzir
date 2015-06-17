#ifndef VAST_CONCEPT_PARSEABLE_NUMERIC_REAL_H
#define VAST_CONCEPT_PARSEABLE_NUMERIC_REAL_H

#include <cmath>
#include <limits>
#include <type_traits>

#include "vast/concept/parseable/numeric/integral.h"

namespace vast {

template <typename T>
struct real_parser : parser<real_parser<T>>
{
  static constexpr auto require_dot = true; // TODO: Make this a policy.

  using attribute = T;

  template <typename Iterator>
  static bool parse_dot(Iterator& f, Iterator const& l)
  {
    if (f == l || *f != '.')
      return false;
    ++f;
    return true;
  }

  template <typename Base, typename Exp>
  static Base pow10(Exp exp)
  {
    return std::pow(Base{10}, exp);
  }

  static void scale(int exp, T& x)
  {
    if (exp >= 0)
    {
      x *= pow10<T>(exp);
    }
    else if (exp < std::numeric_limits<T>::min_exponent10)
    {
      x /= pow10<T>(-std::numeric_limits<T>::min_exponent10);
      x /= pow10<T>(-exp + std::numeric_limits<T>::min_exponent10);
    }
    else
    {
      x /= pow10<T>(-exp);
    }
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
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
    if (! got_dot && (! got_num || require_dot))
    {
      // If we require a dot but don't have it, we're out. We can neither
      // proceed if both dot and integral part are absent.
      f = save;
      return false;
    }
    // Now go for the fractional part.
    auto frac_start = f;
    if (integral_parser<uint64_t>::parse_pos(f, l, fractional))
    {
      // Downscale the fractional part.
      int frac_digits = 0;
      if (! std::is_same<Attribute, unused_type>{})
      {
        frac_digits = static_cast<int>(std::distance(frac_start, f));
        scale(-frac_digits, fractional);
      }
    }
    else if (! got_num)
    {
      // We need an integral or fractional part (or both).
      f = save;
      return false;
    }
    // Put the value together.
    a = integral + fractional;
    // Flip negative values.
    if (negative)
      a = -a;
    return true;
  }
};

template <typename T>
struct parser_registry<T, std::enable_if_t<std::is_floating_point<T>::value>>
{
  using type = real_parser<T>;
};

namespace parsers {

auto const fp = real_parser<float>{};
auto const real = real_parser<double>{};

} // namespace parsers
} // namespace vast

#endif
