#ifndef VAST_CONCEPT_PARSEABLE_NUMERIC_INTEGRAL_H
#define VAST_CONCEPT_PARSEABLE_NUMERIC_INTEGRAL_H

#include <cstdint>

#include "vast/concept/parseable/core/parser.h"

namespace vast {
namespace detail {

template <typename Iterator>
bool parse_sign(Iterator& i) {
  auto minus = *i == '-';
  if (minus || *i == '+') {
    ++i;
    return minus;
  }
  return false;
}

} // namespace detail

template <
  typename T,
  int MaxDigits = std::numeric_limits<T>::digits10 + 1,
  int MinDigits = 1,
  int Radix = 10
>
struct integral_parser 
  : parser<integral_parser<T, MaxDigits, MinDigits, Radix>> {
  static_assert(Radix == 10, "unsupported radix");
  static_assert(MinDigits > 0, "need at least one minimum digit");
  static_assert(MaxDigits > 0, "need at least one maximum digit");
  static_assert(MinDigits <= MaxDigits, "maximum cannot exceed minimum");

  using attribute = T;

  static bool isdigit(char c) {
    return c >= '0' && c <= '9';
  }

  template <typename Iterator, typename Attribute, typename F>
  static auto accumulate(Iterator& f, Iterator const& l, Attribute& a, F acc) {
    int digits = 0;
    a = 0;
    while (f != l && isdigit(*f)) {
      if (++digits > MaxDigits)
        return false;
      acc(a, *f++ - '0');
    }
    if (digits < MinDigits)
      return false;
    return true;
  }

  template <typename Iterator>
  static auto parse_pos(Iterator& f, Iterator const& l, unused_type) {
    return accumulate(f, l, unused, [](auto&, auto) {});
  }

  template <typename Iterator, typename Attribute>
  static auto parse_pos(Iterator& f, Iterator const& l, Attribute& a) {
    return accumulate(f, l, a, [](auto& n, auto x) {
      n *= Radix;
      n += x;
    });
  }

  template <typename Iterator>
  static auto parse_neg(Iterator& f, Iterator const& l, unused_type) {
    return accumulate(f, l, unused, [](auto&, auto) {});
  }

  template <typename Iterator, typename Attribute>
  static auto parse_neg(Iterator& f, Iterator const& l, Attribute& a) {
    return accumulate(f, l, a, [](auto& n, auto x) {
      n *= Radix;
      n -= x;
    });
  }

  template <typename Iterator, typename Attribute>
  static bool parse_signed(Iterator& f, Iterator const& l, Attribute& a) {
    return detail::parse_sign(f) ? parse_neg(f, l, a) : parse_pos(f, l, a);
  }

  template <typename Iterator, typename Attribute>
  static bool parse_unsigned(Iterator& f, Iterator const& l, Attribute& a) {
    return parse_pos(f, l, a);
  }

  template <typename Iterator, typename Attribute, typename This = T>
  static auto dispatch(Iterator& f, Iterator const& l, Attribute& a)
    -> std::enable_if_t<std::is_signed<This>{}, bool> {
    return parse_signed(f, l, a);
  }

  template <typename Iterator, typename Attribute, typename This = T>
  static auto dispatch(Iterator& f, Iterator const& l, Attribute& a)
    -> std::enable_if_t<std::is_unsigned<This>{}, bool> {
    return parse_unsigned(f, l, a);
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    if (f == l)
      return false;
    auto save = f;
    if (dispatch(f, l, a))
      return true;
    f = save;
    return false;
  }
};

template <typename T>
struct parser_registry<T, std::enable_if_t<std::is_integral<T>::value>> {
  using type = integral_parser<T>;
};

namespace parsers {

auto const i8 = integral_parser<int8_t>{};
auto const i16 = integral_parser<int16_t>{};
auto const i32 = integral_parser<int32_t>{};
auto const i64 = integral_parser<int64_t>{};
auto const u8 = integral_parser<uint8_t>{};
auto const u16 = integral_parser<uint16_t>{};
auto const u32 = integral_parser<uint32_t>{};
auto const u64 = integral_parser<uint64_t>{};

} // namespace parsers
} // namespace vast

#endif
