#ifndef VAST_CONCEPT_PRINTABLE_NUMERIC_INTEGRAL_HPP
#define VAST_CONCEPT_PRINTABLE_NUMERIC_INTEGRAL_HPP

#include <cstdint>
#include <type_traits>

#include "vast/concept/printable/detail/print_numeric.hpp"
#include "vast/concept/printable/core/printer.hpp"

namespace vast {

template <typename T>
struct integral_printer : printer<integral_printer<T>> {
  static_assert(std::is_integral<T>{}, "T must be an integral type");

  using attribute = T;

  template <typename Iterator, typename U = T>
  static auto dispatch(Iterator& out, T x)
    -> std::enable_if_t<std::is_unsigned<U>{}, bool> {
    return detail::print_numeric(out, x);
  }

  template <typename Iterator, typename U = T>
  static auto dispatch(Iterator& out, T x)
    -> std::enable_if_t<std::is_signed<U>{}, bool> {
    if (x < 0) {
      *out++ = '-';
      return detail::print_numeric(out, -x);
    }
    *out++ = '+';
    return detail::print_numeric(out, x);
  }

  template <typename Iterator>
  bool print(Iterator& out, T x) const {
    return dispatch(out, x);
  }
};

template <typename T>
struct printer_registry<T, std::enable_if_t<std::is_integral<T>::value>> {
  using type = integral_printer<T>;
};

namespace printers {

template <typename T>
auto const integral = integral_printer<T>{};

auto const i8 = integral_printer<int8_t>{};
auto const i16 = integral_printer<int16_t>{};
auto const i32 = integral_printer<int32_t>{};
auto const i64 = integral_printer<int64_t>{};
auto const u8 = integral_printer<uint8_t>{};
auto const u16 = integral_printer<uint16_t>{};
auto const u32 = integral_printer<uint32_t>{};
auto const u64 = integral_printer<uint64_t>{};

} // namespace printers
} // namespace vast

#endif
