#ifndef VAST_CONCEPT_PRINTABLE_NUMERIC_REAL_HPP
#define VAST_CONCEPT_PRINTABLE_NUMERIC_REAL_HPP

#include <cmath>
#include <cstdint>
#include <string>
#include <type_traits>

#include "vast/concept/printable/detail/print_numeric.hpp"
#include "vast/concept/printable/core/printer.hpp"

namespace vast {

template <typename T, int MaxDigits = 10>
struct real_printer : printer<real_printer<T, MaxDigits>> {
  static_assert(std::is_floating_point<T>{}, "T must be a floating point type");

  using attribute = T;

  template <typename Iterator>
  bool print(Iterator& out, T x) const {
    // negative = positive + sign
    if (x < 0) {
      *out++ = '-';
      x = -x;
    }
    T left;
    uint64_t right = std::round(std::modf(x, &left) * std::pow(10, MaxDigits));
    if (MaxDigits == 0)
      return detail::print_numeric(out, static_cast<uint64_t>(std::round(x)));
    if (!detail::print_numeric(out, static_cast<uint64_t>(left)))
      return false;
    *out++ = '.';
    // Add leading decimal zeros.
    auto magnitude = right == 0 ? MaxDigits : std::log10(right);
    for (auto i = 1.0; i < MaxDigits - magnitude; ++i)
      *out++ = '0';
    // Avoid trailing zeros on the decimal digits.
    while (right > 0 && right % 10 == 0)
      right /= 10;
    return detail::print_numeric(out, right);
  }
};

template <typename T>
struct printer_registry<T, std::enable_if_t<std::is_floating_point<T>::value>> {
  using type = real_printer<T>;
};

namespace printers {

auto const fp = real_printer<float>{};
auto const real = real_printer<double>{};

} // namespace printers
} // namespace vast

#endif
