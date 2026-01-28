//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/numeric/real.hpp"

#include "tenzir/detail/narrow.hpp"

#include <array>
#include <charconv>
#include <cmath>
#include <limits>
#include <memory>
#include <string_view>

namespace tenzir {

namespace {

// Maximum buffer size for double parsing. Sized generously to handle numbers
// with many digits (e.g., 0.000000000000000000001) even though doubles only
// have ~15-17 significant digits.
constexpr size_t max_double_chars = 64;

// Case-insensitive prefix match.
auto matches_prefix(std::string_view str, std::string_view prefix) -> size_t {
  if (str.size() < prefix.size()) {
    return 0;
  }
  for (size_t i = 0; i < prefix.size(); ++i) {
    char c = str[i];
    char p = prefix[i];
    // Convert to lowercase for comparison.
    if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c + ('a' - 'A'));
    }
    if (p >= 'A' && p <= 'Z') {
      p = static_cast<char>(p + ('a' - 'A'));
    }
    if (c != p) {
      return 0;
    }
  }
  return prefix.size();
}

// -----------------------------------------------------------------------------
// std::from_chars implementation (used when TENZIR_HAS_FLOAT_FROM_CHARS is set)
// -----------------------------------------------------------------------------

#if TENZIR_HAS_FLOAT_FROM_CHARS

// The C++ standard says that on result_out_of_range, "value is unmodified".
// We need to determine if the out-of-range condition is overflow (set to
// infinity) or underflow (set to zero) by examining the input string.
// Returns: positive for overflow, negative for underflow, 0 if cannot determine
auto determine_out_of_range_type(const char* first, const char* last,
                                 bool& is_negative) -> int {
  const char* p = first;
  is_negative = false;
  // Skip leading sign
  if (p != last && (*p == '+' || *p == '-')) {
    is_negative = (*p == '-');
    ++p;
  }
  // Skip to exponent part
  while (p != last && *p != 'e' && *p != 'E') {
    ++p;
  }
  if (p == last) {
    // No exponent - this shouldn't cause out_of_range for typical inputs
    // but if it does, assume overflow (too many integer digits)
    return 1;
  }
  ++p; // skip 'e' or 'E'
  bool exp_negative = false;
  if (p != last && (*p == '+' || *p == '-')) {
    exp_negative = (*p == '-');
    ++p;
  }
  // Positive exponent = overflow (number too large)
  // Negative exponent = underflow (number too small, rounds to zero)
  return exp_negative ? -1 : 1;
}

// Parse a double using std::from_chars with a custom decimal separator.
// Returns {pointer past consumed chars, error code}.
auto parse_double_from_chars(const char* first, const char* last, double& value,
                             char separator)
  -> std::pair<const char*, std::errc> {
  if (first == last) {
    return {first, std::errc::invalid_argument};
  }
  // Fast path for '.' separator: std::from_chars handles everything
  // (inf/nan/infinity, scientific notation, etc.) - no pre-scanning needed.
  // We only need to handle leading '+' which std::from_chars rejects.
  if (separator == '.') {
    const char* start = first;
    const bool has_leading_plus = (*first == '+');
    if (has_leading_plus) {
      ++start;
      if (start == last) {
        return {first, std::errc::invalid_argument};
      }
    }
    const auto [ptr, ec] = std::from_chars(start, last, value);
    if (ec == std::errc::result_out_of_range) {
      // Per C++ standard, value is unmodified on result_out_of_range.
      // Determine if overflow (infinity) or underflow (zero).
      bool is_negative = false;
      const auto range_type
        = determine_out_of_range_type(first, ptr, is_negative);
      if (range_type > 0) {
        // Overflow - set to infinity
        value = is_negative ? -std::numeric_limits<double>::infinity()
                            : std::numeric_limits<double>::infinity();
      } else {
        // Underflow - set to zero
        value = is_negative ? -0.0 : 0.0;
      }
      return {ptr, std::errc{}};
    }
    if (ec != std::errc{}) {
      return {first, ec};
    }
    return {ptr, std::errc{}};
  }
  // Slow path for ',' separator: must scan to find extent, copy to buffer,
  // replace separator with '.', then parse. This is inherently less efficient
  // than the fast path since std::from_chars doesn't support custom separators.
  const char* scan = first;
  const char* separator_pos = nullptr;
  bool in_exponent = false;
  bool has_leading_plus = false;
  // Handle optional leading sign. Note: std::from_chars rejects '+', so we
  // track it separately and skip it when parsing.
  if (*scan == '+') {
    has_leading_plus = true;
    ++scan;
  } else if (*scan == '-') {
    ++scan;
  }
  // Check for special values: inf, infinity, nan (case-insensitive).
  // std::from_chars handles these directly, so we just need to find the extent.
  const auto remaining
    = std::string_view{scan, detail::narrow_cast<size_t>(last - scan)};
  if (auto n = matches_prefix(remaining, "infinity"); n > 0) {
    scan += n;
  } else if (auto n = matches_prefix(remaining, "inf"); n > 0) {
    scan += n;
  } else if (auto n = matches_prefix(remaining, "nan"); n > 0) {
    scan += n;
  } else {
    // Scan mantissa and exponent for regular numbers.
    while (scan != last) {
      char c = *scan;
      if (c >= '0' && c <= '9') {
        ++scan;
      } else if (c == separator && ! separator_pos && ! in_exponent) {
        separator_pos = scan;
        ++scan;
      } else if ((c == 'e' || c == 'E') && ! in_exponent) {
        in_exponent = true;
        ++scan;
        // Handle optional exponent sign.
        if (scan != last && (*scan == '-' || *scan == '+')) {
          ++scan;
        }
      } else {
        break;
      }
    }
  }
  const auto len = static_cast<size_t>(scan - first);
  if (len == 0) {
    return {first, std::errc::invalid_argument};
  }
  // Determine the parsing range, skipping leading '+' which from_chars rejects.
  const char* parse_start = has_leading_plus ? first + 1 : first;
  const auto parse_len = has_leading_plus ? len - 1 : len;
  // If no separator was found, parse directly without copying.
  if (! separator_pos) {
    const auto [ptr, ec]
      = std::from_chars(parse_start, parse_start + parse_len, value);
    if (ec == std::errc::result_out_of_range) {
      // Per C++ standard, value is unmodified on result_out_of_range.
      bool is_negative = false;
      const auto range_type
        = determine_out_of_range_type(first, first + len, is_negative);
      if (range_type > 0) {
        value = is_negative ? -std::numeric_limits<double>::infinity()
                            : std::numeric_limits<double>::infinity();
      } else {
        value = is_negative ? -0.0 : 0.0;
      }
    } else if (ec != std::errc{}) {
      return {first, ec};
    }
    // Adjust pointer back to account for skipped '+'.
    const auto consumed
      = static_cast<size_t>(ptr - parse_start) + (has_leading_plus ? 1 : 0);
    return {first + consumed, std::errc{}};
  }
  // Copy to buffer and replace separator.
  if (parse_len > max_double_chars) {
    return {first, std::errc::invalid_argument};
  }
  std::array<char, max_double_chars> buffer;
  std::copy(parse_start, parse_start + parse_len, buffer.begin());
  // Calculate separator index in the buffer (adjusted for skipped '+').
  const auto separator_index
    = static_cast<size_t>(separator_pos - first) - (has_leading_plus ? 1 : 0);
  // Bounds check to prevent buffer overflow with malformed input.
  if (separator_index >= parse_len) {
    return {first, std::errc::invalid_argument};
  }
  buffer[separator_index] = '.';
  const auto [ptr, ec]
    = std::from_chars(buffer.data(), buffer.data() + parse_len, value);
  if (ec == std::errc::result_out_of_range) {
    // Per C++ standard, value is unmodified on result_out_of_range.
    bool is_negative = false;
    const auto range_type
      = determine_out_of_range_type(first, first + len, is_negative);
    if (range_type > 0) {
      value = is_negative ? -std::numeric_limits<double>::infinity()
                          : std::numeric_limits<double>::infinity();
    } else {
      value = is_negative ? -0.0 : 0.0;
    }
  } else if (ec != std::errc{}) {
    return {first, ec};
  }
  // Translate buffer position back to original input.
  const auto consumed
    = static_cast<size_t>(ptr - buffer.data()) + (has_leading_plus ? 1 : 0);
  return {first + consumed, std::errc{}};
}

#endif // TENZIR_HAS_FLOAT_FROM_CHARS

// -----------------------------------------------------------------------------
// Fallback implementation (used when std::from_chars doesn't support floats)
// -----------------------------------------------------------------------------

#if ! TENZIR_HAS_FLOAT_FROM_CHARS

// Any exponent larger than 511 always overflows for doubles.
constexpr int max_double_exponent = 511;

// Pre-computed powers of 10 for scaling.
constexpr double power_table[]
  = {1e1, 1e2, 1e4, 1e8, 1e16, 1e32, 1e64, 1e128, 1e256};

constexpr auto is_digit(char c) -> bool {
  return c >= '0' && c <= '9';
}

auto scale_by_exponent(double value, int exp) -> double {
  if (exp == 0) {
    return value;
  }
  auto result = value;
  auto n = exp < 0 ? -exp : exp;
  auto i = 0;
  if (exp < 0) {
    for (; n != 0; n >>= 1, ++i) {
      if (n & 0x01) {
        result /= power_table[i];
      }
    }
  } else {
    for (; n != 0; n >>= 1, ++i) {
      if (n & 0x01) {
        result *= power_table[i];
      }
    }
  }
  return result;
}

// Parse a double manually with a custom decimal separator.
// Returns {pointer past consumed chars, error code}.
auto parse_double_fallback(const char* first, const char* last, double& value,
                           char separator)
  -> std::pair<const char*, std::errc> {
  if (first == last) {
    return {first, std::errc::invalid_argument};
  }
  const char* p = first;
  bool negative = false;
  if (*p == '+' || *p == '-') {
    negative = *p == '-';
    ++p;
    if (p == last) {
      return {first, std::errc::invalid_argument};
    }
  }
  // Check for special values: inf, infinity, nan (case-insensitive).
  const auto remaining
    = std::string_view{p, detail::narrow_cast<size_t>(last - p)};
  if (auto n = matches_prefix(remaining, "infinity"); n > 0) {
    value = negative ? -std::numeric_limits<double>::infinity()
                     : std::numeric_limits<double>::infinity();
    return {p + n, std::errc{}};
  }
  if (auto n = matches_prefix(remaining, "inf"); n > 0) {
    value = negative ? -std::numeric_limits<double>::infinity()
                     : std::numeric_limits<double>::infinity();
    return {p + n, std::errc{}};
  }
  if (auto n = matches_prefix(remaining, "nan"); n > 0) {
    const auto nan = std::numeric_limits<double>::quiet_NaN();
    value = negative ? -nan : nan;
    return {p + n, std::errc{}};
  }
  double mantissa = 0;
  int dec_exp = 0;
  bool has_digits = false;
  bool has_separator = false;
  while (p != last && is_digit(*p)) {
    mantissa = mantissa * 10.0 + (*p - '0');
    has_digits = true;
    ++p;
  }
  if (p != last && *p == separator) {
    has_separator = true;
    ++p;
    while (p != last && is_digit(*p)) {
      mantissa = mantissa * 10.0 + (*p - '0');
      if (dec_exp > -max_double_exponent - 1) {
        --dec_exp;
      }
      has_digits = true;
      ++p;
    }
  }
  if (! has_digits) {
    return {first, std::errc::invalid_argument};
  }
  int exp = 0;
  if (p != last && (*p == 'e' || *p == 'E')) {
    const char* exp_mark = p;
    ++p;
    bool exp_negative = false;
    if (p != last && (*p == '+' || *p == '-')) {
      exp_negative = *p == '-';
      ++p;
    }
    if (p == last || ! is_digit(*p)) {
      p = exp_mark;
    } else {
      while (p != last && is_digit(*p)) {
        if (exp <= max_double_exponent + 1) {
          exp = exp * 10 + (*p - '0');
        }
        if (exp > max_double_exponent + 1) {
          exp = max_double_exponent + 1;
        }
        ++p;
      }
      if (exp_negative) {
        exp = -exp;
      }
    }
  }
  if (separator == ',' && has_separator
      && static_cast<size_t>(p - first) > max_double_chars) {
    return {first, std::errc::invalid_argument};
  }
  if (mantissa == 0.0) {
    value = negative ? -0.0 : 0.0;
    return {p, std::errc{}};
  }
  exp += dec_exp;
  if (exp > max_double_exponent) {
    value = negative ? -std::numeric_limits<double>::infinity()
                     : std::numeric_limits<double>::infinity();
    return {p, std::errc{}};
  }
  if (exp < -max_double_exponent) {
    value = negative ? -0.0 : 0.0;
    return {p, std::errc{}};
  }
  auto result = scale_by_exponent(mantissa, exp);
  value = negative ? -result : result;
  return {p, std::errc{}};
}

#endif // !TENZIR_HAS_FLOAT_FROM_CHARS

// -----------------------------------------------------------------------------
// Dispatch: select implementation based on platform capabilities
// -----------------------------------------------------------------------------

auto parse_double_with_separator(const char* first, const char* last,
                                 double& value, char separator)
  -> std::pair<const char*, std::errc> {
#if TENZIR_HAS_FLOAT_FROM_CHARS
  return parse_double_from_chars(first, last, value, separator);
#else
  return parse_double_fallback(first, last, value, separator);
#endif
}

} // namespace

template <char Separator>
template <class Iterator>
bool double_parser<Separator>::parse(Iterator& f, const Iterator& l,
                                     unused_type) const {
  auto result = double{};
  const auto [ptr, ec] = parse_double_with_separator(
    std::to_address(f), std::to_address(l), result, Separator);
  const auto success = ec == std::errc{};
  if (success) {
    f += std::ranges::distance(std::to_address(f), ptr);
  }
  return success;
}

template <char Separator>
template <class Iterator>
bool double_parser<Separator>::parse(Iterator& f, const Iterator& l,
                                     double& a) const {
  const auto [ptr, ec] = parse_double_with_separator(
    std::to_address(f), std::to_address(l), a, Separator);
  const auto success = ec == std::errc{};
  if (success) {
    f += std::ranges::distance(std::to_address(f), ptr);
  }
  return success;
}

template bool
double_parser<'.'>::parse(std::string::iterator&, const std::string::iterator&,
                          unused_type) const;
template bool
double_parser<'.'>::parse(std::string::iterator&, const std::string::iterator&,
                          double&) const;
template bool
double_parser<','>::parse(std::string::iterator&, const std::string::iterator&,
                          unused_type) const;
template bool
double_parser<','>::parse(std::string::iterator&, const std::string::iterator&,
                          double&) const;

template bool double_parser<'.'>::parse(std::string::const_iterator&,
                                        const std::string::const_iterator&,
                                        unused_type) const;
template bool
double_parser<'.'>::parse(std::string::const_iterator&,
                          const std::string::const_iterator&, double&) const;
template bool double_parser<','>::parse(std::string::const_iterator&,
                                        const std::string::const_iterator&,
                                        unused_type) const;
template bool
double_parser<','>::parse(std::string::const_iterator&,
                          const std::string::const_iterator&, double&) const;

template bool
double_parser<'.'>::parse(char const*&, char const* const&, unused_type) const;
template bool
double_parser<'.'>::parse(char const*&, char const* const&, double&) const;
template bool
double_parser<','>::parse(char const*&, char const* const&, unused_type) const;
template bool
double_parser<','>::parse(char const*&, char const* const&, double&) const;

} // namespace tenzir
