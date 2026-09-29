//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/unicode.hpp"

#include "tenzir/detail/assert.hpp"

#include <unicode/uchar.h>

namespace tenzir::detail::unicode {

namespace {

/// Whether `category` is one of `categories`. Unassigned code points belong to
/// no category, like in Arrow.
template <class... Categories>
auto is_any_of(int8_t category, Categories... categories) -> bool {
  return category != U_UNASSIGNED and ((category == categories) or ...);
}

} // namespace

auto properties_of(char32_t code_point) -> Properties {
  auto const c = static_cast<UChar32>(code_point);
  auto const category = static_cast<int8_t>(u_charType(c));
  auto const has_upper = to_upper(code_point) != code_point;
  auto const has_lower = to_lower(code_point) != code_point;
  auto const titlecase = is_any_of(category, U_TITLECASE_LETTER);
  auto const direction = u_charDirection(c);
  return {
    .alpha = is_any_of(category, U_UPPERCASE_LETTER, U_LOWERCASE_LETTER,
                       U_TITLECASE_LETTER, U_MODIFIER_LETTER, U_OTHER_LETTER),
    .numeric = is_any_of(category, U_DECIMAL_DIGIT_NUMBER, U_LETTER_NUMBER,
                         U_OTHER_NUMBER),
    .cased = is_any_of(category, U_UPPERCASE_LETTER, U_LOWERCASE_LETTER,
                       U_TITLECASE_LETTER)
             or has_upper or has_lower,
    .lower
    = (is_any_of(category, U_LOWERCASE_LETTER) or (has_upper and not has_lower))
      and not titlecase,
    .upper
    = (is_any_of(category, U_UPPERCASE_LETTER) or (not has_upper and has_lower))
      and not titlecase,
    .printable
    = category != U_UNASSIGNED
      and not is_any_of(category, U_CONTROL_CHAR, U_FORMAT_CHAR, U_SURROGATE,
                        U_PRIVATE_USE_CHAR, U_SPACE_SEPARATOR, U_LINE_SEPARATOR,
                        U_PARAGRAPH_SEPARATOR),
    .space = is_any_of(category, U_SPACE_SEPARATOR)
             or direction == U_WHITE_SPACE_NEUTRAL
             or direction == U_BLOCK_SEPARATOR
             or direction == U_SEGMENT_SEPARATOR,
  };
}

auto to_upper(char32_t code_point) -> char32_t {
  // Unicode defines no simple uppercase mapping for the sharp s, but utf8proc,
  // which the string functions were built on, maps it to the capital sharp s.
  if (code_point == U'\u00df') {
    return U'\u1e9e';
  }
  return static_cast<char32_t>(u_toupper(static_cast<UChar32>(code_point)));
}

auto to_lower(char32_t code_point) -> char32_t {
  return static_cast<char32_t>(u_tolower(static_cast<UChar32>(code_point)));
}

Table::Table() {
  for (auto c = char32_t{0}; c < size; ++c) {
    auto const p = properties_of(c);
    properties_[c] = static_cast<std::uint8_t>(
      (p.alpha ? alpha_bit : 0) | (p.numeric ? numeric_bit : 0)
      | (p.cased ? cased_bit : 0) | (p.lower ? lower_bit : 0)
      | (p.upper ? upper_bit : 0) | (p.printable ? printable_bit : 0)
      | (p.space ? space_bit : 0));
    upper_[c] = to_upper(c);
    lower_[c] = to_lower(c);
    // Case conversion sizes its output for at most 50% growth. Code points
    // beyond the table encode in 4 bytes and map to at most 4 bytes.
    TENZIR_ASSERT(2 * encoded_size(upper_[c]) <= 3 * encoded_size(c));
    TENZIR_ASSERT(2 * encoded_size(lower_[c]) <= 3 * encoded_size(c));
  }
}

auto Table::get() -> Table const& {
  static auto const table = Table{};
  return table;
}

} // namespace tenzir::detail::unicode
