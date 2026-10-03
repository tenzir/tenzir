//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/unicode.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"

#include <unicode/ucasemap.h>
#include <unicode/uchar.h>
#include <unicode/utf8.h>
#include <unicode/utypes.h>

#include <simdjson.h>

namespace tenzir::unicode {

namespace {

/// Whether `category` is one of `categories`. Unassigned code points belong to
/// no category, like in Arrow.
template <class... Categories>
auto is_any_of(int8_t category, Categories... categories) -> bool {
  return category != U_UNASSIGNED and ((category == categories) or ...);
}

constexpr auto replacement_character = char32_t{0xfffd};

auto append_utf16_unit(std::string& output, char32_t unit, std::endian order)
  -> void {
  auto const high = static_cast<char>(unit >> 8);
  auto const low = static_cast<char>(unit & 0xff);
  if (order == std::endian::little) {
    output.push_back(low);
    output.push_back(high);
  } else {
    output.push_back(high);
    output.push_back(low);
  }
}

auto append_utf16(std::string& output, char32_t code_point, std::endian order)
  -> void {
  if (code_point < 0x10000) {
    append_utf16_unit(output, code_point, order);
    return;
  }
  auto const offset = code_point - 0x10000;
  append_utf16_unit(output, 0xd800 + (offset >> 10), order);
  append_utf16_unit(output, 0xdc00 + (offset & 0x3ff), order);
}

template <bool Lossy>
auto decode_utf16_impl(std::span<std::byte const> input, std::endian order)
  -> Option<Utf16Decoded> {
  auto result = Utf16Decoded{};
  result.text.reserve(input.size() / 2);
  auto const unit_at = [&](size_t offset) {
    auto const first = std::to_integer<char32_t>(input[offset]);
    auto const second = std::to_integer<char32_t>(input[offset + 1]);
    return order == std::endian::little ? first | (second << 8)
                                        : (first << 8) | second;
  };
  auto offset = size_t{0};
  while (offset + 1 < input.size()) {
    auto const unit = unit_at(offset);
    offset += 2;
    if (unit < 0xd800 or unit > 0xdfff) {
      append_utf8(result.text, unit);
      continue;
    }
    // A high surrogate must be followed by a low surrogate.
    if (unit < 0xdc00 and offset + 1 < input.size()) {
      auto const low = unit_at(offset);
      if (low >= 0xdc00 and low <= 0xdfff) {
        offset += 2;
        append_utf8(result.text,
                    0x10000 + ((unit - 0xd800) << 10) + (low - 0xdc00));
        continue;
      }
    }
    if constexpr (Lossy) {
      append_utf8(result.text, replacement_character);
      ++result.replacements;
    } else {
      return None{};
    }
  }
  if (offset < input.size()) {
    if constexpr (Lossy) {
      append_utf8(result.text, replacement_character);
      ++result.replacements;
    } else {
      return None{};
    }
  }
  return result;
}

template <bool Lossy>
auto encode_utf16_impl(std::string_view input, std::endian order)
  -> Option<std::string> {
  auto result = std::string{};
  result.reserve(input.size() * 2);
  auto const* pos = input.data();
  auto const* const end = input.data() + input.size();
  while (pos < end) {
    auto code_point = decode(pos, end);
    if (not code_point) {
      if constexpr (Lossy) {
        ++pos;
        code_point = replacement_character;
      } else {
        return None{};
      }
    }
    append_utf16(result, *code_point, order);
  }
  return result;
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

auto decode_utf16(std::span<std::byte const> input, std::endian order)
  -> Option<std::string> {
  return decode_utf16_impl<false>(input, order).map([](Utf16Decoded decoded) {
    return std::move(decoded.text);
  });
}

auto decode_utf16_lossy(std::span<std::byte const> input, std::endian order)
  -> Utf16Decoded {
  auto result = decode_utf16_impl<true>(input, order);
  TENZIR_ASSERT(result);
  return std::move(*result);
}

auto encode_utf16(std::string_view input, std::endian order)
  -> Option<std::string> {
  return encode_utf16_impl<false>(input, order);
}

auto encode_utf16_lossy(std::string_view input, std::endian order)
  -> std::string {
  auto result = encode_utf16_impl<true>(input, order);
  TENZIR_ASSERT(result);
  return std::move(*result);
}

auto is_valid_utf8(std::string_view bytes) -> bool {
  return simdjson::validate_utf8(bytes.data(), bytes.size());
}

auto count_trailing_partial_utf8(std::string_view bytes) -> size_t {
  if (bytes.empty()) {
    return 0;
  }
  auto buf = reinterpret_cast<const uint8_t*>(bytes.data());
  auto len = bytes.size();
  if (buf[len - 1] >= 0xC0) {
    return 1;
  }
  if (len >= 2 and buf[len - 2] >= 0xE0) {
    return 2;
  }
  if (len >= 3 and buf[len - 3] >= 0xF0) {
    return 3;
  }
  return 0;
}

auto is_valid_utf8_prefix(std::string_view bytes) -> bool {
  auto partial = count_trailing_partial_utf8(bytes);
  if (partial == 0) {
    return is_valid_utf8(bytes);
  }
  bytes.remove_suffix(partial);
  return is_valid_utf8(bytes);
}

auto utf8_code_point_isalnum(std::string_view input) noexcept -> bool {
  auto const length = detail::narrow<int32_t>(input.size());
  auto offset = int32_t{0};
  auto code_point = UChar32{};
  U8_NEXT(input.data(), offset, length, code_point);
  auto const category = u_charType(code_point);
  return offset == length
         and (u_isalnum(code_point) or category == U_LETTER_NUMBER
              or category == U_OTHER_NUMBER);
}

auto utf8_fold_case(std::string_view input) -> std::string {
  if (input.empty()) {
    return {};
  }
  auto status = U_ZERO_ERROR;
  auto* csm = ucasemap_open("", 0, &status);
  if (U_FAILURE(status)) {
    return std::string{input};
  }
  const auto src_length = detail::narrow_cast<int32_t>(input.size());
  auto result = std::string{};
  // Full case folding can grow the string (e.g. "ß" -> "ss"), so size up front
  // and retry once if the initial buffer is too small.
  result.resize(input.size());
  auto dest_length
    = ucasemap_utf8FoldCase(csm, result.data(),
                            detail::narrow_cast<int32_t>(result.size()),
                            input.data(), src_length, &status);
  if (status == U_BUFFER_OVERFLOW_ERROR) {
    status = U_ZERO_ERROR;
    result.resize(detail::narrow_cast<size_t>(dest_length));
    dest_length
      = ucasemap_utf8FoldCase(csm, result.data(),
                              detail::narrow_cast<int32_t>(result.size()),
                              input.data(), src_length, &status);
  }
  ucasemap_close(csm);
  if (U_FAILURE(status)) {
    return std::string{input};
  }
  result.resize(detail::narrow_cast<size_t>(dest_length));
  return result;
}

auto utf8_fold_case_find(std::string_view input,
                         std::string_view folded_pattern)
  -> std::vector<std::pair<size_t, size_t>> {
  auto result = std::vector<std::pair<size_t, size_t>>{};
  if (folded_pattern.empty() or input.empty()) {
    return result;
  }
  // Decompose `input` into its code points, recording each one's byte range and
  // its full case folding. Folding per code point matches folding the whole
  // string for Unicode's default full case folding, while letting us map match
  // boundaries back to the original byte offsets.
  struct code_point {
    size_t start;
    size_t end;
    std::string folded;
  };
  auto cps = std::vector<code_point>{};
  for (size_t i = 0; i < input.size();) {
    auto j = i + 1;
    while (j < input.size() and is_continuation_byte(input[j])) {
      ++j;
    }
    cps.push_back({i, j, utf8_fold_case(input.substr(i, j - i))});
    i = j;
  }
  for (size_t i = 0; i < cps.size();) {
    auto acc = std::string{};
    auto matched_end = std::string_view::npos;
    auto next = i;
    for (auto j = i; j < cps.size(); ++j) {
      acc += cps[j].folded;
      if (acc.size() > folded_pattern.size()
          or folded_pattern.compare(0, acc.size(), acc) != 0) {
        // The accumulated folding overshoots or diverges from the pattern.
        break;
      }
      if (acc.size() == folded_pattern.size()) {
        matched_end = cps[j].end;
        next = j + 1;
        break;
      }
    }
    if (matched_end != std::string_view::npos) {
      result.emplace_back(cps[i].start, matched_end);
      i = next;
    } else {
      ++i;
    }
  }
  return result;
}

} // namespace tenzir::unicode
