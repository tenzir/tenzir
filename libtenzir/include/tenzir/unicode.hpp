//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

/// Unicode code point properties, case folding, and UTF-8 and UTF-16 coding.
///
/// The classification follows Arrow's `utf8_*` compute functions, which the
/// string functions were originally built on, so that their results do not
/// depend on the evaluator.
namespace tenzir::unicode {

/// The properties of one code point.
///
/// - `alpha`: general category `Lu`, `Ll`, `Lt`, `Lm`, or `Lo`.
/// - `numeric`: general category `Nd`, `Nl`, or `No`.
/// - `cased`: general category `Lu`, `Ll`, or `Lt`, or a simple case mapping
///   to a different code point.
/// - `lower`: general category `Ll`, or only a simple uppercase mapping, but
///   not `Lt`. `upper` is the mirror image with `Lu`.
/// - `printable`: assigned, and not in `Cc`, `Cf`, `Cs`, `Co`, `Zs`, `Zl`, or
///   `Zp`. The string functions additionally treat the space as printable.
/// - `space`: general category `Zs`, or bidi class `WS`, `B`, or `S`.
struct Properties {
  bool alpha = false;
  bool numeric = false;
  bool cased = false;
  bool lower = false;
  bool upper = false;
  bool printable = false;
  bool space = false;

  friend auto operator==(Properties const&, Properties const&) -> bool
    = default;
};

/// Looks up the properties of any code point in the character database.
auto properties_of(char32_t code_point) -> Properties;

/// Returns the simple uppercase mapping of any code point.
auto to_upper(char32_t code_point) -> char32_t;

/// Returns the simple lowercase mapping of any code point.
auto to_lower(char32_t code_point) -> char32_t;

/// The properties of an ASCII code point, without a lookup, for loops over
/// ASCII strings that the compiler can vectorize.
constexpr auto ascii_properties(char32_t c) -> Properties {
  auto const upper = c >= 'A' and c <= 'Z';
  auto const lower = c >= 'a' and c <= 'z';
  return {
    .alpha = upper or lower,
    .numeric = c >= '0' and c <= '9',
    .cased = upper or lower,
    .lower = lower,
    .upper = upper,
    .printable = c >= 0x21 and c <= 0x7e,
    // Tab through carriage return, the information separators, and space.
    .space = (c >= 0x09 and c <= 0x0d) or (c >= 0x1c and c <= 0x20),
  };
}

/// The simple uppercase mapping of an ASCII code point.
constexpr auto ascii_upper(char32_t c) -> char32_t {
  return c >= 'a' and c <= 'z' ? c - 0x20 : c;
}

/// The simple lowercase mapping of an ASCII code point.
constexpr auto ascii_lower(char32_t c) -> char32_t {
  return c >= 'A' and c <= 'Z' ? c + 0x20 : c;
}

/// Properties and case mappings of all code points. The Basic Multilingual
/// Plane resolves through lookup tables, all other code points through the
/// character database.
class Table {
public:
  /// Returns the table, building it on first use.
  static auto get() -> Table const&;

  auto properties(char32_t code_point) const -> Properties {
    if (code_point >= size) {
      return properties_of(code_point);
    }
    auto const bits = properties_[code_point];
    return {
      .alpha = (bits & alpha_bit) != 0,
      .numeric = (bits & numeric_bit) != 0,
      .cased = (bits & cased_bit) != 0,
      .lower = (bits & lower_bit) != 0,
      .upper = (bits & upper_bit) != 0,
      .printable = (bits & printable_bit) != 0,
      .space = (bits & space_bit) != 0,
    };
  }

  auto upper(char32_t code_point) const -> char32_t {
    return code_point < size ? upper_[code_point] : to_upper(code_point);
  }

  auto lower(char32_t code_point) const -> char32_t {
    return code_point < size ? lower_[code_point] : to_lower(code_point);
  }

private:
  static constexpr auto size = char32_t{0x10000};
  static constexpr auto alpha_bit = std::uint8_t{1 << 0};
  static constexpr auto numeric_bit = std::uint8_t{1 << 1};
  static constexpr auto cased_bit = std::uint8_t{1 << 2};
  static constexpr auto lower_bit = std::uint8_t{1 << 3};
  static constexpr auto upper_bit = std::uint8_t{1 << 4};
  static constexpr auto printable_bit = std::uint8_t{1 << 5};
  static constexpr auto space_bit = std::uint8_t{1 << 6};

  Table();

  std::array<std::uint8_t, size> properties_;
  std::array<char32_t, size> upper_;
  std::array<char32_t, size> lower_;
};

/// Returns whether `byte` continues a multi-byte UTF-8 sequence.
constexpr auto is_continuation_byte(char byte) -> bool {
  return (static_cast<std::uint8_t>(byte) & 0xc0) == 0x80;
}

/// Returns whether `input` consists of ASCII bytes only. The loop has no
/// early exit, so that it vectorizes.
inline auto is_ascii(std::string_view input) -> bool {
  auto bits = std::uint8_t{0};
  for (auto c : input) {
    bits |= static_cast<std::uint8_t>(c);
  }
  return bits < 0x80;
}

/// Decodes the multi-byte code point that starts at `pos`, which must be
/// before `end`, like `decode`.
inline auto decode_multibyte(char const*& pos, char const* end)
  -> Option<char32_t> {
  auto const remaining = end - pos;
  auto const byte = [&](std::ptrdiff_t i) {
    return static_cast<char32_t>(static_cast<std::uint8_t>(pos[i]));
  };
  auto const continues = [&](std::ptrdiff_t i) {
    return is_continuation_byte(pos[i]);
  };
  auto const lead = byte(0);
  if (lead < 0xc2) {
    return None{};
  }
  if (lead < 0xe0) {
    if (remaining < 2 or not continues(1)) {
      return None{};
    }
    auto const result = ((lead & 0x1f) << 6) | (byte(1) & 0x3f);
    pos += 2;
    return result;
  }
  if (lead < 0xf0) {
    if (remaining < 3 or not continues(1) or not continues(2)
        or (lead == 0xe0 and byte(1) < 0xa0)
        or (lead == 0xed and byte(1) > 0x9f)) {
      return None{};
    }
    auto const result
      = ((lead & 0x0f) << 12) | ((byte(1) & 0x3f) << 6) | (byte(2) & 0x3f);
    pos += 3;
    return result;
  }
  if (lead < 0xf5) {
    if (remaining < 4 or not continues(1) or not continues(2)
        or not continues(3) or (lead == 0xf0 and byte(1) < 0x90)
        or (lead == 0xf4 and byte(1) > 0x8f)) {
      return None{};
    }
    auto const result = ((lead & 0x07) << 18) | ((byte(1) & 0x3f) << 12)
                        | ((byte(2) & 0x3f) << 6) | (byte(3) & 0x3f);
    pos += 4;
    return result;
  }
  return None{};
}

/// Decodes the code point that starts at `pos`, which must be before `end`,
/// and advances `pos` past it. Returns `None` for input that is not valid
/// UTF-8, including overlong encodings and surrogates, and then leaves `pos`
/// unchanged. ASCII resolves inline.
inline auto decode(char const*& pos, char const* end) -> Option<char32_t> {
  auto const lead = static_cast<std::uint8_t>(*pos);
  if (lead < 0x80) {
    ++pos;
    return char32_t{lead};
  }
  return decode_multibyte(pos, end);
}

/// Decodes the code point that ends at `end`, which must be after `begin`, and
/// moves `end` to its start. Returns `None` for input that is not valid UTF-8,
/// and then leaves `end` unchanged.
inline auto decode_backward(char const* begin, char const*& end)
  -> Option<char32_t> {
  auto start = end - 1;
  while (start > begin and end - start < 4 and is_continuation_byte(*start)) {
    --start;
  }
  auto pos = start;
  auto const result = decode(pos, end);
  if (not result or pos != end) {
    return None{};
  }
  end = start;
  return result;
}

/// Returns the number of bytes that encode a valid code point.
constexpr auto encoded_size(char32_t code_point) -> std::size_t {
  return code_point < 0x80      ? 1
         : code_point < 0x800   ? 2
         : code_point < 0x10000 ? 3
                                : 4;
}

/// Writes the UTF-8 encoding of a code point up to U+10FFFF to `output`,
/// which must have room for it, and returns the position after it. A surrogate
/// code point produces its three-byte form, which is not valid UTF-8.
inline auto encode(char32_t code_point, char* output) -> char* {
  auto const put = [&](char32_t value) {
    *output++ = static_cast<char>(value);
  };
  if (code_point < 0x80) {
    put(code_point);
  } else if (code_point < 0x800) {
    put(0xc0 | (code_point >> 6));
    put(0x80 | (code_point & 0x3f));
  } else if (code_point < 0x10000) {
    put(0xe0 | (code_point >> 12));
    put(0x80 | ((code_point >> 6) & 0x3f));
    put(0x80 | (code_point & 0x3f));
  } else {
    put(0xf0 | (code_point >> 18));
    put(0x80 | ((code_point >> 12) & 0x3f));
    put(0x80 | ((code_point >> 6) & 0x3f));
    put(0x80 | (code_point & 0x3f));
  }
  return output;
}

/// Appends the UTF-8 encoding of a code point up to U+10FFFF to `output`, like
/// `encode`.
inline auto append_utf8(std::string& output, char32_t code_point) -> void {
  if (code_point < 0x80) {
    output.push_back(static_cast<char>(code_point));
    return;
  }
  auto buffer = std::array<char, 4>{};
  output.append(buffer.data(), encode(code_point, buffer.data()));
}

/// Returns the UTF-16 byte order mark in byte order `order`.
constexpr auto utf16_byte_order_mark(std::endian order) -> std::string_view {
  return order == std::endian::little ? std::string_view{"\xff\xfe", 2}
                                      : std::string_view{"\xfe\xff", 2};
}

/// UTF-8 text decoded from UTF-16.
struct Utf16Decoded {
  std::string text;
  /// The number of invalid code units replaced with U+FFFD.
  size_t replacements = 0;
};

/// Decodes UTF-16 in byte order `order` into UTF-8. Returns `None` for input
/// with an unpaired surrogate or an odd number of bytes. A byte order mark
/// decodes as U+FEFF, so strip it beforehand if the format has one.
auto decode_utf16(std::span<std::byte const> input, std::endian order)
  -> Option<std::string>;

/// Like `decode_utf16`, but replaces each unpaired surrogate and a trailing odd
/// byte with U+FFFD instead of failing.
auto decode_utf16_lossy(std::span<std::byte const> input, std::endian order)
  -> Utf16Decoded;

/// Encodes UTF-8 as UTF-16 bytes in byte order `order`, without a byte order
/// mark. Returns `None` for input that is not valid UTF-8.
auto encode_utf16(std::string_view input, std::endian order)
  -> Option<std::string>;

/// Like `encode_utf16`, but replaces each byte that does not start a valid
/// UTF-8 sequence with U+FFFD instead of failing.
auto encode_utf16_lossy(std::string_view input, std::endian order)
  -> std::string;

/// Validates whether a string contains well-formed UTF-8.
auto is_valid_utf8(std::string_view bytes) -> bool;

/// Returns the number of trailing bytes that form an incomplete UTF-8 sequence.
auto count_trailing_partial_utf8(std::string_view bytes) -> size_t;

/// Validates whether a string is well-formed UTF-8 after ignoring one trailing
/// incomplete UTF-8 sequence.
auto is_valid_utf8_prefix(std::string_view bytes) -> bool;

/// Counts UTF-8 code points in `value`.
///
/// This assumes valid UTF-8 and counts every byte that does not continue a
/// multi-byte sequence.
[[nodiscard]] constexpr auto
utf8_codepoint_count(std::string_view value) noexcept -> size_t {
  auto result = size_t{0};
  for (auto byte : value) {
    if (not is_continuation_byte(byte)) {
      ++result;
    }
  }
  return result;
}

/// Returns whether `input` encodes exactly one alphanumeric Unicode code
/// point.
[[nodiscard]] auto utf8_code_point_isalnum(std::string_view input) noexcept
  -> bool;

/// Returns the full Unicode case folding of `input`.
///
/// Unlike lowercasing, full case folding maps characters so that
/// case-insensitive comparison works across scripts, e.g. the German "ß"
/// folds to "ss", so "STRASSE" and "straße" fold to the same string. Use this
/// for case-insensitive string comparison rather than `ascii_tolower`.
[[nodiscard]] auto utf8_fold_case(std::string_view input) -> std::string;

/// Finds all non-overlapping, left-to-right occurrences of `folded_pattern`
/// within `input` using full Unicode case folding, and returns their byte
/// ranges `[start, end)` in `input`.
///
/// `folded_pattern` must already be case-folded (see `utf8_fold_case`). Matches
/// are aligned to code point boundaries in `input`, so a pattern of `"s"` does
/// not match half of a `"ß"` (which folds to `"ss"`). An empty pattern yields
/// no matches.
[[nodiscard]] auto
utf8_fold_case_find(std::string_view input, std::string_view folded_pattern)
  -> std::vector<std::pair<size_t, size_t>>;

} // namespace tenzir::unicode
