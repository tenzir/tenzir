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
#include <cstdint>
#include <string_view>

/// Code point properties and UTF-8 coding for the string functions.
///
/// The classification follows Arrow's `utf8_*` compute functions, which the
/// string functions were originally built on, so that their results do not
/// depend on the evaluator.
namespace tenzir::detail::unicode {

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
    return (byte(i) & 0xc0) == 0x80;
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
  while (start > begin and end - start < 4
         and (static_cast<std::uint8_t>(*start) & 0xc0) == 0x80) {
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

/// Writes the UTF-8 encoding of a valid code point to `output`, which must
/// have room for it, and returns the position after it.
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

} // namespace tenzir::detail::unicode
