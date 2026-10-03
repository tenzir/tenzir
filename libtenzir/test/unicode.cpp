//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/unicode.hpp"

#include "tenzir/detail/string.hpp"
#include "tenzir/test/test.hpp"

#include <string>

using namespace tenzir::unicode;

namespace {

/// Whether decoding `bytes` to the end succeeds exactly if they are valid
/// UTF-8.
auto decodes_like_validator(std::string_view bytes) -> bool {
  auto const* pos = bytes.data();
  auto const* const end = bytes.data() + bytes.size();
  while (pos < end and decode(pos, end)) {
  }
  return (pos == end) == is_valid_utf8(bytes);
}

} // namespace

TEST("inline ASCII lookups agree with the character database") {
  for (auto c = char32_t{0}; c < 0x80; ++c) {
    CHECK(ascii_properties(c) == properties_of(c));
    CHECK_EQUAL(ascii_upper(c), to_upper(c));
    CHECK_EQUAL(ascii_lower(c), to_lower(c));
  }
}

TEST("lookup table agrees with the character database") {
  auto const& table = Table::get();
  for (auto c = char32_t{0}; c <= 0x10ffff; ++c) {
    if (table.properties(c) != properties_of(c) or table.upper(c) != to_upper(c)
        or table.lower(c) != to_lower(c)) {
      FAIL("lookup table differs for U+{:04X}", static_cast<uint32_t>(c));
    }
  }
}

TEST("code point properties") {
  auto const& table = Table::get();
  // Titlecase letters are cased, but neither lower nor upper.
  auto const dz = table.properties(U'ǅ');
  CHECK(dz.alpha and dz.cased and not dz.lower and not dz.upper);
  CHECK_EQUAL(table.upper(U'ǅ'), U'Ǆ');
  CHECK_EQUAL(table.lower(U'ǅ'), U'ǆ');
  // Simple case mappings map one code point to one code point, so the sharp s
  // uppercases to the capital sharp s rather than to "SS".
  CHECK_EQUAL(table.upper(U'ß'), U'ẞ');
  CHECK_EQUAL(table.lower(U'ẞ'), U'ß');
  CHECK_EQUAL(table.upper(U'𐐨'), U'𐐀');
  CHECK(table.properties(U'٣').numeric);
  CHECK(table.properties(U'Ⅻ').numeric);
  CHECK(not table.properties(U'Ⅻ').alpha);
  // The next line, line separator, and ideographic space are all whitespace.
  CHECK(table.properties(U'\u0085').space);
  CHECK(table.properties(U'\u2028').space);
  CHECK(table.properties(U'\u3000').space);
  CHECK(not table.properties(U'\u200b').space);
  CHECK(not table.properties(U'\u00ad').printable);
  CHECK(not table.properties(U'\U0010ffff').printable);
  CHECK(table.properties(U'€').printable);
}

TEST("decoding accepts exactly valid UTF-8") {
  auto bytes = std::string{};
  for (auto a = 0; a < 0x100; ++a) {
    bytes = {static_cast<char>(a)};
    CHECK(decodes_like_validator(bytes));
    for (auto b = 0; b < 0x100; ++b) {
      bytes = {static_cast<char>(a), static_cast<char>(b)};
      if (not decodes_like_validator(bytes)) {
        FAIL("decoding disagrees for {:02X} {:02X}", a, b);
      }
      for (auto c = 0; c < 0x100; ++c) {
        bytes
          = {static_cast<char>(a), static_cast<char>(b), static_cast<char>(c)};
        if (not decodes_like_validator(bytes)) {
          FAIL("decoding disagrees for {:02X} {:02X} {:02X}", a, b, c);
        }
      }
      for (auto c : {0x7f, 0x80, 0xbf, 0xc0}) {
        for (auto d : {0x7f, 0x80, 0xbf, 0xc0}) {
          bytes = {static_cast<char>(a), static_cast<char>(b),
                   static_cast<char>(c), static_cast<char>(d)};
          if (not decodes_like_validator(bytes)) {
            FAIL("decoding disagrees for {:02X} {:02X} {:02X} {:02X}", a, b, c,
                 d);
          }
        }
      }
    }
  }
}

TEST("UTF-8 coding round-trips every code point") {
  auto buffer = std::array<char, 4>{};
  for (auto c = char32_t{0}; c <= 0x10ffff; ++c) {
    if (c >= 0xd800 and c <= 0xdfff) {
      continue;
    }
    auto const* end = encode(c, buffer.data());
    auto const* pos = static_cast<char const*>(buffer.data());
    auto const decoded = decode(pos, end);
    auto const* backward = end;
    auto const decoded_backward = decode_backward(buffer.data(), backward);
    if (static_cast<size_t>(end - buffer.data()) != encoded_size(c)
        or not decoded or *decoded != c or pos != end or decoded_backward != c
        or backward != buffer.data()) {
      FAIL("UTF-8 round-trip failed for U+{:04X}", static_cast<uint32_t>(c));
    }
  }
}

TEST("backward decoding rejects invalid UTF-8") {
  auto const check = [](std::string_view bytes) {
    auto const* end = bytes.data() + bytes.size();
    return decode_backward(bytes.data(), end);
  };
  auto const e = check("a\xc3\xa9");
  CHECK(e and *e == U'é');
  auto const emoji = check("\xf0\x9f\x98\x80");
  CHECK(emoji and *emoji == U'😀');
  CHECK(not check("\xa9"));
  CHECK(not check("a\xa9"));
  CHECK(not check("\xc3"));
  CHECK(not check("\xe0\x80\x80"));
  CHECK(not check("\xed\xa0\x80"));
  CHECK(not check("\x80\x80\x80\x80"));
}

TEST("ASCII detection") {
  CHECK(is_ascii(""));
  CHECK(is_ascii("hello, world"));
  CHECK(not is_ascii("héllo"));
  CHECK(not is_ascii(std::string(100, 'a') + "\xff"));
}

TEST("UTF-8 code point alphanumeric classification") {
  CHECK(utf8_code_point_isalnum("a"));
  CHECK(utf8_code_point_isalnum("é"));
  CHECK(utf8_code_point_isalnum("²"));
  CHECK(utf8_code_point_isalnum("Ⅻ"));
  CHECK(not utf8_code_point_isalnum("_"));
  CHECK(not utf8_code_point_isalnum("ab"));
}

TEST("UTF-8 code point counting") {
  CHECK_EQUAL(utf8_codepoint_count(""), 0u);
  CHECK_EQUAL(utf8_codepoint_count("tenzir"), 6u);
  CHECK_EQUAL(utf8_codepoint_count("ä"), 1u);
  CHECK_EQUAL(utf8_codepoint_count("äöü"), 3u);
  CHECK_EQUAL(utf8_codepoint_count("日本語"), 3u);
  CHECK_EQUAL(utf8_codepoint_count("🤖"), 1u);
  CHECK_EQUAL(utf8_codepoint_count("a🤖b"), 3u);
}

namespace {

auto bytes(std::string_view text) -> std::span<std::byte const> {
  return std::as_bytes(std::span{text});
}

} // namespace

TEST("UTF-16 coding round-trips every code point") {
  for (auto order : {std::endian::little, std::endian::big}) {
    auto text = std::string{};
    for (auto code_point = char32_t{0}; code_point <= 0x10ffff; ++code_point) {
      if (code_point >= 0xd800 and code_point <= 0xdfff) {
        continue;
      }
      append_utf8(text, code_point);
    }
    auto const encoded = encode_utf16(text, order);
    REQUIRE(encoded);
    CHECK_EQUAL(decode_utf16(bytes(*encoded), order), text);
  }
}

TEST("UTF-16 coding respects the byte order") {
  // U+1F642 needs a surrogate pair: D83D DE42.
  auto const text = std::string_view{"A\xf0\x9f\x99\x82"};
  CHECK_EQUAL(encode_utf16(text, std::endian::little),
              std::string("A\0\x3d\xd8\x42\xde", 6));
  CHECK_EQUAL(encode_utf16(text, std::endian::big),
              std::string("\0A\xd8\x3d\xde\x42", 6));
  CHECK_EQUAL(utf16_byte_order_mark(std::endian::little), "\xff\xfe");
  CHECK_EQUAL(utf16_byte_order_mark(std::endian::big), "\xfe\xff");
}

TEST("UTF-16 decoding rejects or replaces invalid input") {
  // A lone high surrogate, a lone low surrogate, and a trailing odd byte.
  for (auto input : {std::string_view{"a\0\x00\xd8", 4},
                     std::string_view{"\x00\xdc"
                                      "b\0",
                                      4},
                     std::string_view{"a\0b", 3}}) {
    CHECK(not decode_utf16(bytes(input), std::endian::little));
  }
  auto const lossy = decode_utf16_lossy(bytes(std::string_view{"a\0\x00\xd8"
                                                               "b\0\x01",
                                                               7}),
                                        std::endian::little);
  CHECK_EQUAL(lossy.text, "a\xef\xbf\xbd"
                          "b\xef\xbf\xbd");
  CHECK_EQUAL(lossy.replacements, size_t{2});
  // A byte order mark is not stripped.
  CHECK_EQUAL(decode_utf16(bytes(std::string_view{"\xff\xfe"
                                                  "a\0",
                                                  4}),
                           std::endian::little),
              "\xef\xbb\xbf"
              "a");
}

TEST("UTF-16 encoding rejects or replaces invalid UTF-8") {
  auto const invalid = std::string_view{"a\xc3"
                                        "b\xed\xa0\x80"};
  CHECK(not encode_utf16(invalid, std::endian::little));
  auto const lossy = encode_utf16_lossy(invalid, std::endian::little);
  CHECK_EQUAL(decode_utf16(bytes(lossy), std::endian::little),
              "a\xef\xbf\xbd"
              "b\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd");
}

TEST("continuation bytes") {
  CHECK(not is_continuation_byte('a'));
  CHECK(not is_continuation_byte('\xc3'));
  CHECK(is_continuation_byte('\xa4'));
  CHECK(not is_continuation_byte('\xf0'));
}
