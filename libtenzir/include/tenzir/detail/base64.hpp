//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstddef>
#include <ranges>
#include <string>
#include <string_view>

/// [Base64](https://en.wikipedia.org/wiki/Base64) coding.
namespace tenzir::detail::base64 {

constexpr signed char alphabet[] = "ABCDEFGHIJKLMNOP"
                                   "QRSTUVWXYZabcdef"
                                   "ghijklmnopqrstuv"
                                   "wxyz0123456789+/";

constexpr signed char inverse[] = {
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //   0-15
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //  16-31
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, //  32-47
  52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, //  48-63
  -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, //  64-79
  15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, //  80-95
  -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, //  96-111
  41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1, // 112-127
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 128-143
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 144-159
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 160-175
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 176-191
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 192-207
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 208-223
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 224-239
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1  // 240-255
};

/// @see decoded_size
inline constexpr size_t encoded_size(size_t n) {
  return 4 * ((n + 2) / 3);
}

/// @see encoded_size
inline constexpr size_t decoded_size(size_t n) {
  return n / 4 * 3;
}

// Base64-encodes a sequence of bytes.
// @param dst The destination buffer.
// @param src The beginning of the bytes to encode.
// @param len The nubmer of bytes to encode starting at *src*.
// @returns The number of bytes written to *out* without NUL-termination.
// @requires The destination buffers has at least `encoded_size(len)` bytes.
// @see encoded_size
size_t encode(void* dst, const void* src, size_t len);

/// Base64-encodes a string.
/// @param str The string to encode.
/// @returns The Base64-encoded version of *str*.
std::string encode(std::string_view str);

template <class R>
  requires std::ranges::contiguous_range<R>
           && std::same_as<std::ranges::range_value_t<R>, std::byte>
std::string encode(R&& range) {
  return encode(
    std::string_view{reinterpret_cast<const char*>(std::ranges::data(range)),
                     std::ranges::size(range)});
}

// Decodes a Base64-encoded string into a sequence of bytes.
// @param dst The destination buffer.
// @param src The beginning of the bytes to decode.
// @param len The nubmer of bytes to decode starting at *src*.
// @returns The number of bytes written to *out* and read from *src*.
// @requires The destination buffers has at least `decoded_size(len)` bytes.
// @see decoded_size
std::pair<size_t, size_t> decode(void* dst, char const* src, size_t len);

/// Tries to decode a Base64-encoded string. Returns `std::nullopt` if the input
/// string is not valid.
template <class T = std::string>
auto try_decode(std::string_view str) -> std::optional<T> {
  auto result = T{};
  result.resize(decoded_size(str.size()));
  auto [written, read] = decode(result.data(), str.data(), str.size());
  while (read < str.size()) {
    if (str[read] != '=') {
      return std::nullopt;
    }
    read += 1;
  }
  result.resize(written);
  return result;
}

} // namespace tenzir::detail::base64
