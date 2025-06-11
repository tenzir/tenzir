//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/base58.hpp"

#include "tenzir/detail/enumerate.hpp"

#include <algorithm>
#include <vector>

namespace tenzir::detail::base58 {

namespace {
constexpr std::string_view ALPHABET
  = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
} // namespace

auto encode(const std::string_view input) -> std::string {
  if (input.empty()) {
    return "";
  }
  std::vector<unsigned char> result(((input.size() * 138) / 100)
                                    + 1); // log(256)/log(58), rounded up
  size_t result_len = 0;
  for (unsigned char byte : input) {
    int carry = byte;
    for (size_t j = 0; j < result_len; ++j) {
      carry += result[j] << 8;
      result[j] = carry % 58;
      carry /= 58;
    }
    while (carry > 0) {
      result[result_len++] = carry % 58;
      carry /= 58;
    }
  }
  // Skip leading zeroes in the input
  size_t leading_zeroes = 0;
  for (unsigned char byte : input) {
    if (byte != 0) {
      break;
    }
    ++leading_zeroes;
  }
  // Add leading zeroes to the result
  std::string encoded(leading_zeroes + result_len, ALPHABET[0]);
  for (size_t i = 0; i < result_len; ++i) {
    encoded[leading_zeroes + i] = ALPHABET[result[result_len - 1 - i]];
  }
  return encoded;
}

auto decode(const std::string_view input) -> caf::expected<std::string> {
  static const std::array<int8_t, 256> ALPHABET_MAP = [] {
    std::array<int8_t, 256> map = {};
    std::fill(std::begin(map), std::end(map), -1);
    for (const auto [i, c] : detail::enumerate<int8_t>(ALPHABET)) {
      map.at(static_cast<unsigned char>(c)) = i;
    }
    return map;
  }();
  std::vector<unsigned char> result(((input.size() * 733) / 1000)
                                    + 1); // log(58)/log(256), rounded up
  size_t result_len = 0;
  for (char c : input) {
    int carry = ALPHABET_MAP.at(static_cast<unsigned char>(c));
    if (carry == -1) [[unlikely]] {
      return caf::make_error(caf::sec::invalid_argument,
                             "invalid base58 character");
    }
    for (size_t j = 0; j < result_len; ++j) {
      carry += result[j] * 58;
      result[j] = carry % 256;
      carry /= 256;
    }
    while (carry > 0) {
      result[result_len++] = carry % 256;
      carry /= 256;
    }
  }
  // Skip leading zeroes in the input
  size_t leading_zeroes = 0;
  for (char c : input) {
    if (c != ALPHABET[0]) {
      break;
    }
    ++leading_zeroes;
  }
  // Add leading zeroes to the result
  std::string decoded(leading_zeroes + result_len, '\0');
  std::reverse_copy(result.begin(), result.begin() + result_len,
                    decoded.begin() + leading_zeroes);
  return decoded;
}

} // namespace tenzir::detail::base58
