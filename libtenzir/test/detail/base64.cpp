//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/base64.hpp"

#include "tenzir/test/test.hpp"

#include <string_view>

using namespace std::string_view_literals;
using namespace tenzir::detail;

// Ground truth:
//
//   printf "The quick brown fox jumps over the lazy dog" | base64
//   VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw==

TEST("encode") {
  auto dec = "The quick brown fox jumps over the lazy dog"sv;
  auto enc = "VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw=="sv;
  CHECK_EQUAL(base64::encode(dec), enc);
}

TEST("decode") {
  auto enc = "VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw=="sv;
  auto dec = "The quick brown fox jumps over the lazy dog"sv;
  const auto decoded = base64::try_decode(enc);
  CHECK(decoded.has_value());
  CHECK_EQUAL(*decoded, dec);
}

TEST("decode unpadded") {
  auto const one_byte = base64::try_decode("TQ"sv);
  REQUIRE(one_byte.has_value());
  CHECK_EQUAL(*one_byte, "M"sv);
  auto const two_bytes = base64::try_decode("TWE"sv);
  REQUIRE(two_bytes.has_value());
  CHECK_EQUAL(*two_bytes, "Ma"sv);
  auto const four_bytes = base64::try_decode("TWFuTQ"sv);
  REQUIRE(four_bytes.has_value());
  CHECK_EQUAL(*four_bytes, "ManM"sv);
}
