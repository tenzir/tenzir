// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE base64

#include "vast/test/test.hpp"

#include "vast/detail/base64.hpp"

using namespace std::string_view_literals;
using namespace vast::detail;

// Ground truth:
//
//   printf "The quick brown fox jumps over the lazy dog" | base64
//   VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw==

TEST(encode) {
  auto dec = "The quick brown fox jumps over the lazy dog"sv;
  auto enc = "VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw=="sv;
  CHECK_EQUAL(base64::encode(dec), enc);
}

TEST(decode) {
  auto enc = "VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw=="sv;
  auto dec = "The quick brown fox jumps over the lazy dog"sv;
  CHECK_EQUAL(base64::decode(enc), dec);
}
