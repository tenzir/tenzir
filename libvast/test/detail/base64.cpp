//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/base64.hpp"

#include "vast/test/test.hpp"

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
