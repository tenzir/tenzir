//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/base58.hpp"

#include "tenzir/test/test.hpp"

#include <string_view>

using namespace std::string_view_literals;
using namespace tenzir::detail;

// Ground truth:
//
//   printf "The quick brown fox jumps over the lazy dog" | base58
//   7DdiPPYtxLjCD3wA1po2rvZHTDYjkZYiEtazrfiwJcwnKCizhGFhBGHeRdx

TEST("base58 encode") {
  auto dec = "The quick brown fox jumps over the lazy dog"sv;
  auto enc = "7DdiPPYtxLjCD3wA1po2rvZHTDYjkZYiEtazrfiwJcwnKCizhGFhBGHeRdx"sv;
  CHECK_EQUAL(base58::encode(dec), enc);
}

TEST("base58 decode") {
  auto enc = "7DdiPPYtxLjCD3wA1po2rvZHTDYjkZYiEtazrfiwJcwnKCizhGFhBGHeRdx"sv;
  auto dec = "The quick brown fox jumps over the lazy dog"sv;
  auto base58_dec = base58::decode(enc);
  REQUIRE(base58_dec);
  CHECK_EQUAL(*base58_dec, dec);
}
