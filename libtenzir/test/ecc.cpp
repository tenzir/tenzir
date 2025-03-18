//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/ecc.hpp>

#include <fmt/core.h>

#include <tenzir/data.hpp>

#include "tenzir/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace tenzir;

TEST(round_trip) {
  constexpr static std::string_view in = "Dies ist ein grossartiger test, der bestimmt funktioniert.";

  const auto keys = ecc::generate_keypair();
  fmt::print("{}\n{}\n",keys->private_key,keys->public_key);
  REQUIRE(keys);
  const auto encrypted = ecc::encrypt(in, keys->public_key);
  if ( not encrypted) {
    fmt::print("\n{}\n",encrypted.error());
  }
  REQUIRE( encrypted );
  fmt::print("\n{}\n",encrypted);
  const auto decrypted = ecc::decrypt(*encrypted, *keys);
  if ( not decrypted) {
    fmt::print("{}",decrypted.error());
  }
  REQUIRE( decrypted );
  REQUIRE_EQUAL(in, *decrypted);
}
