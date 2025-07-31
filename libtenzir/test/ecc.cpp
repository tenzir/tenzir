//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/test/test.hpp"

#include <tenzir/data.hpp>
#include <tenzir/ecc.hpp>

#include <fmt/core.h>

using namespace tenzir;

TEST("round_trip") {
  constexpr static std::string_view in
    = "Dies ist ein grossartiger test, der bestimmt funktioniert.";
  const auto keys = ecc::generate_keypair();
  REQUIRE(keys);
  const auto encrypted = ecc::encrypt(in, keys->public_key);
  REQUIRE(encrypted);
  const auto decrypted = ecc::decrypt(*encrypted, *keys);
  REQUIRE(decrypted);
  const auto sv = std::string_view{
    reinterpret_cast<const char*>(decrypted->data()),
    reinterpret_cast<const char*>(decrypted->data() + decrypted->size()),
  };
  REQUIRE_EQUAL(in, sv);
  auto decrypted_string = ecc::decrypt_string(*encrypted, *keys);
  REQUIRE(decrypted);
  REQUIRE_EQUAL(in, *decrypted_string);
}
