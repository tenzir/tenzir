//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret_resolution.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::string_view_literals;

TEST(censor) {
  constexpr static auto needle_sv = "needle"sv;
  constexpr static auto noddle_sv = "noodle"sv;

  auto needle = resolved_secret_value{
    ecc::cleansing_blob{
      reinterpret_cast<const std::byte*>(needle_sv.data()),
      reinterpret_cast<const std::byte*>(needle_sv.data()) + needle_sv.size(),
    },
    false,
  };
  auto noodle = resolved_secret_value{
    ecc::cleansing_blob{
      reinterpret_cast<const std::byte*>(noddle_sv.data()),
      reinterpret_cast<const std::byte*>(noddle_sv.data()) + noddle_sv.size(),
    },
    false,
  };
  const auto censor = secret_censor{
    .secrets = {std::move(needle), std::move(noodle)}};

  REQUIRE_EQUAL(censor.censor("needle"), "***");
  REQUIRE_EQUAL(censor.censor("need"), "need");
  REQUIRE_EQUAL(censor.censor("haystack"), "haystack");
  REQUIRE_EQUAL(censor.censor("haystack needle haystack"),
                "haystack *** haystack");
  REQUIRE_EQUAL(censor.censor("neneedle"), "ne***");
  REQUIRE_EQUAL(censor.censor("needle needle"), "*** ***");
}
