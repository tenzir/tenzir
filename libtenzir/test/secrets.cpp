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

TEST("censor") {
  constexpr static auto needle_sv = "needle"sv;
  constexpr static auto noddle_sv = "noodle"sv;
  constexpr static auto stars_sv = "*** stars"sv;
  constexpr static auto star_sv = "*"sv;

  auto needle = ecc::cleansing_blob{
    reinterpret_cast<const std::byte*>(needle_sv.data()),
    reinterpret_cast<const std::byte*>(needle_sv.data()) + needle_sv.size(),
  };
  auto noodle = ecc::cleansing_blob{
    reinterpret_cast<const std::byte*>(noddle_sv.data()),
    reinterpret_cast<const std::byte*>(noddle_sv.data()) + noddle_sv.size(),
  };
  auto stars = ecc::cleansing_blob{
    reinterpret_cast<const std::byte*>(stars_sv.data()),
    reinterpret_cast<const std::byte*>(stars_sv.data()) + stars_sv.size(),
  };
  auto star = ecc::cleansing_blob{
    reinterpret_cast<const std::byte*>(star_sv.data()),
    reinterpret_cast<const std::byte*>(star_sv.data()) + star_sv.size(),
  };
  const auto censor
    = secret_censor{.secrets = {std::move(needle), std::move(noodle),
                                std::move(stars), std::move(star)}};

  REQUIRE_EQUAL(censor.censor("needle"), "***");
  REQUIRE_EQUAL(censor.censor("need"), "need");
  REQUIRE_EQUAL(censor.censor("haystack"), "haystack");
  REQUIRE_EQUAL(censor.censor("haystack needle haystack"),
                "haystack *** haystack");
  REQUIRE_EQUAL(censor.censor("neneedle"), "ne***");
  REQUIRE_EQUAL(censor.censor("needle needle"), "*** ***");
  REQUIRE_EQUAL(censor.censor(std::string{stars_sv} + " ***"), "*** ***");
  REQUIRE_EQUAL(censor.censor("*"), "***");
  REQUIRE_EQUAL(censor.censor("**"), "******");
  REQUIRE_EQUAL(censor.censor("*pike*"), "***pike***");
}
