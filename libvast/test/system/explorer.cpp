//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE explorer

#include "vast/system/spawn_explorer.hpp"
#include "vast/test/test.hpp"
#include "vast/time.hpp"

#include <caf/settings.hpp>

using namespace std::chrono_literals;

TEST(explorer config) {
  {
    MESSAGE("Specifying no options at all is not allowed.");
    caf::settings settings;
    auto& vast = settings["vast"].as_dictionary();
    [[maybe_unused]] auto& explore = vast["explore"].as_dictionary();
    CHECK_NOT_EQUAL(vast::system::explorer_validate_args(settings), caf::none);
  }

  {
    MESSAGE("Specifying only time is allowed, as long as it is > 0.");
    caf::settings settings;
    auto& vast = settings["vast"].as_dictionary();
    auto& explore = vast["explore"].as_dictionary();
    explore["before"] = "0s";
    explore["after"] = "0s";
    CHECK_NOT_EQUAL(vast::system::explorer_validate_args(settings), caf::none);
    explore["after"] = "10s";
    std::cerr << caf::to_string(settings) << std::endl;
    CHECK_EQUAL(vast::system::explorer_validate_args(settings), caf::none);
  }

  {
    MESSAGE("Specifying only 'by' is allowed.");
    caf::settings settings;
    auto& vast = settings["vast"].as_dictionary();
    auto& explore = vast["explore"].as_dictionary();
    explore["by"] = "0s";
    CHECK_EQUAL(vast::system::explorer_validate_args(settings), caf::none);
  }

  {
    MESSAGE("Malformed input is not allowed.");
    caf::settings settings;
    auto& vast = settings["vast"].as_dictionary();
    auto& explore = vast["after"].as_dictionary();
    explore["by"] = "MIP = RE";
    CHECK_NOT_EQUAL(vast::system::explorer_validate_args(settings), caf::none);
  }

  {
    MESSAGE("Specifying all options is fine.");
    caf::settings settings;
    auto& vast = settings["vast"].as_dictionary();
    auto& explore = vast["explore"].as_dictionary();
    explore["before"] = vast::duration{10s};
    explore["after"] = vast::duration{10s};
    explore["by"] = "foo";
    CHECK_EQUAL(vast::system::explorer_validate_args(settings), caf::none);
  }
}
