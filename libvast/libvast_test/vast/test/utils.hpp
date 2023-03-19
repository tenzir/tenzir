//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/plugin.hpp>
#include <vast/test/test.hpp>

namespace vast::test {

inline void reinit_vast_language(const record& config) {
  // We know that the this plugin is safe to initialize multiple times.
  auto language_plugin = const_cast<plugin*>(plugins::find("VAST"));
  REQUIRE(language_plugin);
  REQUIRE_EQUAL(language_plugin->initialize({}, config), caf::error{});
}

} // namespace vast::test
