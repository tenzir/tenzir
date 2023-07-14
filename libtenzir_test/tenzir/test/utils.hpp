//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/plugin.hpp>
#include <tenzir/test/test.hpp>

namespace tenzir::test {

inline void reinit_tenzir_language(const record& config) {
  // We know that the this plugin is safe to initialize multiple times.
  auto language_plugin = const_cast<plugin*>(plugins::find("Tenzir"));
  REQUIRE(language_plugin);
  REQUIRE_EQUAL(language_plugin->initialize({}, config), caf::error{});
}

} // namespace tenzir::test
