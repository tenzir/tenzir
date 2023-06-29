//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/defaults.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/settings.hpp>

#include <random>
#include <string>

namespace vast::defaults::import {

size_t test::seed(const caf::settings& options) {
  constexpr auto key = std::string_view{"tenzir.import.test.seed"};
  if (auto val = caf::get_if<caf::config_value::integer>(&options, key))
    return *val;
  std::random_device rd;
  return rd();
}

} // namespace vast::defaults::import
