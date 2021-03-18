// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/defaults.hpp"

#include <random>
#include <string>

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/settings.hpp>

namespace vast::defaults::import {

size_t test::seed(const caf::settings& options) {
  if (auto val = caf::get_if<size_t>(&options, "vast.import.test.seed"))
    return *val;
  std::random_device rd;
  return rd();
}

} // namespace vast::defaults::import
