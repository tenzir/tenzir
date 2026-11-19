//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/defaults.hpp"

#include "tenzir/nova_flag.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/settings.hpp>

#include <random>
#include <string>
#include <string_view>

namespace tenzir::defaults::import {

auto parser_batch_size() -> size_t {
  return nova_enabled() ? nova_parser_batch_size : table_slice_size;
}

size_t test::seed(const caf::settings& options) {
  constexpr auto key = std::string_view{"tenzir.import.test.seed"};
  if (auto val = caf::get_if<caf::config_value::integer>(&options, key)) {
    return *val;
  }
  std::random_device rd;
  return rd();
}

} // namespace tenzir::defaults::import
