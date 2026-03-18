//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/default_configuration.hpp"

#include <caf/config_value.hpp>

#include <chrono>

namespace tenzir {

default_configuration::default_configuration() {
  // Tweak default logging options.
  using namespace std::chrono_literals;
  set("caf.logger.file.excluded-components",
      caf::make_config_value_list("caf", "caf_flow"));
  set("caf.logger.console.excluded-components",
      caf::make_config_value_list("caf", "caf_flow"));
  set("caf.middleman.connection-timeout", caf::timespan{120s});
  set("caf.middleman.enable-automatic-connections", true);
  set("caf.middleman.app-identifiers", caf::make_config_value_list("tenzir"));
  set("caf.scheduler.max-throughput", 1);
}

} // namespace tenzir
