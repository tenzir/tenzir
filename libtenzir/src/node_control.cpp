//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/node_control.hpp"

#include "tenzir/configuration.hpp"
#include "tenzir/defaults.hpp"

#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

auto node_connection_timeout(const caf::settings& options) -> caf::timespan {
  auto timeout_value = get_or_duration(options, "tenzir.connection-timeout",
                                       defaults::node_connection_timeout);
  if (!timeout_value) {
    TENZIR_ERROR("client failed to read connection-timeout: {}",
                 timeout_value.error());
    return caf::timespan{defaults::node_connection_timeout};
  }
  auto timeout = caf::timespan{*timeout_value};
  if (timeout == timeout.zero())
    return caf::infinite;
  return timeout;
}

} // namespace tenzir
