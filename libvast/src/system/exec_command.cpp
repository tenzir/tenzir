//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/exec_command.hpp"
#include "vast/command.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>

using namespace caf;

namespace vast::system {

caf::message exec_command(const invocation& inv, caf::actor_system& sys) {
  auto split_args = detail::split(inv.arguments.front(), "|");
  auto trimmed_args = std::vector<std::string>{};
  for (auto arg : split_args) {
    trimmed_args.emplace_back(detail::trim(arg));
  }

  return {};
}

} // namespace vast::system
