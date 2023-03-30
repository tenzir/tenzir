//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "web/fwd.hpp"

#include <vast/command.hpp>

#include <caf/actor_system.hpp>

namespace vast::plugins::web {

auto server_command(const vast::invocation&, caf::actor_system&)
  -> caf::message;

} // namespace vast::plugins::web
