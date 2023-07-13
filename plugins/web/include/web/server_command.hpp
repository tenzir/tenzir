//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "web/fwd.hpp"

#include <tenzir/command.hpp>

#include <caf/actor_system.hpp>

namespace tenzir::plugins::web {

auto server_command(const tenzir::invocation&, caf::actor_system&)
  -> caf::message;

} // namespace tenzir::plugins::web
