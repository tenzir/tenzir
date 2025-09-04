//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"
#include "tenzir/exec/actors.hpp"

namespace tenzir::plan {

/// Configured instance of an operator that is ready for execution.
///
/// Subclasses must register a serialization plugin with the same name.
struct operator_spawn_args {
  operator_spawn_args(caf::actor_system& sys, base_ctx ctx,
                      std::optional<chunk_ptr> restore)
    : sys{sys}, ctx{ctx}, restore{std::move(restore)} {
  }

  caf::actor_system& sys;
  base_ctx ctx;
  // nullopt => fresh start
  // nullptr => no chunk sent for restore point
  // otherwise => chunk contents sent for restore point
  std::optional<chunk_ptr> restore;
};

}; // namespace tenzir::plan
