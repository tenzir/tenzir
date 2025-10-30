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

struct restore_t {
  chunk_ptr chunk;
  exec::checkpoint_reader_actor checkpoint_reader;

  friend auto inspect(auto& f, restore_t& x) -> bool {
    return f.object(x).fields(f.field("chunk", x.chunk),
                              f.field("checkpoint_reader",
                                      x.checkpoint_reader));
  }
};

/// Configured instance of an operator that is ready for execution.
///
/// Subclasses must register a serialization plugin with the same name.
struct operator_spawn_args {
  operator_spawn_args(caf::actor_system& sys, base_ctx ctx,
                      std::optional<restore_t> restore)
    : sys{sys}, ctx{ctx}, restore{std::move(restore)} {
  }

  caf::actor_system& sys;
  base_ctx ctx;
  // nullopt => fresh start
  // nullptr => no chunk sent for restore point
  // otherwise => chunk contents sent for restore point
  std::optional<restore_t> restore;
};

}; // namespace tenzir::plan
