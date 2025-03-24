//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"
#include "tenzir/operator_actor.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/uuid.hpp"

namespace tenzir::plan {

/// Configured instance of an operator that is ready for execution.
///
/// Subclasses must register a serialization plugin with the same name.
class operator_base {
public:
  struct spawn_args {
    spawn_args(caf::actor_system& sys, base_ctx ctx,
               exec::checkpoint_receiver_actor checkpoint_receiver,
               exec::operator_shutdown_actor operator_shutdown,
               exec::operator_stop_actor operator_stop,
               std::optional<chunk_ptr> restore)
      : sys{sys},
        ctx{ctx},
        checkpoint_receiver{std::move(checkpoint_receiver)},
        operator_shutdown{std::move(operator_shutdown)},
        operator_stop{std::move(operator_stop)},
        restore{std::move(restore)} {
    }

    caf::actor_system& sys;
    base_ctx ctx;
    exec::checkpoint_receiver_actor checkpoint_receiver;
    exec::operator_shutdown_actor operator_shutdown;
    exec::operator_stop_actor operator_stop;

    // nullopt => fresh start
    // nullptr => no chunk sent for restore point
    // otherwise => chunk contents sent for restore point
    std::optional<chunk_ptr> restore;
  };

  virtual ~operator_base() = default;

  virtual auto name() const -> std::string = 0;

  virtual auto spawn(spawn_args args) const -> exec::operator_actor {
    (void)args;
    TENZIR_TODO();
  }
};

using operator_ptr = std::unique_ptr<operator_base>;

auto inspect(auto& f, operator_ptr& x) -> bool {
  return plugin_inspect(f, x);
}

}; // namespace tenzir::plan
