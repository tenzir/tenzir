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

namespace tenzir::bp {

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

/// An executable pipeline is just a sequence of executable operators.
///
/// TODO: Can we assume that it is well-typed?
class pipeline {
public:
  pipeline() = default;

  explicit(false) pipeline(std::vector<operator_ptr> operators)
    : operators_{std::move(operators)} {
  }

  template <std::derived_from<operator_base> T>
  explicit(false) pipeline(std::unique_ptr<T> ptr) {
    operators_.push_back(std::move(ptr));
  }

  auto begin() {
    return operators_.begin();
  }

  auto end() {
    return operators_.end();
  }

  auto unwrap() && -> std::vector<operator_ptr> {
    return std::move(operators_);
  }

  auto operator[](size_t index) -> operator_ptr& {
    return operators_[index];
  }

  auto id() const -> uuid {
    return id_;
  }

  auto size() const -> size_t {
    return operators_.size();
  }

  friend auto inspect(auto& f, pipeline& x) -> bool {
    // TODO: Tests?
    return f.object(x).fields(f.field("id", x.id_),
                              f.field("operators", x.operators_));
  }

private:
  uuid id_ = uuid::random();
  std::vector<operator_ptr> operators_;
};

}; // namespace tenzir::bp
