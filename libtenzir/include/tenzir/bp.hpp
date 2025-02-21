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
               std::optional<chunk_ptr> chunk)
      : sys{sys}, ctx{ctx}, chunk{std::move(chunk)} {
    }

    caf::actor_system& sys;
    base_ctx ctx;

    // nullopt => fresh start
    // nullptr => no chunk sent for restore point
    // otherwise => chunk contents sent for restore point
    std::optional<chunk_ptr> chunk;
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

  auto id() const -> uuid {
    return id_;
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
