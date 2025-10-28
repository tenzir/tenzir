//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/plan/operator_spawn_args.hpp"
#include "tenzir/plugin.hpp"

namespace tenzir::plan {

/// Configured instance of an operator that is ready for execution.
///
/// Subclasses must register a serialization plugin with the same name.
class operator_base {
public:
  virtual ~operator_base() = default;

  virtual auto name() const -> std::string = 0;

  virtual auto spawn(operator_spawn_args args) const -> exec::operator_actor {
    TENZIR_TODO();
  }

  virtual auto spawn(std::optional<chunk_ptr> restore) && -> OperatorPtr {
    TENZIR_TODO();
  }
};

using operator_ptr = std::unique_ptr<operator_base>;

auto inspect(auto& f, operator_ptr& x) -> bool {
  return plugin_inspect(f, x);
}

}; // namespace tenzir::plan
