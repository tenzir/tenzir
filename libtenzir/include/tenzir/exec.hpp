//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/plugin.hpp>

namespace tenzir {

// TODO: Figure out how this actually looks like.
using operator_actor = int;

} // namespace tenzir

namespace tenzir::exec {

/// Configured instance of an operator that is ready for execution.
///
/// Subclasses must register a serialization plugin with the same name.
class operator_base {
public:
  virtual ~operator_base() = default;

  virtual auto name() const -> std::string = 0;

  virtual auto spawn(/*args*/) const -> operator_actor {
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

  friend auto inspect(auto& f, pipeline& x) -> bool {
    return f.apply(x.operators_);
  }

private:
  std::vector<operator_ptr> operators_;
};

}; // namespace tenzir::exec
