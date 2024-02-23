//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/ast.hpp"

namespace tenzir::tql2 {

struct function_def {
  std::string test;
};

class operator_use {
public:
  virtual ~operator_use() = default;
};

class operator_def {
public:
  virtual ~operator_def() = default;

  virtual auto name() const -> std::string_view = 0;

  virtual auto make(std::vector<ast::expression> args)
    -> std::unique_ptr<operator_use>
    = 0;
};

using entity_def = variant<function_def, std::unique_ptr<operator_def>>;

class registry {
public:
  auto add(function_def fn) -> entity_id {
    defs_.emplace_back(std::move(fn));
    return last_id();
  }

  auto add(std::unique_ptr<operator_def> op) -> entity_id {
    defs_.emplace_back(std::move(op));
    return last_id();
  }

  auto get(entity_id id) const -> const entity_def& {
    TENZIR_ASSERT(id.id < defs_.size());
    return defs_[id.id];
  }

  auto try_fn(entity_id id) const -> const function_def* {
    return std::get_if<function_def>(&get(id));
  }

private:
  auto last_id() const -> entity_id {
    TENZIR_ASSERT(not defs_.empty());
    return entity_id{defs_.size() - 1};
  }

  // TODO: Lifetimes.
  std::vector<entity_def> defs_;
};

auto thread_local_registry() -> const registry*;

void set_thread_local_registry(const registry* reg);

template <class F>
auto with_thread_local_registry(const registry& reg, F&& f) {
  set_thread_local_registry(&reg);
  std::forward<F>(f)();
  set_thread_local_registry(nullptr);
}

} // namespace tenzir::tql2
