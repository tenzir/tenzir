//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/heterogeneous_string_hash.hpp"
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
  void add(std::string name, entity_def fn) {
    auto inserted = defs_.emplace(std::move(name), std::move(fn)).second;
    TENZIR_ASSERT(inserted);
  }

  // TODO
  auto try_get(std::string_view name) const -> const entity_def* {
    auto it = defs_.find(name);
    if (it == defs_.end()) {
      return nullptr;
    }
    return &it->second;
  }

  auto get(std::string_view name) const -> const entity_def& {
    auto result = try_get(name);
    TENZIR_ASSERT(result);
    return *result;
  }

private:
  // TODO: Lifetime?
  detail::heterogeneous_string_hashmap<entity_def> defs_;
};

auto thread_local_registry() -> const registry*;

void set_thread_local_registry(const registry* reg);

template <class F>
auto with_thread_local_registry(const registry& reg, F&& f) {
  auto prev = thread_local_registry();
  set_thread_local_registry(&reg);
  std::forward<F>(f)();
  set_thread_local_registry(prev);
}

} // namespace tenzir::tql2
