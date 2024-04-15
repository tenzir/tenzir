//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/tql2/plugin.hpp"

namespace tenzir::tql2 {

// TODO: Change `entity_def` and everything related to it.

using entity_def
  = variant<const function_plugin*, const operator_factory_plugin*>;

class registry {
public:
  // TODO: The interface of this class is drastically simplified for now. It
  // must be changed eventually to properly enable modules and use an interned
  // representation of `entity_path`.
  void add(std::string name, entity_def def) {
    auto inserted = defs_.emplace(std::move(name), def).second;
    TENZIR_ASSERT(inserted);
  }

  auto try_get(const entity_path& path) const -> const entity_def* {
    if (path.segments().size() != 1) {
      // TODO: We pretend here that only single-name paths exist.
      return nullptr;
    }
    auto it = defs_.find(path.segments()[0]);
    if (it == defs_.end()) {
      return nullptr;
    }
    return &it->second;
  }

  auto get(const entity_path& path) const -> const entity_def& {
    auto result = try_get(path);
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
