//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/registry.hpp"

#include "tenzir/plugin.hpp"

namespace tenzir {

thread_local const registry* g_thread_local_registry = nullptr;

auto thread_local_registry() -> const registry* {
  return g_thread_local_registry;
}

void set_thread_local_registry(const registry* reg) {
  g_thread_local_registry = reg;
}

auto global_registry() -> const registry& {
  static auto reg = std::invoke([] {
    auto reg = registry{};
    for (auto op : plugins::get<operator_factory_plugin>()) {
      auto name = op->name();
      // TODO
      if (name.starts_with("tql2.")) {
        name = name.substr(5);
      }
      reg.add(name, op);
    }
    for (auto fn : plugins::get<function_plugin>()) {
      auto name = fn->name();
      // TODO
      if (name.starts_with("tql2.")) {
        name = name.substr(5);
      }
      reg.add(name, fn);
    }
    return reg;
  });
  return reg;
}

} // namespace tenzir
