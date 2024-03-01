//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/registry.hpp"

namespace tenzir::tql2 {

thread_local const registry* g_thread_local_registry = nullptr;

auto thread_local_registry() -> const registry* {
  return g_thread_local_registry;
}

void set_thread_local_registry(const registry* reg) {
  g_thread_local_registry = reg;
}

} // namespace tenzir::tql2
