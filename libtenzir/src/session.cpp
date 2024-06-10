//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/session.hpp"

#include "tenzir/tql2/registry.hpp"

namespace tenzir {

auto session::reg() -> const registry& {
  // TODO: The registry should be attached to a session instead.
  return global_registry();
}

} // namespace tenzir
