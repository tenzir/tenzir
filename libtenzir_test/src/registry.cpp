//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/test/registry.hpp"

namespace tenzir::test {

auto builtin_registry() -> registry const& {
  // Leak this on purpose: the plugins that it refers to outlive every test.
  static auto const* reg
    = new Arc<registry const>{make_builtin_registry()}; // NOLINT
  return **reg;
}

} // namespace tenzir::test
