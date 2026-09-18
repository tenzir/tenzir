//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace tenzir {

/// Whether `--nova` was passed on the command line: operators with multiple
/// implementations that share the same input type but differ in whether they
/// produce `nova::Events` should prefer the `nova::Events` alternative. Set
/// once at startup before any pipeline is compiled.
auto nova_enabled() -> bool;

/// Sets the `--nova` flag. Must be called before any pipeline is compiled;
/// not safe to call concurrently with `nova_enabled()`.
void set_nova_enabled(bool value);

} // namespace tenzir
