//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ir.hpp"

namespace tenzir {

class plugin;

/// Compile a TQL `if` statement into its native IR operator.
auto make_if_ir(ast::if_stmt stmt, compile_ctx& ctx)
  -> failure_or<Box<ir::Operator>>;

/// Create the inspection plugin for the native if IR operator.
auto make_if_ir_inspection_plugin() -> plugin*;

} // namespace tenzir
