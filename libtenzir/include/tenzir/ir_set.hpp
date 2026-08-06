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

/// Validate an assignment target without resolving its dynamic indexes.
auto validate_assignment_target(ast::expression const& expression,
                                diagnostic_handler& dh) -> failure_or<void>;

/// Create a `set` IR operator with the given assignment.
auto make_set_ir(ast::assignment assignment) -> Box<ir::Operator>;

} // namespace tenzir
