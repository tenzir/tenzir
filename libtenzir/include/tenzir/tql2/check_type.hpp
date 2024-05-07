//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/type.hpp"

namespace tenzir::tql2 {

// TODO: This might not be what we want.

/// Error will be emitted if ill-typed.
auto check_type(const ast::expression& expr, session ctx)
  -> std::optional<type>;

void check_assignment(const ast::assignment& x, session ctx);

} // namespace tenzir::tql2
