//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir {

// TODO: This supersedes the `session` overload, which should be removed soon.
auto resolve_entities(ast::pipeline& pipe, base_ctx ctx) -> failure_or<void>;

auto resolve_entities(ast::pipeline& pipe, session ctx) -> failure_or<void>;

auto resolve_entities(ast::expression& expr, session ctx) -> failure_or<void>;

auto resolve_entities(ast::function_call& fc, session ctx) -> failure_or<void>;

} // namespace tenzir
