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

namespace tenzir {

auto resolve_entities(ast::pipeline& pipe, session ctx) -> failure_or<void>;

auto resolve_entities(ast::expression& expr, session ctx) -> failure_or<void>;

} // namespace tenzir
