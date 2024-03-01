//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/registry.hpp"

namespace tenzir::tql2 {

void resolve_entities(ast::pipeline& pipe, registry& reg,
                      diagnostic_handler& diag);

} // namespace tenzir::tql2
