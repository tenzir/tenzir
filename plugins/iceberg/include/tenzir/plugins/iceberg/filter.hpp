//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/ir.hpp>
#include <tenzir/tql2/ast.hpp>

#include <iceberg/type_fwd.h>

#include <memory>

namespace tenzir::plugins::iceberg {

/// Translates TQL optimizer predicates into a conservative Iceberg Expression
/// for manifest (partition) and data-file (metrics) pruning.
///
/// Soundness invariant: if any row in a file conforming to `schema` could match
/// `filter` in TQL (or emit a runtime warning/diagnostic when evaluated in
/// TQL), the returned expression must NOT cause Iceberg's evaluators to prune
/// that manifest or data file. Every predicate still runs locally in Tenzir.
auto translate_filter(ir::OptimizeFilter const& filter,
                      ::iceberg::Schema const& schema)
  -> std::shared_ptr<::iceberg::Expression>;

/// Like `translate_filter(filter, schema)`, but names each field as
/// `bind_schema` does, which identifies fields by ID. Paths still resolve
/// against `schema`. A field that `bind_schema` lacks, gives another type, or
/// tightens from optional to required along any segment of the path stays
/// conservative. Use this when the scan binds the expression against another
/// schema than the one whose names the events carry.
auto translate_filter(ir::OptimizeFilter const& filter,
                      ::iceberg::Schema const& schema,
                      ::iceberg::Schema const& bind_schema)
  -> std::shared_ptr<::iceberg::Expression>;

/// Translates a single TQL AST predicate against `schema`.
auto translate_expression(ast::expression const& expr,
                          ::iceberg::Schema const& schema)
  -> std::shared_ptr<::iceberg::Expression>;

} // namespace tenzir::plugins::iceberg
