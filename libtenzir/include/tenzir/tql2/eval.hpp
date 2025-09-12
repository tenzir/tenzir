//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/multi_series.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/type.hpp"

namespace tenzir {

auto eval(const ast::expression& expr, const table_slice& input,
          diagnostic_handler& dh) -> multi_series;

// A simple selector always yields a single type.
auto eval(const ast::field_path& expr, const table_slice& input,
          diagnostic_handler& dh) -> series;

// A constant always yields a single type.
auto eval(const ast::constant& expr, const table_slice& input,
          diagnostic_handler& dh) -> series;

/// Constant evaluates an expression, even if it is non-deterministic.
auto const_eval(const ast::expression& expr, diagnostic_handler& dh)
  -> failure_or<data>;

/// Tries to evaluate a determistic expression to a constant value. Emits
/// diagnostics only if the evaluation succeeded.
auto try_const_eval(const ast::expression& expr, session ctx)
  -> std::optional<data>;

auto eval(const ast::lambda_expr& lambda, const basic_series<list_type>& input,
          const table_slice& slice, diagnostic_handler& dh) -> multi_series;

auto eval(const ast::lambda_expr& lambda, const multi_series& input,
          diagnostic_handler& dh) -> multi_series;

auto eval(const ast::lambda_expr& lambda, const data& input,
          diagnostic_handler& dh) -> data;

struct resolve_error {
  struct field_not_found {};
  struct field_not_found_no_error {};
  struct field_of_non_record {
    tenzir::type type;
  };

  using reason_t
    = variant<field_not_found, field_not_found_no_error, field_of_non_record>;

  resolve_error(ast::identifier ident, reason_t reason)
    : ident{std::move(ident)}, reason{std::move(reason)} {
  }

  ast::identifier ident;
  reason_t reason;
};

auto resolve(const ast::field_path& sel, const table_slice& slice)
  -> variant<series, resolve_error>;

auto resolve(const ast::field_path& sel, type ty)
  -> variant<offset, resolve_error>;

} // namespace tenzir
