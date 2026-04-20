//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/offset.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir2/table_slice.hpp"
#include "tenzir2/type_system/array/array.hpp"
#include "tenzir2/type_system/data/data.hpp"
#include "tenzir2/type_system/type.hpp"
#include "tenzir2/variant.hpp"

#include <optional>

namespace tenzir2 {

auto eval(tenzir::ast::expression const& expr, TableSlice const& input,
          tenzir::diagnostic_handler& dh) -> array_<data>;

// A simple selector always yields a single type.
auto eval(tenzir::ast::field_path const& expr, TableSlice const& input,
          tenzir::diagnostic_handler& dh) -> array_<data>;

// A constant always yields a single type.
auto eval(tenzir::ast::constant const& expr, TableSlice const& input,
          tenzir::diagnostic_handler& dh) -> array_<data>;

/// Constant evaluates an expression to a single array (no table input).
auto const_eval_series(tenzir::ast::expression const& expr,
                       tenzir::diagnostic_handler& dh)
  -> tenzir::failure_or<array_<data>>;

/// Constant evaluates an expression, even if it is non-deterministic.
auto const_eval(tenzir::ast::expression const& expr,
                tenzir::diagnostic_handler& dh) -> tenzir::failure_or<data>;

/// Tries to evaluate a determistic expression to a constant value. Emits
/// diagnostics only if the evaluation succeeded.
auto try_const_eval(tenzir::ast::expression const& expr, tenzir::session ctx)
  -> std::optional<data>;

auto eval(tenzir::ast::lambda_expr const& lambda, array_<list> const& input,
          TableSlice const& slice, tenzir::diagnostic_handler& dh)
  -> array_<data>;

auto eval(tenzir::ast::lambda_expr const& lambda, data const& input,
          tenzir::diagnostic_handler& dh) -> data;

struct resolve_error {
  struct field_not_found {};
  struct field_not_found_no_error {};
  struct field_of_non_record {
    type_<data> type;
  };

  using reason_t
    = variant<field_not_found, field_not_found_no_error, field_of_non_record>;

  resolve_error(tenzir::ast::identifier ident, reason_t reason);

  tenzir::ast::identifier ident;
  reason_t reason;
};

auto resolve(tenzir::ast::field_path const& sel, TableSlice const& slice)
  -> variant<array_<data>, resolve_error>;

auto resolve(tenzir::ast::field_path const& sel, type_<data> ty)
  -> variant<tenzir::offset, resolve_error>;

} // namespace tenzir2
