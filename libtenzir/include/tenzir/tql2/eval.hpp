//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/registry.hpp"

namespace tenzir::tql2 {

auto const_eval(const ast::expression& expr, context& ctx)
  -> std::optional<data>;

struct resolve_error {
  struct field_not_found {};
  struct not_a_record {
    type type;
  };

  using reason_t = variant<field_not_found, not_a_record>;

  resolve_error(ast::identifier ident, reason_t reason)
    : ident{std::move(ident)}, reason{std::move(reason)} {
  }

  ast::identifier ident;
  reason_t reason;
};

auto resolve(const ast::selector& sel, const table_slice& slice)
  -> variant<series, resolve_error>;

auto resolve(const ast::selector& sel, type ty)
  -> variant<offset, resolve_error>;

auto eval(const ast::expression& expr, const table_slice& input,
          diagnostic_handler& dh) -> series;

} // namespace tenzir::tql2
