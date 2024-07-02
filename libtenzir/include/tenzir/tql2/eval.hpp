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
#include "tenzir/type.hpp"

namespace tenzir {

auto eval(const ast::expression& expr, const table_slice& input,
          diagnostic_handler& dh) -> series;

auto const_eval(const ast::expression& expr, diagnostic_handler& dh)
  -> std::optional<data>;

struct resolve_error {
  struct field_not_found {};
  struct field_of_non_record {
    tenzir::type type;
  };

  using reason_t = variant<field_not_found, field_of_non_record>;

  resolve_error(ast::identifier ident, reason_t reason)
    : ident{std::move(ident)}, reason{std::move(reason)} {
  }

  ast::identifier ident;
  reason_t reason;
};

auto resolve(const ast::simple_selector& sel, const table_slice& slice)
  -> variant<series, resolve_error>;

auto resolve(const ast::simple_selector& sel, type ty)
  -> variant<offset, resolve_error>;

} // namespace tenzir
