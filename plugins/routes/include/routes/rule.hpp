//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#pragma once

#include <tenzir/tql2/ast.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/view.hpp>

#include <tenzir/data.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/session.hpp>
#include <tenzir/table_slice.hpp>

#include <string>
#include <utility>

namespace tenzir::plugins::routes {

/// Represents a routing rule within a route.
struct rule {
  /// The predicate condition for this rule.
  ast::expression where = ast::constant{true, location::unknown};

  /// The string representation of the where expression. This is needed for roundtripping.
  std::string where_str = "true";

  /// The output destination for matching data.
  std::string output;

  /// Whether this rule is final (stops further rule evaluation).
  bool final = false;

  /// Creates a rule from a record view.
  static auto make(const view<record>& data, session ctx) -> failure_or<rule>;

  /// Converts a rule to a record for printing.
  auto to_record() const -> record;

  /// Evaluates the rule against a table slice.
  /// @param slice The input table slice to evaluate.
  /// @returns A pair where the first element contains rows that match the rule,
  ///          and the second element contains rows that need further evaluation.
  auto evaluate(const table_slice& slice) const -> std::pair<table_slice, table_slice>;

  template <class Inspector>
  friend auto inspect(Inspector& f, rule& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.routes.rule")
      .fields(f.field("where", x.where),
              f.field("where_str", x.where_str),
              f.field("output", x.output),
              f.field("final", x.final));
  }
};

} // namespace tenzir::plugins::routes
