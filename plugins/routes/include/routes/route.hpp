//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#pragma once

#include "routes/connection.hpp"
#include "routes/rule.hpp"

#include <tenzir/data.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/session.hpp>
#include <tenzir/view.hpp>

#include <string>
#include <vector>

namespace tenzir::plugins::routes {

/// Represents a stateful route with cascading predicates.
struct route {
  /// The input source for this route.
  output input;

  /// Ordered list of routing rules (cascading predicates).
  std::vector<rule> rules;

  /// Creates a route from a record view.
  static auto make(const view<record>& data, session ctx) -> failure_or<route>;

  /// Converts a route to a record for printing.
  auto to_record() const -> record;

  template <class Inspector>
  friend auto inspect(Inspector& f, route& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.routes.route")
      .fields(f.field("input", x.input), f.field("rules", x.rules));
  }
};

} // namespace tenzir::plugins::routes
