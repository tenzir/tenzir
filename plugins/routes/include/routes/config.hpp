//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#pragma once

#include "routes/fwd.hpp"
#include "routes/connection.hpp"
#include "routes/route.hpp"

#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/view.hpp>

#include <tenzir/data.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/session.hpp>

#include <string>
#include <unordered_map>
#include <vector>

namespace tenzir::plugins::routes {

/// Complete routing configuration containing connections and routes.
struct config {
  /// List of input-to-output connections.
  std::vector<connection> connections;

  /// Named routes with their routing logic.
  std::unordered_map<std::string, route> routes;

  /// Creates a config from a record view.
  static auto make(const view<record>& data, session ctx) -> failure_or<config>;

  /// Converts a config to a record for printing.
  auto to_record() const -> record;

  template <class Inspector>
  friend auto inspect(Inspector& f, config& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.routes.config")
      .fields(f.field("connections", x.connections),
              f.field("routes", x.routes));
  }
};

} // namespace tenzir::plugins::routes
