//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#pragma once

#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/view.hpp>

#include <tenzir/data.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/session.hpp>

#include <string>

namespace tenzir::plugins::routes {

/// Represents a connection between an input and an output.
struct connection {
  /// The name of the input source.
  std::string from;

  /// The name of the output destination.
  std::string to;

  /// Creates a connection from a record view.
  static auto make(const view<record>& data, session ctx) -> failure_or<connection>;

  /// Converts a connection to a record for printing.
  auto to_record() const -> record;

  template <class Inspector>
  friend auto inspect(Inspector& f, connection& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.routes.connection")
      .fields(f.field("from", x.from),
              f.field("to", x.to));
  }
};

} // namespace tenzir::plugins::routes
