//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#pragma once

#include <tenzir/data.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/session.hpp>
#include <tenzir/view.hpp>

#include <functional>
#include <string>

namespace tenzir::plugins::routes {

/// Strong type for a route input name. SEEN FROM THE PoV OF THE ROUTE.
/// A route configures an *input*, which is populated by the `routes::output`
/// operator.
struct input_name {
  std::string name;

  friend auto inspect(auto& f, input_name& x) -> bool {
    return f.apply(x.name);
  }

  friend auto operator<=>(const input_name&, const input_name&) = default;
};

/// Strong type for a route output name. SEEN FROM THE PoV OF THE ROUTE.
/// A route configures an *output*, which is consumed by the `routes::input`
/// operator.
struct output_name {
  std::string name;

  friend auto inspect(auto& f, output_name& x) -> bool {
    return f.apply(x.name);
  }

  friend auto operator<=>(const output_name&, const output_name&) = default;
};

/// Represents a connection between an input and an output.
struct connection {
  /// The name of the source
  output_name from;

  /// The name of the destination
  input_name to;

  /// Creates a connection from a record view.
  static auto make(const view<record>& data, session ctx)
    -> failure_or<connection>;

  /// Converts a connection to a record for printing.
  auto to_record() const -> record;

  template <class Inspector>
  friend auto inspect(Inspector& f, connection& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.routes.connection")
      .fields(f.field("from", x.from), f.field("to", x.to));
  }
};

} // namespace tenzir::plugins::routes

namespace std {

template <>
struct hash<tenzir::plugins::routes::input_name> {
  auto operator()(const tenzir::plugins::routes::input_name& x) const noexcept
    -> size_t {
    return hash<string>{}(x.name);
  }
};

template <>
struct hash<tenzir::plugins::routes::output_name> {
  auto operator()(const tenzir::plugins::routes::output_name& x) const noexcept
    -> size_t {
    return hash<string>{}(x.name);
  }
};

} // namespace std

template <>
struct fmt::formatter<tenzir::plugins::routes::input_name>
  : fmt::formatter<std::string> {
  auto format(const tenzir::plugins::routes::input_name& x,
              format_context& ctx) const {
    return fmt::formatter<std::string>::format(x.name, ctx);
  }
};

template <>
struct fmt::formatter<tenzir::plugins::routes::output_name>
  : fmt::formatter<std::string> {
  auto format(const tenzir::plugins::routes::output_name& x,
              format_context& ctx) const {
    return fmt::formatter<std::string>::format(x.name, ctx);
  }
};
