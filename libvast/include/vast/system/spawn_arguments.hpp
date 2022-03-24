//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/command.hpp"
#include "vast/expression.hpp"

#include <caf/deep_to_string.hpp>
#include <caf/fwd.hpp>
#include <caf/meta/type_name.hpp>
#include <fmt/format.h>

#include <filesystem>
#include <optional>
#include <string>

namespace vast::system {

/// Wraps arguments for spawn functions.
struct spawn_arguments {
  /// Current command executed by the node actor.
  const invocation& inv;

  /// Path to persistent node state.
  const std::filesystem::path& dir;

  /// Label for the new component.
  const std::string& label;

  /// An optional expression for components that expect one.
  std::optional<expression> expr;

  /// Returns whether CLI arguments are empty.
  [[nodiscard]] bool empty() const noexcept {
    return inv.arguments.empty();
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, spawn_arguments& x) ->
    typename Inspector::result_type {
    return f(caf::meta::type_name("vast.system.spawn_arguments"), x.inv, x.dir,
             x.label, x.expr);
  }
};

/// Attempts to parse `[args.first, args.last)` as ::expression and returns a
/// normalized and validated version of that expression on success.
caf::expected<expression>
normalized_and_validated(const std::vector<std::string>& args);

caf::expected<expression>
normalized_and_validated(std::vector<std::string>::const_iterator begin,
                         std::vector<std::string>::const_iterator end);

caf::expected<expression> get_expression(const spawn_arguments& args);

/// Attemps to read a schema file and parse its content. Can either 1) return
/// nothing if the user didn't specifiy a schema file in `args.options`, 2)
/// produce a valid schema, or 3) run into an error.
caf::expected<std::optional<module>> read_module(const spawn_arguments& args);

/// Generates an error for unexpected CLI arguments in `args`.
caf::error unexpected_arguments(const spawn_arguments& args);

} // namespace vast::system

namespace fmt {

template <>
struct formatter<vast::system::spawn_arguments> : formatter<std::string> {
  template <class FormatContext>
  auto format(const vast::system::spawn_arguments& value, FormatContext& ctx) {
    return formatter<std::string>::format(caf::deep_to_string(value), ctx);
  }
};

} // namespace fmt
